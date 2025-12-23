#!/usr/bin/env python3
# forwarder.py
# Запускает initial import (последние history_hours) и затем live-forwarding новых сообщений.
# Usage:
#   ./venv/bin/python forwarder.py           # запустит import (если enabled) + live
#   ./venv/bin/python forwarder.py --no-history   # только live

import asyncio, json, re, sqlite3, time, hashlib, sys, argparse
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient, events, errors

# ---- load config ----
with open("config.json", encoding="utf-8") as f:
    cfg = json.load(f)

API_ID = cfg["api_id"]
API_HASH = cfg["api_hash"]
SESSION = cfg.get("session_name", "vacancy_session")
TARGET = cfg.get("target_channel", "me")
MONITOR_ALL = bool(cfg.get("monitor_all_dialogs", False))
MONITOR_IDS = cfg.get("monitor_chat_ids", []) or []
HISTORY_SCAN = bool(cfg.get("history_scan", True))
HISTORY_HOURS = int(cfg.get("history_hours", 24))
HISTORY_LIMIT_PER_CHAT = int(cfg.get("history_limit_per_chat", 500))
MIN_DELAY = float(cfg.get("min_delay_seconds", 1.5))
PROX = int(cfg.get("proximity_chars", 80))
DB_PATH = cfg.get("db_file", "vacancy_seen.db")

ROLE_KW = [k for k in cfg.get("role_keywords", []) if k]
SEEK_KW = [k for k in cfg.get("seeking_keywords", []) if k]
EXCL_KW = [k for k in cfg.get("exclude_keywords", []) if k]
EXCL_PLAT = [k.lower() for k in cfg.get("exclude_platforms", []) if k]

# ---- compile regex once ----
def compile_or(words, flags=re.IGNORECASE | re.UNICODE):
    if not words:
        return None
    safe = [w if any(ch in w for ch in "\\.^$*+?[](){}|") else re.escape(w) for w in words]
    return re.compile(r"(?i)(?:"+r"|".join(safe)+r")", flags=flags)

EXCL_RE = compile_or(EXCL_KW)
ROLE_RE = compile_or(ROLE_KW)
SEEK_RE = compile_or(SEEK_KW)

# Proximity pattern: role ...{0,PROX}... seek OR seek ...{0,PROX}... role
def build_prox_pattern(roles, seeks, prox):
    if not roles or not seeks:
        return None
    r = "|".join(re.escape(x) for x in roles)
    s = "|".join(re.escape(x) for x in seeks)
    pat = rf"(?is)(?:(?:{r}).{{0,{prox}}}(?:{s}))|(?:(?:{s}).{{0,{prox}}}(?:{r}))"
    return re.compile(pat, flags=re.IGNORECASE)

PROX_RE = build_prox_pattern(ROLE_KW, SEEK_KW, PROX)

# ---- DB ----
def init_db(path=DB_PATH):
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS seen_messages (
        id TEXT PRIMARY KEY,
        chat_id INTEGER,
        message_id INTEGER,
        ts INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS forwarded_hashes (
        hash TEXT PRIMARY KEY,
        first_seen_id TEXT,
        forwarded_ts INTEGER
    )""")
    conn.commit()
    return conn

def safe_insert_seen(conn, unique_id, chat_id, message_id, ts):
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO seen_messages (id, chat_id, message_id, ts) VALUES (?,?,?,?)",
                    (unique_id, chat_id, message_id, ts))
        conn.commit()
    except sqlite3.IntegrityError:
        pass

def seen_check(conn, unique_id):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM seen_messages WHERE id=? LIMIT 1", (unique_id,))
    return cur.fetchone() is not None

def forwarded_hash_check(conn, h):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM forwarded_hashes WHERE hash=? LIMIT 1", (h,))
    return cur.fetchone() is not None

def mark_forwarded_hash(conn, h, first_seen_id):
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO forwarded_hashes (hash, first_seen_id, forwarded_ts) VALUES (?,?,?)",
                    (h, first_seen_id, int(time.time())))
        conn.commit()
    except sqlite3.IntegrityError:
        pass

# ---- helpers ----
def norm_text(s: str) -> str:
    if not s:
        return ""
    t = s.lower()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def text_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def classify_text(text: str):
    """Возвращает (is_client_vacancy, is_person_offer, reason_snippet)"""
    t = norm_text(text)
    # 1) exclude platform (google, fb etc.) -> reject
    for p in EXCL_PLAT:
        if p in t:
            return False, True, f"platform_excluded:{p}"
    # 2) explicit "offer" phrases anywhere -> person offer
    if EXCL_RE and EXCL_RE.search(t):
        return False, True, "offer_keyword"
    # 3) proximity: role + seeking within PROX -> client vacancy
    if PROX_RE and PROX_RE.search(t):
        return True, False, "prox_match"
    # 4) weaker: only role present AND a seeking word somewhere (not prox) -> check distance threshold by tokens
    if ROLE_RE and ROLE_RE.search(t) and SEEK_RE and SEEK_RE.search(t):
        # if both present but maybe far — still consider vacancy unless offer keywords exist
        return True, False, "role_and_seek"
    return False, False, "no_match"

# ---- Forward logic ----
async def forward_message(client, message, db_conn):
    # normalize text for dedup
    text = (message.message or "") + " " + (getattr(message, "text", "") or "")
    txt = norm_text(text)
    if not txt.strip():
        return False
    h = text_hash(txt[:2000])  # truncate long text for speed
    if forwarded_hash_check(db_conn, h):
        return False
    # forward preserving original
    try:
        await client.forward_messages(entity=TARGET, messages=message, from_peer=message.chat_id)
        mark_forwarded_hash(db_conn, h, f"{message.chat_id}:{message.id}")
        return True
    except Exception:
        # fallback: send a short summary message
        try:
            snippet = txt[:800]
            await client.send_message(TARGET, f"Forward candidate (fallback):\n\n{snippet}\n\n— source: {message.chat.title if hasattr(message.chat,'title') else message.chat_id}")
            mark_forwarded_hash(db_conn, h, f"{message.chat_id}:{message.id}")
            return True
        except Exception as e:
            print("Forward failed:", type(e).__name__, e)
            return False

# ---- scanning history ----
async def import_history(client, conn, cutoff_dt):
    print("Importing messages newer than", cutoff_dt.isoformat())
    forwarded = 0
    skipped = 0
    # Decide dialogs to scan
    if MONITOR_ALL:
        dialogs = []
        async for d in client.iter_dialogs():
            if getattr(d, "title", None):
                dialogs.append(d.id)
    else:
        dialogs = list(MONITOR_IDS)

    for chat_id in dialogs:
        try:
            async for msg in client.iter_messages(chat_id, limit=HISTORY_LIMIT_PER_CHAT):
                if not msg or not getattr(msg, "date", None):
                    continue
                if msg.date.replace(tzinfo=timezone.utc) < cutoff_dt:
                    break
                unique_id = f"{msg.chat_id}:{msg.id}"
                if seen_check(conn, unique_id):
                    skipped += 1
                    continue
                txt = (msg.message or "") + " " + (getattr(msg, "text", "") or "")
                is_vac, is_offer, reason = classify_text(txt)
                if is_vac and not is_offer:
                    ok = await forward_message(client, msg, conn)
                    if ok:
                        forwarded += 1
                        safe_insert_seen(conn, unique_id, msg.chat_id, msg.id, int(msg.date.timestamp()))
                        await asyncio.sleep(MIN_DELAY)
                    else:
                        skipped += 1
                else:
                    skipped += 1
                    safe_insert_seen(conn, unique_id, msg.chat_id, msg.id, int(msg.date.timestamp()))
        except Exception as e:
            print("History scan error for", chat_id, type(e).__name__, e)
    print("Done. forwarded:", forwarded, "skipped:", skipped)
    return forwarded, skipped

# ---- live handler ----
def make_live_handler(client, conn):
    async def handler(event):
        msg = event.message
        unique_id = f"{msg.chat_id}:{msg.id}"
        if seen_check(conn, unique_id):
            return
        text = (msg.message or "") + " " + (getattr(msg, "text", "") or "")
        is_vac, is_offer, reason = classify_text(text)
        if is_vac and not is_offer:
            ok = await forward_message(client, msg, conn)
            if ok:
                safe_insert_seen(conn, unique_id, msg.chat_id, msg.id, int(msg.date.timestamp()))
                await asyncio.sleep(MIN_DELAY)
        else:
            safe_insert_seen(conn, unique_id, msg.chat_id, msg.id, int(msg.date.timestamp()))
    return handler

# ---- main ----
async def main(no_history=False):
    conn = init_db(DB_PATH)
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print("Signed in as:", getattr(me, "username", me.id))
    # initial import
    if HISTORY_SCAN and not no_history:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=HISTORY_HOURS)
        await import_history(client, conn, cutoff)
    # live listening
    handler = make_live_handler(client, conn)
    if MONITOR_ALL:
        # listen to all new messages, but ignore private one-on-one if desired (config can be extended)
        client.add_event_handler(handler, events.NewMessage(incoming=True))
        print("Listening for new messages on ALL dialogs...")
    else:
        # listen only for configured chats
        client.add_event_handler(handler, events.NewMessage(chats=MONITOR_IDS, incoming=True))
        print("Listening for new messages on monitor_chat_ids:", MONITOR_IDS)
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-history", action="store_true", help="skip historical import")
    args = parser.parse_args()
    try:
        asyncio.run(main(no_history=args.no_history))
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print("Fatal error:", type(e).__name__, e)
        raise
