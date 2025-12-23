#!/usr/bin/env python3
# delete_channel_messages.py — осторожно: удаляет сообщения в целевом канале
import asyncio, json
from telethon import TelegramClient

cfg = json.load(open("config.json", encoding="utf-8"))
API_ID = cfg["api_id"]; API_HASH = cfg["api_hash"]; SESSION = cfg.get("session_name","vacancy_session")
TARGET = cfg["target_channel"]

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    print("Connected. Deleting messages in", TARGET)
    async for msg in client.iter_messages(TARGET):
        try:
            await client.delete_messages(TARGET, msg.id)
        except Exception as e:
            print("Delete error:", e)
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
