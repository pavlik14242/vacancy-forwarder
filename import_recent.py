#!/usr/bin/env python3
# import_recent.py — запускает только import_history (например: после очистки канала)
import asyncio, json
from forwarder import main as forwarder_main

if __name__ == "__main__":
    # forwarder.main supports no_history flag; but we want only history and exit.
    # to avoid complexity, reuse forwarder.import_history via wrapper — simplest: run forwarder.py with no live
    import subprocess, sys
    subprocess.run([sys.executable, "forwarder.py"])
