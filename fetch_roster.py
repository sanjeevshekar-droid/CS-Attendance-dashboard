"""
One-off script: fetch recent roster images + text from CS Internal group.
Downloads photos and prints messages mentioning roster/weekend/schedule.
"""
import asyncio, os, re, sys
from datetime import datetime, timezone, timedelta
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = os.path.dirname(__file__)

def load_env(path):
    env = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env

async def main():
    from telethon import TelegramClient
    env      = load_env(os.path.join(BASE_DIR, "env"))
    API_ID   = int(env["TELEGRAM_API_ID"])
    API_HASH = env["TELEGRAM_API_HASH"]
    SESSION  = os.path.join(BASE_DIR, env.get("TELEGRAM_SESSION_FILE",
                "cityflo_session.session").replace(".session", ""))
    GROUP_ID = int(env["TGID_CS_INTERNAL"])
    IST      = timezone(timedelta(hours=5, minutes=30))

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: Not authorized."); return

    print(f"Fetching last 500 messages from group {GROUP_ID}...\n")

    keywords = re.compile(
        r"(roster|schedule|shift|weekend|saturday|sunday|week\s*\d|wef|w\.e\.f|"
        r"march|apr|may|roster\s*\d)", re.I
    )

    photo_count = 0
    async for msg in client.iter_messages(GROUP_ID, limit=500):
        if not msg: continue
        msg_ist = msg.date.astimezone(IST)
        sender  = ""
        if msg.sender:
            fn = getattr(msg.sender, "first_name", "") or ""
            ln = getattr(msg.sender, "last_name",  "") or ""
            sender = f"{fn} {ln}".strip()

        # Download photos — likely roster images
        if msg.photo:
            fname = os.path.join(BASE_DIR, f"roster_candidate_{msg.id}.jpg")
            if not os.path.exists(fname):
                await client.download_media(msg, file=fname)
                photo_count += 1
                print(f"[PHOTO] {msg_ist.strftime('%d %b %Y %H:%M')} from {sender} -> {os.path.basename(fname)}")
                if msg.text:
                    print(f"        Caption: {msg.text[:120]}")
            else:
                print(f"[PHOTO] {msg_ist.strftime('%d %b %Y %H:%M')} {os.path.basename(fname)} (already exists)")

        # Text messages mentioning roster/schedule/weekend
        elif msg.text and keywords.search(msg.text):
            print(f"[TEXT]  {msg_ist.strftime('%d %b %Y %H:%M')} from {sender}:")
            print(f"        {msg.text[:300].replace(chr(10), ' | ')}\n")

    print(f"\nDone. Downloaded {photo_count} new photos.")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
