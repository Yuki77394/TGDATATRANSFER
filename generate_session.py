"""Generate a Telethon StringSession for use on Heroku.

Run this script LOCALLY (not on Heroku) to authenticate with Telegram
and produce a session string. Then set the string as a Heroku Config Var:

    heroku config:set SESSION_STRING=<the-printed-string>

This is the standard Heroku-compatible approach for Telethon apps, because
Heroku's dyno filesystem is ephemeral — a file-based session would be lost
on every restart/redeploy.

Usage:
    python generate_session.py

The script will:
  1. Read API_ID, API_HASH, PHONE from .env (or environment).
  2. Connect to Telegram and prompt for OTP / 2FA password interactively.
  3. Print the StringSession to stdout.
  4. Exit (does NOT run the backup).

SAFETY: The session string grants full access to your Telegram account.
Treat it like a password. Never commit it to git. Set it directly as a
Heroku Config Var — do not paste it into files that might be committed.
"""
from __future__ import annotations

import asyncio
import getpass
import sys
from pathlib import Path

# Make sure local modules are importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

from config import Config  # noqa: E402


async def generate_session_string(config: Config) -> str:
    """Connect to Telegram, authenticate interactively, return the session string."""
    client = TelegramClient(
        StringSession(),
        config.api_id,
        config.api_hash,
        device_model="TelegramBackupTool",
        system_version="1.0",
        app_version="1.0.0",
        lang_code="en",
        system_lang_code="en",
    )

    print()
    print("=" * 60)
    print("  Telegram login (generating StringSession)")
    print("  A login code will be sent to your Telegram app/SMS.")
    print("=" * 60)

    await client.connect()

    if await client.is_user_authorized():
        print("  Already authorized (existing session).")
    else:
        if not config.phone:
            print("ERROR: PHONE is not set. Add it to .env or environment.", file=sys.stderr)
            sys.exit(2)

        await client.send_code_request(config.phone)
        code = input("Enter the login code you received: ").strip()
        try:
            await client.sign_in(phone=config.phone, code=code)
        except Exception as e:
            # 2FA may be required
            if "SessionPasswordNeeded" in type(e).__name__:
                password = getpass.getpass("Enter your 2FA cloud password: ")
                await client.sign_in(password=password)
            else:
                raise

    me = await client.get_me()
    display = " ".join(p for p in [me.first_name, me.last_name] if p)
    session_string = client.session.save()

    print()
    print("=" * 60)
    print("  SUCCESS! Session string generated.")
    print(f"  Authenticated as: {display} (id={me.id})")
    print("=" * 60)
    print()
    print("  Set this as a Heroku Config Var:")
    print()
    print("  heroku config:set SESSION_STRING='<string-below>'")
    print()
    print("  Session string (copy everything between the quotes):")
    print()
    print(f"  {session_string}")
    print()

    await client.disconnect()
    return session_string


if __name__ == "__main__":
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        print("Tip: copy .env.example to .env and fill in your values.", file=sys.stderr)
        sys.exit(2)

    asyncio.run(generate_session_string(config))
