"""Authentication module.

Handles Telethon client creation, OTP prompt, and 2FA password prompt.

Session handling
----------------
Two session modes are supported:

1. **File-based session** (default for local use): Telethon stores the
   session in a ``.session`` file on disk. This persists across runs as
   long as the filesystem is persistent.

2. **String session** (for Heroku / ephemeral filesystems): If the
   ``SESSION_STRING`` environment variable is set, we use Telethon's
   ``StringSession`` instead of a file. The session is loaded from the
   env var on every startup, so it survives dyno restarts. Use
   ``generate_session.py`` locally to produce the string.

Non-interactive auth
--------------------
For first-time authentication on Heroku (where there's no interactive
terminal), the following env vars can be used instead of prompts:

- ``TG_OTP_CODE``: the login code received from Telegram.
- ``TG_2FA_PASSWORD``: the 2FA cloud password (if enabled).

WARNING: Setting ``TG_2FA_PASSWORD`` as an env var is less secure than
using a pre-generated ``SESSION_STRING``. Prefer the session-string
workflow for production use.

SAFETY: This module NEVER logs API_HASH, phone numbers, OTP codes, 2FA
passwords, or session strings.
"""
from __future__ import annotations

import asyncio
import getpass
import logging
import sys
from typing import Optional

from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneNumberBannedError,
    PhoneNumberUnoccupiedError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    FloodWaitError,
)
from telethon.sessions import StringSession

from config import Config

logger = logging.getLogger("telegram_backup.auth")


def create_client(config: Config) -> TelegramClient:
    """Create a TelegramClient configured with the appropriate session type.

    If ``config.session_string`` is set, we use ``StringSession`` (for
    Heroku / ephemeral filesystems). Otherwise, we use a file-based session.
    """
    config.ensure_directories()

    if config.session_string:
        # String session mode (Heroku / cloud)
        logger.info("Using StringSession (session_string is set).")
        session = StringSession(config.session_string)
    else:
        # File-based session mode (local)
        session_path = str(config.session_file.with_suffix(""))
        logger.info("Using file-based session: %s", session_path)
        session = session_path

    client = TelegramClient(
        session,
        config.api_id,
        config.api_hash,
        device_model="TelegramBackupTool",
        system_version="1.0",
        app_version="1.0.0",
        lang_code="en",
        system_lang_code="en",
        # Use reasonable timeouts so a stuck connection doesn't hang forever
        connection_retries=config.max_retries,
        retry_delay=2,
        request_retries=config.max_retries,
        timeout=30,
    )
    return client


async def authenticate(client: TelegramClient, config: Config) -> None:
    """Connect and authenticate the client.

    Handles:
      - Existing valid session (no prompts)
      - Fresh login: OTP prompt (interactive) or TG_OTP_CODE env var
      - 2FA-enabled accounts: password prompt or TG_2FA_PASSWORD env var

    Raises a RuntimeError on unrecoverable auth failures (banned number, etc.).
    """
    logger.info("Connecting to Telegram servers...")
    await client.connect()

    if not await client.is_user_authorized():
        await _fresh_login(client, config)
    else:
        logger.info("Existing session is valid; no login required.")

    # Verify we have a usable identity
    me = await client.get_me()
    # Log only non-sensitive identity info
    display = " ".join(part for part in [getattr(me, "first_name", None), getattr(me, "last_name", None)] if part)
    logger.info("Authenticated as user id=%s (%s)", me.id, display or "<no name>")


async def _fresh_login(client: TelegramClient, config: Config) -> None:
    """Perform a fresh login (interactive or via env vars)."""
    if not config.phone:
        raise RuntimeError("PHONE is not set; cannot perform a fresh login.")

    # Determine if we can do non-interactive auth
    can_do_noninteractive = bool(config.tg_otp_code)

    if can_do_noninteractive:
        logger.info("Performing non-interactive login (TG_OTP_CODE is set).")
    else:
        # Check if we're in a non-interactive environment (Heroku)
        if config.is_heroku and not sys.stdin.isatty():
            raise RuntimeError(
                "Fresh login required but no interactive terminal available. "
                "Either set SESSION_STRING (preferred) or set TG_OTP_CODE and "
                "optionally TG_2FA_PASSWORD as Config Vars. "
                "See README for the session-string workflow."
            )
        print()
        print("=" * 60)
        print("  Telegram login required")
        print("  A login code will be sent to your Telegram app/SMS.")
        print("  Your phone number is NOT logged or stored beyond the .env file.")
        print("=" * 60)

    # Send code request (handles FloodWait internally up to a point)
    try:
        await client.send_code_request(config.phone)
    except FloodWaitError as e:
        logger.warning("Telegram requested FloodWait of %s seconds before sending code.", e.seconds)
        if can_do_noninteractive:
            logger.info("Sleeping %s seconds (non-interactive mode)...", e.seconds)
            await asyncio.sleep(e.seconds + 1)
            await client.send_code_request(config.phone)
        else:
            print(f"Telegram asks us to wait {e.seconds} seconds before sending a code. Sleeping...")
            await asyncio.sleep(e.seconds + 1)
            await client.send_code_request(config.phone)
    except PhoneNumberBannedError:
        logger.error("This phone number is banned from Telegram. Cannot continue.")
        raise RuntimeError("Phone number is banned. The tool will not attempt to bypass this.")
    except PhoneNumberUnoccupiedError:
        logger.error("This phone number is not registered on Telegram.")
        raise RuntimeError("Phone number is not registered on Telegram.")

    # Get the OTP code
    if can_do_noninteractive:
        code = config.tg_otp_code
        logger.info("Using OTP code from TG_OTP_CODE env var.")
    else:
        code = _prompt_otp()

    try:
        await client.sign_in(phone=config.phone, code=code)
    except SessionPasswordNeededError:
        await _handle_2fa(client, config)
    except PhoneCodeInvalidError:
        raise RuntimeError("The code you entered is invalid. Please restart and try again.")
    except PhoneCodeExpiredError:
        raise RuntimeError("The code has expired. Please restart and request a new one.")
    except FloodWaitError as e:
        logger.warning("FloodWait during sign_in: %s seconds", e.seconds)
        raise RuntimeError(f"Telegram rate-limited the login. Wait {e.seconds}s and try again.")


def _prompt_otp() -> str:
    """Prompt the user for the OTP code received from Telegram."""
    while True:
        code = input("Enter the login code you received: ").strip()
        if code:
            return code
        print("Code cannot be empty. Please try again.")


async def _handle_2fa(client: TelegramClient, config: Config) -> None:
    """Handle Telegram cloud password (2FA).

    If ``TG_2FA_PASSWORD`` env var is set, use it (non-interactive).
    Otherwise, prompt interactively via getpass (no echo).

    The password is NEVER logged. After use, we overwrite the local
    variable as a best-effort wipe.
    """
    if config.tg_2fa_password:
        logger.info("Using 2FA password from TG_2FA_PASSWORD env var.")
        password = config.tg_2fa_password
    else:
        # Check if we're in a non-interactive environment
        if config.is_heroku and not sys.stdin.isatty():
            raise RuntimeError(
                "2FA password required but no interactive terminal available. "
                "Set TG_2FA_PASSWORD as a Config Var, or (preferred) generate "
                "a SESSION_STRING locally using generate_session.py."
            )
        print()
        print("=" * 60)
        print("  Two-factor authentication (cloud password) is enabled.")
        print("  Your password will NOT be displayed, logged, or stored.")
        print("=" * 60)
        password = getpass.getpass("Enter your 2FA cloud password: ")

    try:
        await client.sign_in(password=password)
    except PasswordHashInvalidError:
        raise RuntimeError("The 2FA password you entered is incorrect.")
    except FloodWaitError as e:
        raise RuntimeError(f"Telegram rate-limited 2FA. Wait {e.seconds}s and try again.")
    finally:
        # Best-effort wipe: overwrite the local variable with empty string
        # so it isn't lingering in memory until GC.
        if password:
            password = "0" * len(password)
        del password


async def safe_disconnect(client: Optional[TelegramClient]) -> None:
    """Safely disconnect the client, ignoring errors during shutdown."""
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception as e:  # noqa: BLE001
        logger.warning("Error during disconnect (safe to ignore): %s", e)
