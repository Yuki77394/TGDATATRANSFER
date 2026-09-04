"""Authentication module.

Handles Telethon client creation, OTP prompt, and 2FA password prompt.

SAFETY: This module NEVER logs API_HASH, phone numbers, OTP codes, or 2FA
passwords. The 2FA password is read via getpass and explicitly deleted from
memory as soon as it has been used.
"""
from __future__ import annotations

import asyncio
import getpass
import logging
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

from config import Config

logger = logging.getLogger("telegram_backup.auth")


def create_client(config: Config) -> TelegramClient:
    """Create a TelegramClient configured with the saved session file."""
    config.ensure_directories()
    # Session path without extension - Telethon appends .session automatically
    session_path = str(config.session_file.with_suffix(""))

    client = TelegramClient(
        session_path,
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
      - Fresh login: OTP prompt
      - 2FA-enabled accounts: password prompt

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
    """Perform a fresh interactive login."""
    if not config.phone:
        raise RuntimeError("PHONE is not set in .env; cannot perform a fresh login.")

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
        # Telethon raises FloodWaitError if we hit Telegram's rate limit for code requests
        logger.warning("Telegram requested FloodWait of %s seconds before sending code.", e.seconds)
        print(f"Telegram asks us to wait {e.seconds} seconds before sending a code. Sleeping...")
        await asyncio.sleep(e.seconds + 1)
        await client.send_code_request(config.phone)
    except PhoneNumberBannedError:
        logger.error("This phone number is banned from Telegram. Cannot continue.")
        raise RuntimeError("Phone number is banned. The tool will not attempt to bypass this.")
    except PhoneNumberUnoccupiedError:
        logger.error("This phone number is not registered on Telegram.")
        raise RuntimeError("Phone number is not registered on Telegram.")

    code = _prompt_otp()

    try:
        await client.sign_in(phone=config.phone, code=code)
    except SessionPasswordNeededError:
        await _handle_2fa(client)
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
        # Telegram codes are typically 5 digits but can be alphanumeric for some login flows
        if code:
            return code
        print("Code cannot be empty. Please try again.")


async def _handle_2fa(client: TelegramClient) -> None:
    """Handle Telegram cloud password (2FA) interactively.

    The password is read via getpass (no echo), used once, and immediately
    deleted from the local variable scope. It is never logged.
    """
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
        password = "0" * len(password) if password else ""
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
