"""Entry point for the Saved Messages migration tool (Railway deployment).

Migrates Saved Messages from a SOURCE Telegram account to a TARGET
Telegram account using authorized Telethon StringSessions.

Usage:
    python migrate.py

Required environment variables:
    API_ID           Telegram app ID
    API_HASH         Telegram app hash
    SOURCE_SESSION   Telethon StringSession for the source account
    TARGET_SESSION   Telethon StringSession for the target account
    DATA_DIR         Persistent volume path (default: /app/data)

The application runs as a long-running worker. It processes Saved Messages
oldest-first, downloads media to DATA_DIR/media/, uploads to the target
account's Saved Messages, and persists state to DATA_DIR/state/.

On Railway, a persistent Volume MUST be mounted at /app/data for crash
recovery to work. Without it, state is lost on every restart.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

# Make sure local modules are importable when running from any cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))

from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

from migration_config import MigrationConfig  # noqa: E402
from migration_state import MigrationStateManager  # noqa: E402
from migrator import SavedMessagesMigrator  # noqa: E402
from utils import setup_logging  # noqa: E402


async def validate_client(client: TelegramClient, label: str) -> int:
    """Connect and validate a Telethon client. Returns the user ID.

    Raises RuntimeError if the session is invalid or unauthorized.
    """
    logger = logging.getLogger("telegram_backup.migrate")
    logger.info("Validating %s session...", label)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError(
            f"{label} session is not authorized. The StringSession may be "
            "invalid, expired, or revoked. Generate a new one locally."
        )
    me = await client.get_me()
    display = " ".join(p for p in [me.first_name, me.last_name] if p)
    logger.info(
        "%s authenticated: user id=%s (%s)",
        label, me.id, display or "<no name>",
    )
    print(f"  {label}: user id={me.id} ({display or '<no name>'})")
    return me.id


async def main() -> int:
    # --- Load and validate configuration ---
    config = MigrationConfig()
    try:
        config.validate()
        config.validate_api_id()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        print("Required environment variables:", file=sys.stderr)
        print("  API_ID, API_HASH, SOURCE_SESSION, TARGET_SESSION, DATA_DIR",
              file=sys.stderr)
        return 2

    config.ensure_directories()

    # --- Set up logging (stdout + persistent file in DATA_DIR) ---
    logger = setup_logging(config.log_file)
    error_logger = logging.getLogger("telegram_backup.errors")
    error_logger.setLevel(logging.ERROR)
    try:
        error_handler = logging.FileHandler(
            config.logs_dir / "errors.log", encoding="utf-8"
        )
        error_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        error_logger.addHandler(error_handler)
    except OSError:
        fallback = logging.StreamHandler(sys.stderr)
        fallback.setFormatter(logging.Formatter("[ERROR] %(message)s"))
        error_logger.addHandler(fallback)
    error_logger.propagate = False

    logger.info("Starting Saved Messages migration tool.")
    logger.info("Configuration: %s", config.safe_summary())

    # --- State manager (resume support) ---
    state = MigrationStateManager(config.state_file)

    # --- Create Telegram clients ---
    source_client = TelegramClient(
        StringSession(config.source_session),
        config.api_id,
        config.api_hash,
        device_model="TGMigrator-Source",
        system_version="1.0",
        app_version="1.0.0",
        lang_code="en",
        system_lang_code="en",
        connection_retries=config.max_retries,
        retry_delay=2,
        request_retries=config.max_retries,
        timeout=30,
    )
    target_client = TelegramClient(
        StringSession(config.target_session),
        config.api_id,
        config.api_hash,
        device_model="TGMigrator-Target",
        system_version="1.0",
        app_version="1.0.0",
        lang_code="en",
        system_lang_code="en",
        connection_retries=config.max_retries,
        retry_delay=2,
        request_retries=config.max_retries,
        timeout=30,
    )

    # --- Validate both sessions ---
    try:
        source_uid = await validate_client(source_client, "Source")
        target_uid = await validate_client(target_client, "Target")
    except Exception as e:
        logger.error("Session validation failed: %s: %s", type(e).__name__, e)
        print(f"\nSession validation failed: {e}", file=sys.stderr)
        print("Generate valid session strings locally and set them as "
              "SOURCE_SESSION and TARGET_SESSION.", file=sys.stderr)
        await _safe_disconnect(source_client)
        await _safe_disconnect(target_client)
        return 3

    # --- Safety: refuse same account (unless overridden) ---
    if source_uid == target_uid and not config.allow_same_account:
        logger.error(
            "Source and target are the same account (id=%s). "
            "Set ALLOW_SAME_ACCOUNT=true to override (NOT recommended).",
            source_uid,
        )
        print(f"\nERROR: Source and target are the same account (id={source_uid}).",
              file=sys.stderr)
        print("Migrating to the same account would duplicate all messages.",
              file=sys.stderr)
        print("Set ALLOW_SAME_ACCOUNT=true to override.", file=sys.stderr)
        await _safe_disconnect(source_client)
        await _safe_disconnect(target_client)
        return 4

    # --- Run migration ---
    migrator = SavedMessagesMigrator(
        source_client, target_client, config, state, error_logger
    )
    try:
        await migrator.run()
    except KeyboardInterrupt:
        print("\nInterrupted. Saving state and exiting...")
        logger.info("Interrupted by user (SIGTERM/SIGINT).")
        state.save()
        return 130
    except Exception as e:
        logger.exception("Migration failed unexpectedly.")
        print(f"\nMigration failed: {e}", file=sys.stderr)
        state.save()
        return 1
    finally:
        await _safe_disconnect(source_client)
        await _safe_disconnect(target_client)
        logger.info("Migration tool finished.")

    return 0


async def _safe_disconnect(client: TelegramClient | None) -> None:
    """Safely disconnect a Telegram client, ignoring errors."""
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception as e:  # noqa: BLE001
        logging.getLogger("telegram_backup.migrate").warning(
            "Error during disconnect (safe to ignore): %s", e
        )


def _install_signal_handlers() -> None:
    """Translate SIGTERM (Railway/Heroku shutdown) into KeyboardInterrupt
    so the async loop can shut down gracefully and persist state.

    Railway sends SIGTERM ~30 seconds before SIGKILL.
    """
    def _handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt(f"Received signal {signum}")
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except (AttributeError, ValueError, OSError):
            pass


if __name__ == "__main__":
    _install_signal_handlers()
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
        exit_code = 130
    sys.exit(exit_code)
