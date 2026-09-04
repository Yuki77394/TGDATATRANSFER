"""Entry point for the Telegram Backup Tool.

Usage:
    python main.py

Reads configuration from .env, authenticates with Telethon (prompting for
OTP / 2FA as needed), then runs the backup. State is checkpointed so the
tool can be safely interrupted and re-run.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

# Make sure local modules are importable when running from any cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))

from auth import authenticate, create_client, safe_disconnect
from backup import BackupOrchestrator
from config import Config
from state_manager import StateManager
from utils import setup_logging


async def main() -> int:
    # --- Load and validate configuration ---
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        print("Tip: copy .env.example to .env and fill in your values.", file=sys.stderr)
        return 2

    config.ensure_directories()

    # --- Set up logging (file + console) ---
    logger = setup_logging(config.errors_log.parent / "backup.log")
    # Separate error logger writes only to errors.log
    error_logger = logging.getLogger("telegram_backup.errors")
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(config.errors_log, encoding="utf-8")
    error_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    error_logger.addHandler(error_handler)
    error_logger.propagate = False

    logger.info("Starting Telegram backup tool.")
    logger.info("Configuration: %s", config.safe_summary())

    # --- State manager (resume support) ---
    state = StateManager(config.state_file)

    # --- Telethon client + auth ---
    client = create_client(config)
    try:
        await authenticate(client, config)
    except Exception as e:
        logger.error("Authentication failed: %s: %s", type(e).__name__, e)
        print(f"Authentication failed: {e}", file=sys.stderr)
        await safe_disconnect(client)
        return 3
    except KeyboardInterrupt:
        print("\nInterrupted during authentication. State saved; rerun to continue.")
        await safe_disconnect(client)
        return 130

    # --- Run backup ---
    orchestrator = BackupOrchestrator(client, config, state, error_logger)
    try:
        await orchestrator.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving state and exiting...")
        logger.info("Interrupted by user (KeyboardInterrupt).")
        state.save()
        return 130
    except Exception as e:
        logger.exception("Backup failed unexpectedly.")
        print(f"Backup failed: {e}", file=sys.stderr)
        state.save()
        return 1
    finally:
        await safe_disconnect(client)
        logger.info("Backup tool finished.")

    return 0


def _install_signal_handlers() -> None:
    """Translate SIGTERM (e.g. systemd stop) into a KeyboardInterrupt so the
    async loop can shut down gracefully and persist state."""
    def _handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt(f"Received signal {signum}")
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (AttributeError, ValueError):
        pass  # not available on all platforms


if __name__ == "__main__":
    _install_signal_handlers()
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
        exit_code = 130
    sys.exit(exit_code)
