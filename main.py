"""Entry point for the Telegram Backup Tool.

Usage:
    python main.py            # local run (reads .env)
    python main.py            # Heroku worker (reads Config Vars)

Reads configuration from environment variables (``.env`` locally,
Heroku Config Vars on Heroku), authenticates with Telethon, then runs
the backup. State is checkpointed so the tool can be safely interrupted
and re-run.

Heroku notes
------------
- The Worker process is defined in ``Procfile`` as ``worker: python main.py``.
- On Heroku, set ``SESSION_STRING`` as a Config Var (generate it locally
  with ``python generate_session.py``).
- The dyno filesystem is EPHEMERAL — backup files are lost on restart.
  See README for details.
- SIGTERM (sent by Heroku on shutdown) is handled gracefully: state is
  saved and the client disconnects cleanly.
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
        if config.is_heroku:
            print("Tip: set the missing values as Heroku Config Vars:", file=sys.stderr)
            print("  heroku config:set API_ID=... API_HASH=... PHONE=...", file=sys.stderr)
            print("  heroku config:set SESSION_STRING=...  # generate locally", file=sys.stderr)
        else:
            print("Tip: copy .env.example to .env and fill in your values.", file=sys.stderr)
        return 2

    config.ensure_directories()

    # --- Set up logging (file + stdout) ---
    # On Heroku, stdout is captured by the log drain; the file is best-effort.
    logger = setup_logging(config.errors_log.parent / "backup.log")
    # Separate error logger writes only to errors.log (best-effort on Heroku)
    error_logger = logging.getLogger("telegram_backup.errors")
    error_logger.setLevel(logging.ERROR)
    try:
        error_handler = logging.FileHandler(config.errors_log, encoding="utf-8")
        error_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        error_logger.addHandler(error_handler)
    except OSError:
        # Can't write errors.log (ephemeral/read-only fs) — use stdout fallback
        fallback = logging.StreamHandler(sys.stderr)
        fallback.setFormatter(logging.Formatter("[ERROR] %(message)s"))
        error_logger.addHandler(fallback)
    error_logger.propagate = False

    logger.info("Starting Telegram backup tool.")
    logger.info("Configuration: %s", config.safe_summary())

    # --- Heroku ephemeral storage warning ---
    if config.is_heroku:
        logger.warning(
            "Running on Heroku: the dyno filesystem is EPHEMERAL. "
            "Backup files, state, and logs will be LOST on dyno restart/redeploy. "
            "This is NOT a persistent backup solution. See README for details."
        )
        if not config.session_string:
            logger.warning(
                "SESSION_STRING is not set. If fresh auth is needed, the worker "
                "will fail (no interactive terminal on Heroku). Generate a session "
                "string locally with: python generate_session.py"
            )

    # --- State manager (resume support) ---
    state = StateManager(config.state_file)

    # --- Telethon client + auth ---
    client = create_client(config)
    try:
        await authenticate(client, config)
    except Exception as e:
        logger.error("Authentication failed: %s: %s", type(e).__name__, e)
        print(f"Authentication failed: {e}", file=sys.stderr)
        if config.is_heroku and "SESSION_STRING" in str(e):
            print("Tip: generate a session string locally with "
                  "'python generate_session.py' and set it as SESSION_STRING.",
                  file=sys.stderr)
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
        logger.info("Interrupted by user (KeyboardInterrupt / SIGTERM).")
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
    """Translate SIGTERM (sent by Heroku on dyno shutdown) into a
    KeyboardInterrupt so the async loop can shut down gracefully and
    persist state.

    Heroku sends SIGTERM ~30 seconds before forcing SIGKILL. This gives
    us time to save state and disconnect cleanly.
    """
    def _handler(signum, frame):  # noqa: ANN001
        raise KeyboardInterrupt(f"Received signal {signum}")
    for sig in (signal.SIGTERM,):
        try:
            signal.signal(sig, _handler)
        except (AttributeError, ValueError, OSError):
            pass  # not available on all platforms


if __name__ == "__main__":
    _install_signal_handlers()
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
        exit_code = 130
    sys.exit(exit_code)
