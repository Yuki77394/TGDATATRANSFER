"""Migration configuration loader.

Loads credentials and runtime settings from environment variables for the
Saved Messages migration tool (source → target).

Required environment variables:
  - API_ID           Telegram app ID (shared by both clients)
  - API_HASH         Telegram app hash (shared by both clients)
  - SOURCE_SESSION   Telethon StringSession for the SOURCE account
  - TARGET_SESSION   Telethon StringSession for the TARGET account
  - DATA_DIR         Persistent volume mount path (default: /app/data)

Optional environment variables:
  - MAX_RETRIES          (default: 3)
  - CHECKPOINT_EVERY     (default: 10)
  - ALLOW_SAME_ACCOUNT   (default: false) — safety override
  - DELETE_AFTER_UPLOAD  (default: false) — delete media files after upload

SAFETY: Secrets (API_HASH, session strings) are NEVER logged. The
``safe_summary()`` method only returns non-sensitive booleans.
"""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}


def _as_int(value: str | None, default: int = 0) -> int:
    try:
        return int(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


class MigrationConfig:
    """Holds all configuration for the Saved Messages migration."""

    def __init__(self, env_path: Path | None = None) -> None:
        if env_path is None:
            env_path = Path(__file__).resolve().parent / ".env"
        load_dotenv(dotenv_path=str(env_path))

        # --- Credentials (NEVER log these) ---
        self.api_id: int = _as_int(os.getenv("API_ID"), 0)
        self.api_hash: str = os.getenv("API_HASH", "").strip()
        self.source_session: str = os.getenv("SOURCE_SESSION", "").strip()
        self.target_session: str = os.getenv("TARGET_SESSION", "").strip()

        # --- Persistent data directory (Railway Volume) ---
        data_dir_raw = os.getenv("DATA_DIR", "/app/data").strip()
        self.data_dir: Path = Path(data_dir_raw)

        # Subdirectories under DATA_DIR
        self.state_dir: Path = self.data_dir / "state"
        self.media_dir: Path = self.data_dir / "media"
        self.failed_dir: Path = self.data_dir / "failed"
        self.logs_dir: Path = self.data_dir / "logs"

        # State files
        self.db_path: Path = self.state_dir / "migration.db"
        self.state_file: Path = self.state_dir / "migration_state.json"  # legacy, for migration
        self.failed_messages_file: Path = self.failed_dir / "failed_messages.jsonl"
        self.log_file: Path = self.logs_dir / "migration.log"

        # --- Reliability ---
        self.max_retries: int = max(1, _as_int(os.getenv("MAX_RETRIES"), 5))
        self.checkpoint_every: int = max(1, _as_int(os.getenv("CHECKPOINT_EVERY"), 10))

        # --- Safety overrides ---
        self.allow_same_account: bool = _as_bool(os.getenv("ALLOW_SAME_ACCOUNT"), False)
        self.delete_after_upload: bool = _as_bool(os.getenv("DELETE_AFTER_UPLOAD"), False)

        # --- Reconciliation ---
        # When recovering from an 'uploading' crash, check the last N target
        # Saved Messages for a potential duplicate before re-uploading.
        self.reconciliation_window: int = max(
            1, _as_int(os.getenv("RECONCILIATION_WINDOW"), 20)
        )

    def validate(self) -> None:
        """Validate that required configuration is present. Raises ValueError."""
        missing = []
        if not self.api_id:
            missing.append("API_ID")
        if not self.api_hash:
            missing.append("API_HASH")
        if not self.source_session:
            missing.append("SOURCE_SESSION")
        if not self.target_session:
            missing.append("TARGET_SESSION")
        if missing:
            raise ValueError(
                "Missing required configuration: " + ", ".join(missing) +
                ". Set these as environment variables (or Railway Config Vars)."
            )

    def validate_api_id(self) -> None:
        """Validate that API_ID is a positive integer."""
        if self.api_id <= 0:
            raise ValueError(
                f"API_ID must be a positive integer (got {self.api_id}). "
                "Get your API credentials from https://my.telegram.org"
            )

    def ensure_directories(self) -> None:
        """Create the DATA_DIR directory tree. Called at startup."""
        for d in (self.data_dir, self.state_dir, self.media_dir,
                  self.failed_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)

    def safe_summary(self) -> dict:
        """Return a safe (non-secret) summary for logging."""
        return {
            "api_id": self.api_id,
            "api_hash_set": bool(self.api_hash),
            "source_session_set": bool(self.source_session),
            "target_session_set": bool(self.target_session),
            "data_dir": str(self.data_dir),
            "db_path": str(self.db_path),
            "max_retries": self.max_retries,
            "checkpoint_every": self.checkpoint_every,
            "allow_same_account": self.allow_same_account,
            "delete_after_upload": self.delete_after_upload,
            "reconciliation_window": self.reconciliation_window,
        }
