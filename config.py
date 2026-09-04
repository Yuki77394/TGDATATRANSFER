"""Configuration loader.

Loads credentials and runtime settings from a .env file.
Secrets (API_HASH, phone, password) are NEVER logged.
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


class Config:
    """Holds all configuration values loaded from the environment."""

    def __init__(self, env_path: Path | None = None) -> None:
        if env_path is None:
            env_path = Path(__file__).resolve().parent / ".env"
        # python-dotenv silently does nothing if the file is missing
        load_dotenv(dotenv_path=str(env_path))

        # --- Credentials (NEVER log these) ---
        self.api_id: int = _as_int(os.getenv("API_ID"), 0)
        self.api_hash: str = os.getenv("API_HASH", "").strip()
        self.phone: str = os.getenv("PHONE", "").strip()
        self.session_name: str = os.getenv("SESSION_NAME", "telegram_backup_session").strip()

        # --- Scope ---
        self.backup_saved_messages: bool = _as_bool(os.getenv("BACKUP_SAVED_MESSAGES"), True)
        self.backup_private_chats: bool = _as_bool(os.getenv("BACKUP_PRIVATE_CHATS"), True)
        self.backup_groups: bool = _as_bool(os.getenv("BACKUP_GROUPS"), False)
        self.backup_channels: bool = _as_bool(os.getenv("BACKUP_CHANNELS"), False)
        # Whether to include bot conversations in private chat backup (default: yes)
        self.include_bots: bool = _as_bool(os.getenv("INCLUDE_BOTS"), True)

        # --- Media ---
        self.backup_media: bool = _as_bool(os.getenv("BACKUP_MEDIA"), True)
        self.download_photos: bool = _as_bool(os.getenv("DOWNLOAD_PHOTOS"), True)
        self.download_videos: bool = _as_bool(os.getenv("DOWNLOAD_VIDEOS"), True)
        self.download_documents: bool = _as_bool(os.getenv("DOWNLOAD_DOCUMENTS"), True)
        self.download_audio: bool = _as_bool(os.getenv("DOWNLOAD_AUDIO"), True)
        self.download_voice: bool = _as_bool(os.getenv("DOWNLOAD_VOICE"), True)

        self.max_photo_size: int = _as_int(os.getenv("MAX_PHOTO_SIZE_MB"), 0) * 1024 * 1024
        self.max_video_size: int = _as_int(os.getenv("MAX_VIDEO_SIZE_MB"), 0) * 1024 * 1024
        self.max_document_size: int = _as_int(os.getenv("MAX_DOCUMENT_SIZE_MB"), 0) * 1024 * 1024

        # --- Reliability ---
        self.max_retries: int = max(1, _as_int(os.getenv("MAX_RETRIES"), 3))
        self.checkpoint_every: int = max(1, _as_int(os.getenv("CHECKPOINT_EVERY"), 50))

        # --- Paths ---
        base_dir_raw = os.getenv("BACKUP_DIR", "telegram_backup").strip()
        base = Path(base_dir_raw)
        if not base.is_absolute():
            base = Path(__file__).resolve().parent / base
        self.base_dir: Path = base

        self.saved_messages_dir: Path = self.base_dir / "Saved_Messages"
        self.private_chats_dir: Path = self.base_dir / "Private_Chats"
        self.groups_dir: Path = self.base_dir / "Groups"
        self.channels_dir: Path = self.base_dir / "Channels"

        self.state_file: Path = self.base_dir / "backup_state.json"
        self.metadata_file: Path = self.base_dir / "backup_metadata.json"
        self.errors_log: Path = self.base_dir / "errors.log"
        self.session_file: Path = self.base_dir / f"{self.session_name}.session"

    def validate(self) -> None:
        """Validate that required credentials are present. Raises ValueError if missing."""
        missing = []
        if not self.api_id:
            missing.append("API_ID")
        if not self.api_hash:
            missing.append("API_HASH")
        if not self.phone:
            missing.append("PHONE")
        if missing:
            raise ValueError(
                "Missing required configuration: " + ", ".join(missing) +
                ". Please copy .env.example to .env and fill in your values."
            )

    def ensure_directories(self) -> None:
        """Create the base directory tree."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_saved_messages:
            self.saved_messages_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_private_chats:
            self.private_chats_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_groups:
            self.groups_dir.mkdir(parents=True, exist_ok=True)
        if self.backup_channels:
            self.channels_dir.mkdir(parents=True, exist_ok=True)

    def safe_summary(self) -> dict:
        """Return a safe (non-secret) summary of the configuration for logging."""
        return {
            "backup_saved_messages": self.backup_saved_messages,
            "backup_private_chats": self.backup_private_chats,
            "backup_groups": self.backup_groups,
            "backup_channels": self.backup_channels,
            "include_bots": self.include_bots,
            "backup_media": self.backup_media,
            "max_retries": self.max_retries,
            "checkpoint_every": self.checkpoint_every,
            "base_dir": str(self.base_dir),
            "api_id": self.api_id,  # api_id is not secret (it's a public app identifier)
            "phone_set": bool(self.phone),
            "api_hash_set": bool(self.api_hash),
        }
