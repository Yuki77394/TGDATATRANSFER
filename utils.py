"""Utility helpers: filename sanitization, logging setup, safe JSON writes.

Filename sanitization is designed to be safe on Windows, Linux, and Android
(which uses FAT-like naming for SD cards and has a 255-byte filename limit).
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

# Windows reserved base names (case-insensitive)
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}

# Characters forbidden in filenames across common filesystems
# Windows: \ / : * ? " < > |  and control chars (0x00-0x1F)
# Also strip leading/trailing spaces and dots (Windows quirk)
_FORBIDDEN_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def sanitize_filename(name: str, max_length: int = 80, default: str = "unnamed") -> str:
    """Sanitize a filename for cross-platform safety.

    - NFC-normalizes unicode
    - replaces forbidden characters with underscore
    - strips trailing dots/spaces (Windows)
    - avoids Windows reserved names (CON, PRN, NUL, COM1.., LPT1..)
    - collapses repeated underscores
    - truncates to max_length preserving extension
    """
    if not name:
        return default

    # Normalize unicode form so the same string looks identical on all platforms
    name = unicodedata.normalize("NFC", name)

    # Replace forbidden characters
    name = _FORBIDDEN_RE.sub("_", name)

    # Strip leading/trailing whitespace and dots (Windows quirk)
    name = name.strip(" .")

    if not name:
        return default

    # Split extension (last . only, to handle names like "archive.tar.gz")
    stem, ext = os.path.splitext(name)

    # Collapse multiple underscores produced by substitutions.
    # Only strip TRAILING underscores - leading underscores are significant
    # because we may add one below to escape Windows reserved names.
    stem = re.sub(r"_+", "_", stem).rstrip("_")
    ext = re.sub(r"_+", "_", ext).strip("_")

    if not stem:
        stem = default

    # Handle Windows reserved names (must come AFTER stripping so the
    # protective underscore isn't removed).
    if stem.upper() in _WINDOWS_RESERVED:
        stem = f"_{stem}"

    # Truncate stem if total length exceeds max_length
    if len(stem) + len(ext) > max_length:
        stem = stem[: max(1, max_length - len(ext))]

    result = stem + ext
    return result or default


def safe_chat_dir_name(chat_id: int | str, display_name: str, max_length: int = 60) -> str:
    """Build a folder name for a chat, embedding the numeric chat ID.

    Format: ``<sanitized_display_name>_<chat_id>``

    The numeric ID prevents collisions when two chats have identical
    display names (e.g. two contacts both named "John").
    """
    safe = sanitize_filename(display_name, max_length=max_length, default="chat")
    # Strip trailing underscores from the sanitized name so we don't end up
    # with double underscores like "John__123" when the name already ends
    # with an underscore after sanitization.
    safe = safe.rstrip("_")
    return f"{safe}_{chat_id}"


def get_file_extension(filename: str) -> str:
    """Return the lowercase extension (including dot) of a filename."""
    _, ext = os.path.splitext(filename)
    return ext.lower()


def setup_logging(log_file: Path, level: int = logging.INFO) -> logging.Logger:
    """Configure a project-wide logger that writes to a file (UTF-8).

    Returns the named logger "telegram_backup". Handlers are added only once
    so repeated calls don't create duplicate handlers.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("telegram_backup")
    logger.setLevel(level)

    # Remove old handlers (e.g. when reconfigured)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)

    # Also add a console handler for visibility, but only at WARNING+ to keep stdout clean
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    console.setLevel(logging.WARNING)
    logger.addHandler(console)

    # Don't propagate to root logger (avoid duplicate messages)
    logger.propagate = False
    return logger


def safe_write_json(path: Path, data: Any) -> None:
    """Atomically write JSON to a file by writing to a temp file then renaming.

    This prevents partial writes from corrupting the file on crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp_path, path)


def human_size(num_bytes: int | None) -> str:
    """Format a byte count as a human-readable string."""
    if num_bytes is None:
        return "unknown"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    units = ["KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        size /= 1024.0
        if size < 1024:
            return f"{size:.1f} {unit}"
    return f"{size:.1f} PB"


def truncate_text(text: str | None, max_len: int = 200) -> str:
    """Truncate a long string for display, returning '...' suffix when truncated."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."
