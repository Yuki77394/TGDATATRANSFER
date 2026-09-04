"""Media downloader.

Downloads media attached to Telegram messages, classifying each item into
one of: photos / videos / documents / audio / voice / other.

Design goals
------------
- **Idempotent**: never re-download a file that's already on disk AND
  passes integrity checks.
- **Resumable**: media keys are recorded in the state manager so a restart
  skips them. Failed media is tracked separately so it can be retried.
- **Robust**: a single failed download does not abort the whole chat.
- **Polite**: ``FloodWaitError`` is re-raised so the caller can sleep and
  resume; other errors are retried with exponential backoff, then logged.
- **Integrity**: a file is only considered "downloaded" if it exists, has
  non-zero size, AND (when Telegram reports a size) matches the expected
  size. Partial files from interrupted downloads are deleted before retry.
- **Original filenames** are preserved when Telegram provides them;
  otherwise a deterministic name is derived from the message id.
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Optional

from telethon.errors import FloodWaitError
from telethon.tl.types import (
    Document,
    DocumentAttributeAnimated,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGame,
    MessageMediaInvoice,
    MessageMediaPoll,
    MessageMediaDice,
)

from config import Config
from state_manager import StateManager
from utils import sanitize_filename

logger = logging.getLogger("telegram_backup.media")


# Mapping from media category -> config flag attribute name
_CATEGORY_CONFIG_FLAG = {
    "photos": "download_photos",
    "videos": "download_videos",
    "documents": "download_documents",
    "audio": "download_audio",
    "voice": "download_voice",
}

# Mapping from mime type -> file extension, used when no filename is provided
_MIME_EXT_MAP = {
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/x-matroska": ".mkv",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/x-wav": ".wav",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
    "application/x-7z-compressed": ".7z",
    "application/x-rar-compressed": ".rar",
    "application/x-tar": ".tar",
    "application/gzip": ".gz",
    "application/octet-stream": "",
    "text/plain": ".txt",
    "application/json": ".json",
}


class MediaDownloader:
    """Handles per-message media classification and download."""

    def __init__(
        self,
        client: Any,
        config: Config,
        state: StateManager,
        error_logger: logging.Logger,
    ) -> None:
        self.client = client
        self.config = config
        self.state = state
        self.error_logger = error_logger

        self.stats: dict[str, int] = {
            "photos": 0,
            "videos": 0,
            "documents": 0,
            "audio": 0,
            "voice": 0,
            "other": 0,
            "failed": 0,
            "skipped": 0,
            "retried_success": 0,
        }

    # --------------------------------------------------------------- public API

    async def download_media_for_message(
        self,
        message: Any,
        media_dir: Path,
        chat_key: str,
    ) -> Optional[dict[str, Any]]:
        """Download media for a single message.

        Returns a metadata dict describing the media, or ``None`` if the
        message has no downloadable media.

        The returned dict has at minimum:
          - ``type``: one of photos/videos/documents/audio/voice/other
          - ``local_path``: relative path to the downloaded file, or None
          - ``skipped``: True if intentionally not downloaded
          - ``file_size``: int bytes if known
          - ``error``: error string if download failed
        """
        if not message or not message.media:
            return None

        # Web page previews have no real media to download
        if isinstance(message.media, MessageMediaWebPage):
            return None

        # Non-downloadable media types: just record metadata
        if isinstance(
            message.media,
            (
                MessageMediaContact,
                MessageMediaGeo,
                MessageMediaGame,
                MessageMediaInvoice,
                MessageMediaPoll,
                MessageMediaDice,
            ),
        ):
            return {
                "type": "other",
                "local_path": None,
                "skipped": True,
                "reason": "non_downloadable_media_type",
            }

        category = self._categorize_media(message.media)
        if category is None:
            return None

        doc = getattr(message.media, "document", None)
        photo = getattr(message.media, "photo", None)
        media_id = self._media_id(message)
        media_key = f"{message.id}:{media_id}" if media_id is not None else f"{message.id}:none"

        # Determine expected file size for integrity check
        expected_size = self._get_expected_size(doc, photo)

        # Determine file size for early skip
        file_size = expected_size

        # Apply size limits
        max_size = self._max_size_for_category(category)
        if max_size > 0 and file_size > max_size:
            logger.info(
                "Skipping media msg_id=%s in %s: size %s exceeds limit %s",
                message.id, chat_key, file_size, max_size,
            )
            self.stats["skipped"] += 1
            return {
                "type": category,
                "local_path": None,
                "skipped": True,
                "reason": "size_exceeds_limit",
                "file_size": file_size,
            }

        # Apply category filter
        if not self._should_download(category):
            self.stats["skipped"] += 1
            return {
                "type": category,
                "local_path": None,
                "skipped": True,
                "reason": "category_disabled",
                "file_size": file_size,
            }

        # Build target path
        category_dir = media_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        filename = self._build_filename(message, category, doc)
        target_path = category_dir / filename

        # ALREADY-DOWNLOADED CHECK 1: state says downloaded AND file passes integrity
        if media_id is not None and self.state.is_media_downloaded(chat_key, media_key):
            if self._verify_file_integrity(target_path, expected_size):
                return {
                    "type": category,
                    "local_path": str(target_path.relative_to(self.config.base_dir)),
                    "file_size": target_path.stat().st_size,
                    "already_downloaded": True,
                }
            # State says downloaded but file is missing/corrupt - fall through to re-download
            logger.warning(
                "Media %s marked as downloaded but file fails integrity; re-downloading.",
                media_key,
            )
            # Clear the stale state entry
            # (We don't call clear_failed_media because it's not failed; just re-download)

        # ALREADY-DOWNLOADED CHECK 2: file exists on disk and passes integrity
        if self._verify_file_integrity(target_path, expected_size):
            if media_id is not None:
                self.state.mark_media_downloaded(chat_key, media_key)
            return {
                "type": category,
                "local_path": str(target_path.relative_to(self.config.base_dir)),
                "file_size": target_path.stat().st_size,
                "already_downloaded": True,
            }

        # If a partial/zero-byte file exists from a previous interrupted
        # download, delete it before retrying.
        if target_path.exists() and not self._verify_file_integrity(target_path, expected_size):
            logger.info("Removing partial/corrupt file %s before re-download.", target_path)
            try:
                target_path.unlink()
            except OSError as e:
                logger.warning("Could not remove partial file %s: %s", target_path, e)

        # Download with retries (FloodWait propagates to caller)
        result = await self._download_with_retries(
            message, target_path, chat_key, media_key, media_id, expected_size, category
        )
        return result

    # --------------------------------------------------------------- categorize

    def _categorize_media(self, media: Any) -> Optional[str]:
        """Classify a media object into one of our category strings."""
        if isinstance(media, MessageMediaPhoto):
            return "photos"

        if isinstance(media, MessageMediaDocument):
            doc = media.document
            if not isinstance(doc, Document):
                return None

            # Inspect attributes first - more reliable than mime type
            has_video = False
            has_audio = False
            is_voice = False
            is_animated = False

            for attr in doc.attributes or []:
                if isinstance(attr, DocumentAttributeVideo):
                    has_video = True
                    # Round video messages are treated like voice/video notes
                    if getattr(attr, "round_message", False):
                        is_voice = True
                elif isinstance(attr, DocumentAttributeAudio):
                    has_audio = True
                    if getattr(attr, "voice", False):
                        is_voice = True
                elif isinstance(attr, DocumentAttributeAnimated):
                    is_animated = True

            # Decision priority:
            # 1. Voice notes -> voice
            # 2. Round video messages -> voice (video note category)
            # 3. Animated GIFs (no audio) -> videos
            # 4. Audio files -> audio
            # 5. Video files -> videos
            # 6. Anything else -> documents
            if is_voice:
                return "voice"
            if has_video or is_animated:
                return "videos"
            if has_audio:
                return "audio"
            # Fall back to mime type
            mime = (doc.mime_type or "").lower()
            if mime.startswith("video/"):
                return "videos"
            if mime.startswith("audio/"):
                return "audio"
            if mime.startswith("image/"):
                # Sticker or animated image without explicit attribute
                return "photos"
            return "documents"

        return "other"

    # --------------------------------------------------------------- filename

    def _build_filename(self, message: Any, category: str, doc: Optional[Document]) -> str:
        """Build a safe, dedup-friendly filename.

        Pattern: ``<sanitized_original_stem>_msg<id><ext>``
        Fallback when no original name: ``msg_<id><ext>``

        Including ``msg<id>`` in every filename ensures no two messages
        can produce colliding filenames even if they share an original name.
        """
        original_name = None
        ext = ""

        # Try to find an original filename
        if isinstance(doc, Document):
            for attr in doc.attributes or []:
                if isinstance(attr, DocumentAttributeFilename) and attr.file_name:
                    original_name = attr.file_name
                    break

        if original_name:
            stem, ext = os.path.splitext(original_name)
            stem = sanitize_filename(stem, max_length=80, default="file")
            ext = sanitize_filename(ext, max_length=20, default="")
            if ext and not ext.startswith("."):
                ext = "." + ext
            if not ext:
                # No extension from original name - derive from mime type
                ext = self._ext_from_mime(doc) if isinstance(doc, Document) else ""
            return f"{stem}_msg{message.id}{ext}"

        # No original name - derive from category and mime type
        if category == "photos":
            ext = ".jpg"
        elif isinstance(doc, Document) and doc.mime_type:
            ext = _MIME_EXT_MAP.get(doc.mime_type.lower(), "")
        elif category == "voice":
            ext = ".ogg"
        elif category == "audio":
            ext = ".mp3"
        elif category == "videos":
            ext = ".mp4"

        return f"msg_{message.id}{ext}"

    def _ext_from_mime(self, doc: Document) -> str:
        """Get file extension from document mime type."""
        if doc.mime_type:
            return _MIME_EXT_MAP.get(doc.mime_type.lower(), "")
        return ""

    # --------------------------------------------------------------- download

    async def _download_with_retries(
        self,
        message: Any,
        target_path: Path,
        chat_key: str,
        media_key: str,
        media_id: Optional[int],
        expected_size: int,
        category: str,
    ) -> dict[str, Any]:
        """Attempt to download media, retrying on transient errors.

        FloodWaitError is propagated to the caller (we don't sleep here so
        the orchestrator can show progress and update state).
        """
        last_error: Optional[Exception] = None
        was_previously_failed = (
            media_id is not None and self.state.is_media_failed(chat_key, media_key)
        )

        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Telethon's download_media accepts a path (string or Path).
                # If the file already exists, Telethon would overwrite it,
                # but we already removed any partial file above.
                result = await self.client.download_media(message, file=str(target_path))
                if result is None:
                    # No media was actually downloaded (e.g. empty document)
                    self.stats["failed"] += 1
                    err_msg = "telethon returned no media"
                    if media_id is not None:
                        self.state.mark_media_failed(chat_key, media_key)
                    return {
                        "type": "other",
                        "local_path": None,
                        "error": err_msg,
                    }

                # Verify the file landed on disk and passes integrity checks
                if not self._verify_file_integrity(target_path, expected_size):
                    # Download returned success but file is missing/empty/corrupt
                    self.stats["failed"] += 1
                    err_msg = "file missing, empty, or size mismatch after download"
                    # Clean up partial file
                    try:
                        if target_path.exists():
                            target_path.unlink()
                    except OSError:
                        pass
                    if media_id is not None:
                        self.state.mark_media_failed(chat_key, media_key)
                    return {
                        "type": "other",
                        "local_path": None,
                        "error": err_msg,
                    }

                # SUCCESS
                if media_id is not None:
                    self.state.mark_media_downloaded(chat_key, media_key)
                    if was_previously_failed:
                        self.stats["retried_success"] += 1
                        logger.info("Previously-failed media %s succeeded on retry.", media_key)

                # Increment category stat
                self.stats[category] = self.stats.get(category, 0) + 1

                actual_size = target_path.stat().st_size
                return {
                    "type": category,
                    "local_path": str(target_path.relative_to(self.config.base_dir)),
                    "file_size": actual_size,
                }

            except FloodWaitError:
                # Don't retry here - let the orchestrator handle it.
                # IMPORTANT: do NOT mark media as failed or downloaded.
                raise

            except Exception as e:  # noqa: BLE001
                last_error = e
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Media download attempt %d/%d failed for msg_id=%s in %s: %s. Sleeping %ss.",
                    attempt, self.config.max_retries, message.id, chat_key, e, wait,
                )
                # Clean up any partial file before retrying
                try:
                    if target_path.exists():
                        target_path.unlink()
                except OSError:
                    pass
                # Don't sleep on the last attempt
                if attempt < self.config.max_retries:
                    await asyncio.sleep(wait)

        # All retries exhausted
        self.stats["failed"] += 1
        err_msg = f"{type(last_error).__name__}: {last_error}" if last_error else "unknown"
        self.error_logger.error(
            "Media download failed for chat_key=%s message_id=%s media_key=%s: %s",
            chat_key, message.id, media_key, err_msg,
        )
        if media_id is not None:
            self.state.mark_media_failed(chat_key, media_key)
        return {
            "type": "other",
            "local_path": None,
            "error": err_msg,
            "media_key": media_key,
        }

    # --------------------------------------------------------------- integrity

    def _verify_file_integrity(
        self, path: Path, expected_size: int
    ) -> bool:
        """Check that a downloaded file exists, is non-empty, and (if we
        know the expected size) matches it exactly.

        We do NOT hash the file because Telegram doesn't expose content
        hashes for arbitrary media in a way that's easy to compare. Size
        matching is the best integrity check available without re-downloading.
        """
        if not path.exists():
            return False
        try:
            stat = path.stat()
        except OSError:
            return False
        if stat.st_size == 0:
            return False
        # If Telegram told us the expected size, require an exact match.
        # This catches partial downloads where the file exists but is
        # truncated.
        if expected_size > 0 and stat.st_size != expected_size:
            logger.warning(
                "File %s size %d != expected %d; treating as corrupt.",
                path, stat.st_size, expected_size,
            )
            return False
        return True

    def _get_expected_size(self, doc: Optional[Document], photo: Any) -> int:
        """Get the expected file size in bytes, or 0 if unknown."""
        if isinstance(doc, Document):
            return int(doc.size or 0)
        if photo is not None:
            # Photo size: Telethon exposes the largest photo size's size
            # attribute via the photo object's `size` attribute (if present)
            # or via the largest PhotoSize in `sizes`.
            photo_size = getattr(photo, "size", None)
            if photo_size:
                return int(photo_size)
            sizes = getattr(photo, "sizes", None) or []
            if sizes:
                # Find the largest size
                try:
                    largest = max(
                        (s for s in sizes if hasattr(s, "size") and s.size),
                        key=lambda s: s.size,
                        default=None,
                    )
                    if largest is not None:
                        return int(largest.size)
                except (TypeError, ValueError):
                    pass
        return 0

    # --------------------------------------------------------------- helpers

    def _media_id(self, message: Any) -> Optional[int]:
        """Get a stable Telegram-side ID for the media object.

        Used together with the message id to form a dedup key.
        Returns None for media without a stable id (rare).
        """
        media = message.media
        if media is None:
            return None
        photo = getattr(media, "photo", None)
        if photo is not None:
            return getattr(photo, "id", None)
        doc = getattr(media, "document", None)
        if isinstance(doc, Document):
            return getattr(doc, "id", None)
        return None

    def _should_download(self, category: str) -> bool:
        if not self.config.backup_media:
            return False
        flag_name = _CATEGORY_CONFIG_FLAG.get(category)
        if flag_name is None:
            return True  # 'other' - allow
        return bool(getattr(self.config, flag_name, True))

    def _max_size_for_category(self, category: str) -> int:
        if category == "photos":
            return self.config.max_photo_size
        if category == "videos":
            return self.config.max_video_size
        if category in ("documents", "audio", "voice", "other"):
            return self.config.max_document_size
        return 0
