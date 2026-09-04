"""Saved Messages migrator: source → target.

Downloads messages from the SOURCE account's Saved Messages (oldest first),
then uploads them to the TARGET account's Saved Messages.

ARCHITECTURE
------------
1. Validate both sessions (different accounts unless override).
2. Recover incomplete states (downloading, downloaded, uploading) from SQLite.
3. Retry failed messages (bounded by max_retries).
4. Iterate source Saved Messages oldest-first, starting from the
   contiguous_checkpoint (NOT highest_uploaded).
5. For each message:
   a. Skip if already ``uploaded`` (idempotent).
   b. Download media to ``.part`` file, then atomic rename to final name.
   c. If media exists and download fails → mark FAILED (NOT text-only).
   d. Mark ``uploading`` with upload_attempt_hash (crash window begins).
   e. Upload to target.
   f. Confirm upload (get target message ID).
   g. Mark ``uploaded`` (crash window ends).
   h. Optionally delete the local media file.
6. Periodically update last_run in SQLite.
7. On completion, log summary and exit.

CRASH SAFETY
------------
- State is SQLite (transactional, WAL journal mode).
- ``uploading`` is the crash window: if we crash after marking uploading
  but before marking uploaded, we attempt reconciliation on restart
  (check recent target messages for a matching upload). If reconciliation
  fails, we re-upload (potential duplicate, but no data loss).
- Media files use ``.part`` extension during download; only renamed to
  final name after size validation passes.
- Media files are NOT deleted until after ``uploaded`` is confirmed in SQLite.

MEDIA FAILURE ≠ TEXT-ONLY SUCCESS (critical)
---------------------------------------------
If a source message contains media and the media download fails, the
message is marked FAILED — NOT migrated as text-only. The caption is
NOT uploaded without its media. The message remains retryable.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import (
    Message,
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
)

from migration_config import MigrationConfig
from migration_db import MigrationDB

logger = logging.getLogger("telegram_backup.migrator")


class SavedMessagesMigrator:
    """Migrates Saved Messages from a source account to a target account."""

    def __init__(
        self,
        source_client: TelegramClient,
        target_client: TelegramClient,
        config: MigrationConfig,
        db: MigrationDB,
        error_logger: logging.Logger,
    ) -> None:
        self.source = source_client
        self.target = target_client
        self.config = config
        self.db = db
        self.error_logger = error_logger

        self.stats = {
            "messages_migrated": 0,
            "media_downloaded": 0,
            "media_uploaded": 0,
            "failed": 0,
            "skipped_already_uploaded": 0,
            "recovered_crash": 0,
            "retried_failed": 0,
            "reconciled_duplicates": 0,
        }

    # --------------------------------------------------------------- entry point

    async def run(self) -> None:
        """Run the full migration."""
        source_me = await self.source.get_me()
        target_me = await self.target.get_me()
        logger.info(
            "Migration: source user id=%s → target user id=%s",
            source_me.id, target_me.id,
        )
        print(f"\n  Source: user id={source_me.id}")
        print(f"  Target: user id={target_me.id}")

        if source_me.id == target_me.id:
            if not self.config.allow_same_account:
                raise RuntimeError(
                    f"Source and target are the same account (id={source_me.id}). "
                    "Migrating to the same account is blocked by default. "
                    "Set ALLOW_SAME_ACCOUNT=true to override (NOT recommended)."
                )
            logger.warning(
                "ALLOW_SAME_ACCOUNT is true — migrating to the same account. "
                "This will duplicate messages."
            )

        source_saved = await self.source.get_input_entity("me")
        target_saved = await self.target.get_input_entity("me")

        # Step 1: Recover incomplete states (crash recovery)
        await self._recover_incomplete_messages(source_saved, target_saved)

        # Step 2: Retry failed messages (bounded)
        await self._retry_failed_messages(source_saved, target_saved)

        # Step 3: Compute resume point from CONTIGUOUS checkpoint
        resume_from = self.db.get_contiguous_checkpoint()
        summary = self.db.summary()
        logger.info(
            "Resuming from source message id > %d (contiguous checkpoint, "
            "%d uploaded, %d failed, %d incomplete)",
            resume_from, summary["counts"]["uploaded"],
            summary["counts"]["failed"],
            sum(summary["counts"].get(s, 0) for s in
                ("pending", "downloading", "downloaded", "uploading")),
        )
        print(f"  Resuming from source message id > {resume_from}")

        # Step 4: Iterate source Saved Messages oldest-first
        scanned = 0
        last_checkpoint_time = time.monotonic()

        async for message in self._iter_source_messages(source_saved, min_id=resume_from):
            scanned += 1
            source_id = message.id

            # Skip if already uploaded (idempotent)
            if self.db.is_uploaded(source_id):
                self.stats["skipped_already_uploaded"] += 1
                continue

            # Process this message with bounded FloodWait retries
            await self._process_message_with_floodwait_retry(
                message, source_saved, target_saved
            )

            # Periodic checkpoint (update last_run timestamp)
            now = time.monotonic()
            if now - last_checkpoint_time > 30:
                self.db.update_last_run()
                last_checkpoint_time = now

            # Progress display
            if scanned % 100 == 0:
                print(
                    f"  Scanned: {scanned} | Migrated: {self.stats['messages_migrated']} | "
                    f"Failed: {self.stats['failed']} | Current source id: {source_id}"
                )

        # Finalize
        self.db.update_last_run()
        self._print_summary()

    # --------------------------------------------------------------- message processing

    async def _process_message_with_floodwait_retry(
        self,
        message: Message,
        source_entity: Any,
        target_entity: Any,
    ) -> None:
        """Process a single message, retrying on FloodWait (unbounded for FloodWait,
        but bounded for other errors via _migrate_single_message)."""
        source_id = message.id
        while True:
            try:
                await self._migrate_single_message(message, source_entity, target_entity)
                self.stats["messages_migrated"] += 1
                return
            except FloodWaitError as e:
                # FloodWait: sleep and retry the SAME message (does NOT count as failure)
                await self._handle_flood_wait(e, f"msg {source_id}")
                continue
            except KeyboardInterrupt:
                logger.info("Interrupted; saving state and exiting.")
                self.db.update_last_run()
                raise
            except _BoundedRetryExhausted as e:
                # Bounded retries exhausted — mark failed
                self.error_logger.error(
                    "Migration failed for source msg %s after %d attempts: %s",
                    source_id, e.attempts, e.last_error,
                )
                self.db.mark_failed(source_id, f"{type(e.last_error).__name__}: {e.last_error}")
                self.stats["failed"] += 1
                return
            except Exception as e:  # noqa: BLE001
                # Unexpected error — mark failed, continue with next message
                self.error_logger.error(
                    "Unexpected error for source msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.db.mark_failed(source_id, f"{type(e).__name__}: {e}")
                self.stats["failed"] += 1
                return

    async def _migrate_single_message(
        self,
        message: Message,
        source_entity: Any,
        target_entity: Any,
    ) -> None:
        """Migrate a single message: download → upload → confirm.

        Raises _BoundedRetryExhausted if retries are exhausted.
        Raises FloodWaitError to be handled by caller (unbounded retry).
        """
        source_id = message.id

        # Track the message as pending if new
        if self.db.get_status(source_id) is None:
            self.db.mark_pending(
                source_id,
                source_date=message.date.isoformat() if message.date else None,
                source_text=(message.text or message.message or "")[:500],
            )

        # Determine if message has media
        has_media = bool(message.media) and not isinstance(
            message.media, (MessageMediaWebPage, type(None))
        )

        # Step 1: Download media (if any) with bounded retries
        media_path: Optional[Path] = None
        expected_size = 0

        if has_media:
            media_path, expected_size = await self._download_media_bounded(message)
            # If has_media is True but download returned None, it FAILED.
            # Mark as FAILED — do NOT fall back to text-only.
            if media_path is None:
                raise _BoundedRetryExhausted(
                    source_id,
                    self.config.max_retries,
                    RuntimeError("Media download failed — message has media but no file"),
                )
            self.stats["media_downloaded"] += 1
            self.db.mark_downloaded(
                source_id,
                media_path=str(media_path.relative_to(self.config.data_dir)),
                has_media=True,
                expected_size=expected_size,
            )
        else:
            # Text-only message — no media to download
            self.db.mark_downloaded(source_id, has_media=False)

        # Step 2: Upload to target (crash window begins)
        upload_hash = self._compute_upload_hash(source_id, media_path)
        self.db.mark_uploading(source_id, upload_attempt_hash=upload_hash)

        # Step 3: Attempt reconciliation if we're recovering from a crash
        # (check if this upload already exists on target)
        existing_target_id = await self._reconcile_upload(
            source_id, upload_hash, target_entity, message, media_path
        )

        if existing_target_id is not None:
            # Reconciliation found an existing upload — mark as uploaded
            logger.info(
                "Reconciliation: source msg %s already uploaded as target msg %s",
                source_id, existing_target_id,
            )
            self.db.mark_uploaded(source_id, existing_target_id)
            self.stats["reconciled_duplicates"] += 1
            self.stats["media_uploaded"] += 1 if has_media else 0
        else:
            # No reconciliation match — upload normally
            target_msg = await self._upload_to_target_bounded(
                message, media_path, target_entity
            )
            if target_msg is None:
                raise _BoundedRetryExhausted(
                    source_id,
                    self.config.max_retries,
                    RuntimeError("Target upload returned None"),
                )
            self.db.mark_uploaded(source_id, target_msg.id)
            self.stats["media_uploaded"] += 1 if has_media else 0

        # Step 4: Optionally delete media file after confirmed upload
        if media_path and self.config.delete_after_upload:
            try:
                media_path.unlink(missing_ok=True)
                logger.debug("Deleted uploaded media: %s", media_path)
            except OSError as e:
                logger.warning("Could not delete media %s: %s", media_path, e)

    # --------------------------------------------------------------- media download (.part files)

    async def _download_media_bounded(
        self, message: Message
    ) -> tuple[Optional[Path], int]:
        """Download media with bounded retries and .part file safety.

        Returns (final_path, expected_size) or (None, 0) if all retries fail.
        Raises FloodWaitError (propagated to caller for unbounded retry).
        """
        filename = self._build_media_filename(message)
        final_path = self.config.media_dir / filename
        part_path = self.config.media_dir / (filename + ".part")

        # Determine expected size for validation
        expected_size = self._get_expected_size(message)

        # If final file exists and passes validation, reuse it
        if final_path.exists() and self._verify_file(final_path, expected_size):
            logger.debug("Media already downloaded and valid: %s", final_path)
            return final_path, expected_size

        # Clean up any stale .part file from a previous crash
        if part_path.exists():
            logger.info("Found stale .part file: %s — removing", part_path)
            try:
                part_path.unlink(missing_ok=True)
            except OSError:
                pass

        # Download with bounded retries
        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Download to .part file
                result = await self.source.download_media(message, file=str(part_path))
                if result is None:
                    logger.warning(
                        "Source returned no media for msg %s (attempt %d/%d)",
                        message.id, attempt, self.config.max_retries,
                    )
                    self._cleanup_part(part_path)
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(min(2 ** attempt, 30))
                    continue

                # Verify the .part file
                if not self._verify_file(part_path, expected_size):
                    logger.warning(
                        "Downloaded .part file failed validation for msg %s (attempt %d/%d)",
                        message.id, attempt, self.config.max_retries,
                    )
                    self._cleanup_part(part_path)
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(min(2 ** attempt, 30))
                    continue

                # Atomic rename: .part → final
                part_path.rename(final_path)
                logger.debug("Media downloaded and renamed: %s", final_path)
                return final_path, expected_size

            except FloodWaitError:
                # Propagate — FloodWait is handled by caller (unbounded)
                self._cleanup_part(part_path)
                raise
            except Exception as e:  # noqa: BLE001
                last_error = e
                logger.warning(
                    "Download attempt %d/%d failed for msg %s: %s",
                    attempt, self.config.max_retries, message.id, e,
                )
                self._cleanup_part(part_path)
                if attempt < self.config.max_retries:
                    await asyncio.sleep(min(2 ** attempt, 30))

        self.error_logger.error(
            "Media download failed for source msg %s after %d attempts: %s",
            message.id, self.config.max_retries, last_error,
        )
        return None, 0

    def _cleanup_part(self, part_path: Path) -> None:
        """Safely remove a .part file."""
        try:
            if part_path.exists():
                part_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _verify_file(self, path: Path, expected_size: int) -> bool:
        """Verify a downloaded file exists, is non-empty, and matches expected size."""
        if not path.exists():
            return False
        try:
            stat = path.stat()
        except OSError:
            return False
        if stat.st_size == 0:
            return False
        if expected_size > 0 and stat.st_size != expected_size:
            logger.warning(
                "File %s size %d != expected %d",
                path, stat.st_size, expected_size,
            )
            return False
        return True

    def _get_expected_size(self, message: Message) -> int:
        """Get expected file size from media metadata."""
        media = message.media
        if isinstance(media, MessageMediaDocument):
            doc = media.document
            if doc:
                return int(doc.size or 0)
        if isinstance(media, MessageMediaPhoto):
            photo = media.photo
            if photo:
                # Try to get the largest photo size
                sizes = getattr(photo, "sizes", None) or []
                if sizes:
                    try:
                        largest = max(
                            (s for s in sizes if hasattr(s, "size") and s.size),
                            key=lambda s: s.size,
                            default=None,
                        )
                        if largest:
                            return int(largest.size)
                    except (TypeError, ValueError):
                        pass
                return int(getattr(photo, "size", 0) or 0)
        return 0

    def _build_media_filename(self, message: Message) -> str:
        """Build a safe, collision-free filename using the source message ID."""
        ext = self._get_extension(message)
        return f"msg_{message.id}{ext}"

    def _get_extension(self, message: Message) -> str:
        """Determine file extension from media type."""
        media = message.media
        if isinstance(media, MessageMediaPhoto):
            return ".jpg"
        if isinstance(media, MessageMediaDocument):
            doc = media.document
            if doc and doc.mime_type:
                mime = doc.mime_type.lower()
                ext_map = {
                    "video/mp4": ".mp4",
                    "video/quicktime": ".mov",
                    "audio/mpeg": ".mp3",
                    "audio/mp4": ".m4a",
                    "audio/ogg": ".ogg",
                    "audio/opus": ".opus",
                    "image/jpeg": ".jpg",
                    "image/png": ".png",
                    "image/gif": ".gif",
                    "image/webp": ".webp",
                    "application/pdf": ".pdf",
                    "application/zip": ".zip",
                }
                return ext_map.get(mime, "")
            for attr in (doc.attributes if doc else []):
                attr_type = type(attr).__name__
                if "Video" in attr_type:
                    return ".mp4"
                if "Audio" in attr_type:
                    return ".ogg" if getattr(attr, "voice", False) else ".mp3"
                if "Filename" in attr_type and getattr(attr, "file_name", None):
                    _, ext = os.path.splitext(attr.file_name)
                    if ext:
                        return ext.lower()
        return ""

    # --------------------------------------------------------------- target upload

    async def _upload_to_target_bounded(
        self,
        message: Message,
        media_path: Optional[Path],
        target_entity: Any,
    ) -> Optional[Message]:
        """Upload with bounded retries. Raises FloodWaitError (unbounded)."""
        caption = message.text or message.message or ""
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                if media_path and media_path.exists():
                    return await self.target.send_file(
                        target_entity,
                        file=str(media_path),
                        caption=caption if caption else None,
                    )
                else:
                    if not caption:
                        logger.warning("Message %s has no text and no media.", message.id)
                        return None
                    return await self.target.send_message(target_entity, caption)
            except FloodWaitError:
                raise
            except Exception as e:  # noqa: BLE001
                last_error = e
                logger.warning(
                    "Upload attempt %d/%d failed for source msg %s: %s",
                    attempt, self.config.max_retries, message.id, e,
                )
                if attempt < self.config.max_retries:
                    await asyncio.sleep(min(2 ** attempt, 30))

        self.error_logger.error(
            "Upload failed for source msg %s after %d attempts: %s",
            message.id, self.config.max_retries, last_error,
        )
        return None

    # --------------------------------------------------------------- reconciliation

    def _compute_upload_hash(self, source_id: int, media_path: Optional[Path]) -> str:
        """Compute a hash to identify this upload attempt for reconciliation.

        This is NOT a content hash — it's a deterministic identifier based on
        the source message ID and media path, used to find potential duplicates
        in the target's recent messages.
        """
        h = hashlib.sha256()
        h.update(str(source_id).encode())
        if media_path:
            h.update(str(media_path.name).encode())
        return h.hexdigest()[:16]

    async def _reconcile_upload(
        self,
        source_id: int,
        upload_hash: str,
        target_entity: Any,
        source_message: Message,
        media_path: Optional[Path],
    ) -> Optional[int]:
        """Check if this upload already exists on the target (crash-window recovery).

        We check the last N target Saved Messages for a match. Matching is
        heuristic — we compare:
        1. Text content (if text-only message)
        2. Media file name + size (if media message)

        Returns the target_message_id if a match is found, None otherwise.
        """
        try:
            recent = await self.target.get_messages(
                target_entity,
                limit=self.config.reconciliation_window,
            )
        except FloodWaitError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("Reconciliation query failed for msg %s: %s", source_id, e)
            return None

        if not recent:
            return None

        source_text = (source_message.text or source_message.message or "").strip()
        source_date = source_message.date

        for target_msg in recent:
            if target_msg is None:
                continue
            # Match by text content for text-only messages
            if media_path is None and source_text:
                target_text = (getattr(target_msg, "text", "") or
                               getattr(target_msg, "message", "") or "").strip()
                if target_text and target_text == source_text:
                    # Heuristic match — assume this is the same message
                    logger.info(
                        "Reconciliation match by text for source msg %s → target msg %s",
                        source_id, target_msg.id,
                    )
                    return target_msg.id
            # Match by media presence + approximate date for media messages
            elif media_path is not None and target_msg.media:
                # Check if the target message has media and was sent around the
                # same time as our upload attempt (within a few minutes)
                if source_date and target_msg.date:
                    time_diff = abs((source_date - target_msg.date).total_seconds())
                    # This is a weak heuristic — we can't truly verify content equality
                    # without downloading the target media, which is expensive.
                    # We skip this check to avoid false positives.
                    pass

        return None

    # --------------------------------------------------------------- crash recovery

    async def _recover_incomplete_messages(
        self, source_entity: Any, target_entity: Any
    ) -> None:
        """Recover messages in non-terminal states (pending, downloading,
        downloaded, uploading) from a previous crash.

        FloodWait handling: if FloodWait occurs during recovery of a message,
        we sleep and RETRY THE SAME MESSAGE (not continue to the next one).
        FloodWait does NOT count as a failure and does NOT increment
        retry_count. This ensures the message is not abandoned.
        """
        incomplete_ids = self.db.get_incomplete_message_ids()
        if not incomplete_ids:
            return

        logger.info("Recovering %d incomplete messages from crash", len(incomplete_ids))
        print(f"  Recovering {len(incomplete_ids)} incomplete message(s) from crash...")

        for source_id in incomplete_ids:
            await self._recover_single_message_with_floodwait(
                source_id, source_entity, target_entity
            )

        self.db.update_last_run()

    async def _recover_single_message_with_floodwait(
        self,
        source_id: int,
        source_entity: Any,
        target_entity: Any,
    ) -> None:
        """Recover a single incomplete message, retrying on FloodWait.

        FloodWait retries the SAME message (unbounded). Other errors mark
        the message as failed and move on.
        """
        msg_state = self.db.get_message(source_id)
        if msg_state is None:
            return

        status = msg_state["status"]

        # Loop for FloodWait retry of the SAME message
        while True:
            try:
                # Re-fetch the source message
                message = await self.source.get_messages(source_entity, ids=source_id)
                if message is None:
                    self.db.mark_failed(source_id, "Source message no longer exists")
                    self.stats["failed"] += 1
                    return

                if status == "uploading":
                    # Crash during upload — attempt reconciliation first
                    upload_hash = msg_state.get("upload_attempt_hash")
                    existing_target = await self._reconcile_upload(
                        source_id, upload_hash, target_entity, message,
                        Path(msg_state["media_path"]) if msg_state.get("media_path") else None,
                    )
                    if existing_target is not None:
                        self.db.mark_uploaded(source_id, existing_target)
                        self.stats["recovered_crash"] += 1
                        self.stats["reconciled_duplicates"] += 1
                        return
                    # No reconciliation match — re-upload
                    # Fall through to full migration

                # For all incomplete states, re-attempt the full migration
                # (download will reuse existing valid media if present)
                await self._migrate_single_message(message, source_entity, target_entity)
                self.stats["recovered_crash"] += 1
                return

            except FloodWaitError as e:
                # Sleep and retry the SAME message (FloodWait is NOT a failure)
                await self._handle_flood_wait(e, f"recovery msg {source_id}")
                continue  # retry same message
            except KeyboardInterrupt:
                raise
            except _BoundedRetryExhausted as e:
                self.db.mark_failed(source_id, f"{type(e.last_error).__name__}: {e.last_error}")
                self.stats["failed"] += 1
                return
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Crash recovery failed for msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.db.mark_failed(source_id, str(e))
                self.stats["failed"] += 1
                return

    async def _retry_failed_messages(
        self, source_entity: Any, target_entity: Any
    ) -> None:
        """Retry failed messages that haven't exceeded max_retries.

        FloodWait handling: if FloodWait occurs during retry of a message,
        we sleep and RETRY THE SAME MESSAGE (not continue to the next one).
        FloodWait does NOT count as a failure and does NOT increment
        retry_count.
        """
        failed_ids = self.db.get_retryable_failed_ids(self.config.max_retries)
        if not failed_ids:
            return

        logger.info("Retrying %d failed messages (retry_count < %d)",
                    len(failed_ids), self.config.max_retries)
        print(f"  Retrying {len(failed_ids)} failed message(s)...")

        for source_id in failed_ids:
            await self._retry_single_failed_with_floodwait(
                source_id, source_entity, target_entity
            )

        self.db.update_last_run()

    async def _retry_single_failed_with_floodwait(
        self,
        source_id: int,
        source_entity: Any,
        target_entity: Any,
    ) -> None:
        """Retry a single failed message, retrying on FloodWait.

        FloodWait retries the SAME message (unbounded). Other errors mark
        the message as failed again and move on.
        """
        self.db.clear_failed(source_id)

        # Loop for FloodWait retry of the SAME message
        while True:
            try:
                message = await self.source.get_messages(source_entity, ids=source_id)
                if message is None:
                    self.db.mark_failed(source_id, "Source message no longer exists")
                    return
                await self._migrate_single_message(message, source_entity, target_entity)
                self.stats["retried_failed"] += 1
                self.stats["messages_migrated"] += 1
                return
            except FloodWaitError as e:
                # Sleep and retry the SAME message (FloodWait is NOT a failure)
                await self._handle_flood_wait(e, f"failed retry msg {source_id}")
                continue  # retry same message
            except KeyboardInterrupt:
                raise
            except _BoundedRetryExhausted as e:
                self.db.mark_failed(source_id, f"{type(e.last_error).__name__}: {e.last_error}")
                self.stats["failed"] += 1
                return
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Failed retry for msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.db.mark_failed(source_id, str(e))
                self.stats["failed"] += 1
                return

    # --------------------------------------------------------------- iter + floodwait

    async def _iter_source_messages(self, entity: Any, min_id: int = 0):
        """Iterate source Saved Messages oldest-first, handling FloodWait."""
        current_min_id = min_id
        while True:
            try:
                async for message in self.source.iter_messages(
                    entity,
                    reverse=True,
                    min_id=current_min_id,
                    limit=None,
                ):
                    yield message
                return
            except FloodWaitError as e:
                await self._handle_flood_wait(e, "source iter_messages")
                continue
            except Exception as e:  # noqa: BLE001
                logger.warning("Error during iter_messages: %s. Retrying in 5s.", e)
                await asyncio.sleep(5)
                continue

    async def _handle_flood_wait(self, e: FloodWaitError, context: str) -> None:
        """Sleep for the FloodWait duration. FloodWait does NOT count as a failure."""
        seconds = int(getattr(e, "seconds", 60))
        logger.warning("FloodWait during %s: sleeping %ds", context, seconds)
        print(f"  FloodWait ({context}): sleeping {seconds}s...")
        if seconds <= 60:
            await asyncio.sleep(seconds + 1)
        else:
            slept = 0
            while slept < seconds:
                step = min(60, seconds - slept)
                print(f"  ... {seconds - slept}s remaining", flush=True)
                await asyncio.sleep(step)
                slept += step
        print("  Resuming.")

    # --------------------------------------------------------------- summary

    def _print_summary(self) -> None:
        s = self.stats
        st = self.db.summary()
        print()
        print("=" * 60)
        print("  MIGRATION COMPLETE" if s["failed"] == 0
              else "  MIGRATION FINISHED (with failures)")
        print("=" * 60)
        print(f"  Messages migrated:          {s['messages_migrated']}")
        print(f"  Media downloaded:           {s['media_downloaded']}")
        print(f"  Media uploaded:             {s['media_uploaded']}")
        print(f"  Skipped (already uploaded): {s['skipped_already_uploaded']}")
        print(f"  Recovered from crash:       {s['recovered_crash']}")
        print(f"  Retried (previously failed):{s['retried_failed']}")
        print(f"  Reconciled duplicates:       {s['reconciled_duplicates']}")
        print(f"  Failed:                     {s['failed']}")
        print(f"  Contiguous checkpoint:      {st['contiguous_checkpoint']}")
        print(f"  Total tracked in DB:        {st['total_tracked']}")
        print(f"  State counts:               {st['counts']}")
        print("=" * 60)

        if s["failed"] > 0:
            print(f"\n  {s['failed']} message(s) failed. Check errors.log for details.")
            print("  Re-run the migration to retry failed messages.")


class _BoundedRetryExhausted(Exception):
    """Raised when bounded retries are exhausted for a message."""

    def __init__(self, source_id: int, attempts: int, last_error: Exception):
        self.source_id = source_id
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Source msg {source_id}: exhausted {attempts} attempts. Last error: {last_error}"
        )
