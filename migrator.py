"""Saved Messages migrator: source → target.

Downloads messages from the SOURCE account's Saved Messages (oldest first),
then uploads them to the TARGET account's Saved Messages.

Architecture
------------
1. Validate both sessions (different accounts unless override).
2. Retry any messages in ``uploading`` or ``failed`` state (crash recovery).
3. Iterate source Saved Messages oldest-first, starting from the highest
   already-uploaded source message ID.
4. For each message:
   a. Skip if already ``uploaded`` (idempotent).
   b. Download media to ``DATA_DIR/media/`` (if the message has media).
   c. Mark ``downloaded``.
   d. Mark ``uploading`` (crash window begins).
   e. Upload to target Saved Messages.
   f. Confirm upload (get target message ID).
   g. Mark ``uploaded`` with target message ID (crash window ends).
   h. Optionally delete the local media file (``DELETE_AFTER_UPLOAD``).
5. Periodically checkpoint state (every ``CHECKPOINT_EVERY`` messages).
6. On completion, log summary and exit.

Crash safety
------------
- State is persisted after every status transition.
- ``uploading`` is the crash window: if we crash between marking
  ``uploading`` and marking ``uploaded``, the message will be re-uploaded
  on restart. Telegram may create a duplicate, but no data is lost.
- Media files are NOT deleted until after ``uploaded`` is confirmed.
"""
from __future__ import annotations

import asyncio
import logging
import os
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
from migration_state import MigrationStateManager

logger = logging.getLogger("telegram_backup.migrator")


class SavedMessagesMigrator:
    """Migrates Saved Messages from a source account to a target account."""

    def __init__(
        self,
        source_client: TelegramClient,
        target_client: TelegramClient,
        config: MigrationConfig,
        state: MigrationStateManager,
        error_logger: logging.Logger,
    ) -> None:
        self.source = source_client
        self.target = target_client
        self.config = config
        self.state = state
        self.error_logger = error_logger

        # Stats for this run
        self.stats = {
            "messages_migrated": 0,
            "media_downloaded": 0,
            "media_uploaded": 0,
            "failed": 0,
            "skipped_already_uploaded": 0,
            "retried_crash": 0,
            "retried_failed": 0,
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

        # Safety: refuse if source and target are the same account
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

        # Get the "Saved Messages" entity (it's the user's own "me" dialog)
        source_saved = await self.source.get_input_entity("me")
        target_saved = await self.target.get_input_entity("me")

        # Step 1: Retry messages left in 'uploading' state (crash recovery)
        await self._retry_uploading_messages(source_saved, target_saved)

        # Step 2: Retry messages in 'failed' state
        await self._retry_failed_messages(source_saved, target_saved)

        # Step 3: Compute resume point
        resume_from = self.state.get_highest_uploaded_source_id()
        logger.info(
            "Resuming from source message id > %d (%d messages already uploaded)",
            resume_from, self.state.summary()["counts"]["uploaded"],
        )
        print(f"  Resuming from source message id > {resume_from}")

        # Step 4: Iterate source Saved Messages oldest-first
        scanned = 0
        last_save = 0

        async for message in self._iter_source_messages(source_saved, min_id=resume_from):
            scanned += 1
            source_id = message.id

            # Skip if already uploaded (idempotent)
            if self.state.is_uploaded(source_id):
                self.stats["skipped_already_uploaded"] += 1
                continue

            # Process this message
            try:
                await self._migrate_single_message(
                    message, source_saved, target_saved
                )
                self.stats["messages_migrated"] += 1
            except FloodWaitError as e:
                await self._handle_flood_wait(e, "source iteration")
                # Retry this message once after FloodWait
                try:
                    await self._migrate_single_message(
                        message, source_saved, target_saved
                    )
                    self.stats["messages_migrated"] += 1
                except Exception as e2:  # noqa: BLE001
                    self.error_logger.error(
                        "Migration failed for source msg %s after FloodWait: %s: %s",
                        source_id, type(e2).__name__, e2,
                    )
                    self.state.mark_failed(source_id, str(e2))
                    self.stats["failed"] += 1
            except KeyboardInterrupt:
                logger.info("Interrupted; saving state and exiting.")
                self.state.save()
                raise
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Migration failed for source msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.state.mark_failed(source_id, f"{type(e).__name__}: {e}")
                self.stats["failed"] += 1
                # Continue to next message — one failure shouldn't stop the migration

            # Periodic checkpoint
            if scanned - last_save >= self.config.checkpoint_every:
                self.state.save()
                last_save = scanned

            # Progress display
            if scanned % 100 == 0:
                print(
                    f"  Scanned: {scanned} | Migrated: {self.stats['messages_migrated']} | "
                    f"Failed: {self.stats['failed']} | Current source id: {source_id}"
                )

        # Finalize
        self.state.save()
        self._print_summary()

    # --------------------------------------------------------------- single message

    async def _migrate_single_message(
        self,
        message: Message,
        source_entity: Any,
        target_entity: Any,
    ) -> None:
        """Migrate a single message: download → upload → confirm."""
        source_id = message.id

        # Step 1: Track the message as pending
        if self.state.get_status(source_id) is None:
            self.state.mark_pending(
                source_id,
                date=message.date.isoformat() if message.date else None,
            )

        # Step 2: Download media (if any)
        media_path = None
        has_media = bool(message.media) and not isinstance(
            message.media, (MessageMediaWebPage, type(None))
        )

        if has_media:
            media_path = await self._download_media(message)
            if media_path:
                self.state.mark_downloaded(
                    source_id,
                    media_path=str(media_path.relative_to(self.config.data_dir)),
                    has_media=True,
                )
                self.stats["media_downloaded"] += 1
            else:
                # Media exists but couldn't be downloaded — mark as downloaded
                # without media (text-only migration for this message)
                self.state.mark_downloaded(source_id, has_media=False)
        else:
            self.state.mark_downloaded(source_id, has_media=False)

        # Step 3: Upload to target (crash window begins)
        self.state.mark_uploading(source_id)

        target_msg = await self._upload_to_target(message, media_path, target_entity)

        # Step 4: Confirm upload (crash window ends)
        if target_msg is not None:
            target_id = target_msg.id
            self.state.mark_uploaded(source_id, target_id)
            self.stats["media_uploaded"] += 1 if has_media else 0

            # Optionally delete the local media file
            if media_path and self.config.delete_after_upload:
                try:
                    media_path.unlink(missing_ok=True)
                    logger.debug("Deleted uploaded media: %s", media_path)
                except OSError as e:
                    logger.warning("Could not delete media %s: %s", media_path, e)
        else:
            raise RuntimeError("Target upload returned None — upload failed")

    # --------------------------------------------------------------- media download

    async def _download_media(self, message: Message) -> Optional[Path]:
        """Download media for a message to DATA_DIR/media/.

        Returns the local file path, or None if download failed.
        """
        # Build a safe filename
        filename = self._build_media_filename(message)
        target_path = self.config.media_dir / filename

        # If file already exists (from a previous run), verify size and reuse
        if target_path.exists() and target_path.stat().st_size > 0:
            logger.debug("Media already downloaded: %s", target_path)
            return target_path

        # Download with retries
        for attempt in range(1, self.config.max_retries + 1):
            try:
                result = await self.source.download_media(message, file=str(target_path))
                if result is None:
                    logger.warning("Source returned no media for msg %s", message.id)
                    return None
                if not target_path.exists() or target_path.stat().st_size == 0:
                    logger.warning("Downloaded file is missing/empty for msg %s", message.id)
                    if target_path.exists():
                        target_path.unlink(missing_ok=True)
                    continue  # retry
                return target_path
            except FloodWaitError:
                raise  # propagate to caller
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "Download attempt %d/%d failed for msg %s: %s",
                    attempt, self.config.max_retries, message.id, e,
                )
                # Clean up partial file
                if target_path.exists():
                    try:
                        target_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                if attempt < self.config.max_retries:
                    await asyncio.sleep(min(2 ** attempt, 30))

        self.error_logger.error(
            "Media download failed for source msg %s after %d attempts",
            message.id, self.config.max_retries,
        )
        return None

    def _build_media_filename(self, message: Message) -> str:
        """Build a safe, collision-free filename for the media."""
        # Use the source message ID to guarantee uniqueness
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
            # No mime type — guess from attributes
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

    async def _upload_to_target(
        self,
        message: Message,
        media_path: Optional[Path],
        target_entity: Any,
    ) -> Optional[Message]:
        """Upload a message (text + optional media) to target Saved Messages.

        Uses ``send_file`` for media messages and ``send_message`` for text-only.
        """
        caption = message.text or message.message or ""

        if media_path and media_path.exists():
            # Upload media with caption
            for attempt in range(1, self.config.max_retries + 1):
                try:
                    result = await self.target.send_file(
                        target_entity,
                        file=str(media_path),
                        caption=caption if caption else None,
                        # Don't set formatting to avoid compatibility issues
                    )
                    return result
                except FloodWaitError:
                    raise
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "Upload attempt %d/%d failed for source msg %s: %s",
                        attempt, self.config.max_retries, message.id, e,
                    )
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(min(2 ** attempt, 30))
            return None
        else:
            # Text-only message
            if not caption:
                logger.warning("Message %s has no text and no media; skipping.", message.id)
                return None
            for attempt in range(1, self.config.max_retries + 1):
                try:
                    result = await self.target.send_message(target_entity, caption)
                    return result
                except FloodWaitError:
                    raise
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "Text upload attempt %d/%d failed for source msg %s: %s",
                        attempt, self.config.max_retries, message.id, e,
                    )
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(min(2 ** attempt, 30))
            return None

    # --------------------------------------------------------------- retry passes

    async def _retry_uploading_messages(
        self, source_entity: Any, target_entity: Any
    ) -> None:
        """Retry messages left in 'uploading' state (crash recovery)."""
        uploading_ids = self.state.get_uploading_message_ids()
        if not uploading_ids:
            return

        logger.info("Recovering %d messages left in 'uploading' state", len(uploading_ids))
        print(f"  Recovering {len(uploading_ids)} message(s) from crash (uploading state)...")

        for source_id in uploading_ids:
            try:
                # Re-fetch the source message
                message = await self.source.get_messages(source_entity, ids=source_id)
                if message is None:
                    self.state.mark_failed(source_id, "Source message no longer exists")
                    self.stats["failed"] += 1
                    continue

                # Re-download media if needed
                media_path = None
                msg_state = self.state._state["messages"].get(str(source_id), {})
                if msg_state.get("media_path"):
                    media_path = self.config.data_dir / msg_state["media_path"]
                    if not media_path.exists():
                        media_path = await self._download_media(message)

                # Re-upload
                target_msg = await self._upload_to_target(message, media_path, target_entity)
                if target_msg:
                    self.state.mark_uploaded(source_id, target_msg.id)
                    self.stats["retried_crash"] += 1
                else:
                    self.state.mark_failed(source_id, "Re-upload returned None")
                    self.stats["failed"] += 1
            except FloodWaitError as e:
                await self._handle_flood_wait(e, f"crash recovery msg {source_id}")
                continue
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Crash recovery failed for msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.state.mark_failed(source_id, str(e))
                self.stats["failed"] += 1

        self.state.save()

    async def _retry_failed_messages(
        self, source_entity: Any, target_entity: Any
    ) -> None:
        """Retry messages in 'failed' state."""
        failed_ids = self.state.get_failed_message_ids()
        if not failed_ids:
            return

        logger.info("Retrying %d failed messages", len(failed_ids))
        print(f"  Retrying {len(failed_ids)} failed message(s)...")

        for source_id in failed_ids:
            self.state.clear_failed(source_id)
            try:
                message = await self.source.get_messages(source_entity, ids=source_id)
                if message is None:
                    self.state.mark_failed(source_id, "Source message no longer exists")
                    continue
                await self._migrate_single_message(message, source_entity, target_entity)
                self.stats["retried_failed"] += 1
            except FloodWaitError as e:
                await self._handle_flood_wait(e, f"failed retry msg {source_id}")
                continue
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Failed retry for msg %s: %s: %s",
                    source_id, type(e).__name__, e,
                )
                self.state.mark_failed(source_id, str(e))

        self.state.save()

    # --------------------------------------------------------------- iter + floodwait

    async def _iter_source_messages(self, entity: Any, min_id: int = 0):
        """Iterate source Saved Messages oldest-first, handling FloodWait."""
        current_min_id = min_id
        while True:
            try:
                async for message in self.source.iter_messages(
                    entity,
                    reverse=True,        # oldest first
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
        """Sleep for the FloodWait duration."""
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
        st = self.state.summary()
        print()
        print("=" * 60)
        print("  MIGRATION COMPLETE" if st["completed"] or s["failed"] == 0
              else "  MIGRATION FINISHED (with failures)")
        print("=" * 60)
        print(f"  Messages migrated:        {s['messages_migrated']}")
        print(f"  Media downloaded:         {s['media_downloaded']}")
        print(f"  Media uploaded:           {s['media_uploaded']}")
        print(f"  Skipped (already done):   {s['skipped_already_uploaded']}")
        print(f"  Recovered from crash:     {s['retried_crash']}")
        print(f"  Retried (previously failed): {s['retried_failed']}")
        print(f"  Failed:                   {s['failed']}")
        print(f"  Total tracked in state:   {st['total_tracked']}")
        print(f"  State counts:             {st['counts']}")
        print("=" * 60)

        if s["failed"] > 0:
            print(f"\n  {s['failed']} message(s) failed. See {self.config.failed_messages_file}")
            print("  Re-run the migration to retry failed messages.")
