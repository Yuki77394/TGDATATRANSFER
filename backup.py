"""Backup orchestration.

Discovers accessible chats and runs the backup loop. Responsibilities:

  - Discover Saved Messages and accessible private chats (and optionally
    groups / channels when explicitly enabled).
  - For each chat: retry failed messages/media from previous runs, then
    iterate messages oldest-first, download media, append structured
    records to messages.jsonl, periodically checkpoint state.
  - Handle FloodWaitError by sleeping the requested duration and resuming.
  - Print live progress and a final summary.
  - Write backup_metadata.json at the end.

CRITICAL RESUME/SAFETY DESIGN — read this carefully
----------------------------------------------------

INVARIANT 1: messages.jsonl is the authoritative source of truth.

    The set of message_ids present in messages.jsonl for a chat defines
    what has been successfully exported. ``state.last_message_id`` is ONLY
    a performance hint and may be stale, corrupted, or ahead of the JSONL.

INVARIANT 2: Resume point = jsonl_max (the highest message_id in JSONL).

    On restart, we compute:
        state_hint  = state.last_message_id  # may be stale
        jsonl_max   = max(message_id in messages.jsonl)
        resume_from = jsonl_max              # safe AND efficient

    We resume from ``jsonl_max``, NOT ``min(state, jsonl_max)`` and NOT
    ``max(state, jsonl_max)``. This is both SAFE and EFFICIENT:

    SAFE: Every message with id <= jsonl_max is in one of two states:
      (a) Present in messages.jsonl → already exported, has_message()
          will skip it if re-fetched.
      (b) Present in failed_messages.jsonl → will be retried by
          _retry_failed_messages() BEFORE this main loop runs.
    No message below jsonl_max can be silently skipped.

    EFFICIENT: iter_messages(min_id=jsonl_max) only fetches messages with
    id > jsonl_max — i.e., only genuinely NEW messages. We do NOT re-fetch
    the entire chat history on every run.

    state_hint is NOT used for resume — only as a sanity check. If
    state_hint > jsonl_max, we log a warning but still resume from
    jsonl_max.

INVARIANT 3: A failed message STOPS the chat.

    If processing message N fails for any non-FloodWait reason:
      - The message ID is recorded in ``failed_messages.jsonl``.
      - State's ``last_message_id`` is NOT advanced past N.
      - Processing of this chat STOPS (we do NOT continue to N+1).
      - On the next run, ``_retry_failed_messages()`` retries N BEFORE
        the main loop runs.

    This prevents the resume cursor from advancing past a failed message
    and permanently skipping it.

    Exception: FloodWait is NOT a failure. We sleep and retry the SAME
    message repeatedly until it succeeds or a non-FloodWait error occurs.

INVARIANT 4: Message is marked processed ONLY after JSONL append succeeds.

    Order of operations per message:
      1. Download media (if any). On media failure, record ``media.error``
         in the message record and add to ``failed_media`` set — but the
         message itself is still exported (media failure ≠ message failure).
      2. Build the message record.
      3. Append to messages.jsonl (atomic + fsync).
      4. ONLY THEN call ``state.mark_message_processed(id)``.

INVARIANT 5: Failed messages and failed media are tracked separately.

    - ``failed_messages.jsonl`` — message IDs that failed to export.
      Retried by ``_retry_failed_messages()`` before the main loop.
    - ``failed_media.jsonl`` — media keys that failed to download.
      Retried by ``_retry_failed_media()`` before the main loop.

    A message with failed media is STILL written to JSONL (with
    ``media.error`` set). The message is considered "exported" — only
    its media needs retry.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Optional

from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, User

from config import Config
from exporter import MessageExporter
from media_downloader import MediaDownloader
from state_manager import StateManager
from utils import safe_chat_dir_name, safe_write_json, truncate_text

logger = logging.getLogger("telegram_backup.backup")


class BackupOrchestrator:
    """Top-level backup coordinator."""

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

        self.media_downloader = MediaDownloader(client, config, state, error_logger)

        self.stats: dict[str, int] = {
            "chats_processed": 0,
            "messages_exported": 0,
            "photos_downloaded": 0,
            "videos_downloaded": 0,
            "files_downloaded": 0,
            "voice_downloaded": 0,
            "audio_downloaded": 0,
            "failed_items": 0,
            "media_retried_success": 0,
        }

    # --------------------------------------------------------------- entry point

    async def run(self) -> None:
        """Run the full backup, then print summary and write metadata."""
        me = await self.client.get_me()
        logger.info(
            "Backup starting for user id=%s (%s)",
            me.id,
            " ".join(p for p in [me.first_name, me.last_name] if p) or "<no name>",
        )

        # Build the list of chats to back up.
        # We use a dict keyed by chat_key so that if dialog discovery
        # restarts after a FloodWait (which can re-yield already-seen
        # dialogs), we don't end up with duplicate entries in the plan.
        chat_plan: dict[str, tuple[str, str, Path, Any]] = {}
        # Each value: (chat_key, display_name, chat_dir, entity_or_marker)

        if self.config.backup_saved_messages:
            chat_plan["saved_messages"] = (
                "saved_messages",
                "Saved Messages",
                self.config.saved_messages_dir,
                "me",
            )

        if self.config.backup_private_chats:
            async for chat_key, name, chat_dir, entity in self._discover_private_chats():
                chat_plan[chat_key] = (chat_key, name, chat_dir, entity)

        if self.config.backup_groups:
            async for chat_key, name, chat_dir, entity in self._discover_groups():
                chat_plan[chat_key] = (chat_key, name, chat_dir, entity)

        if self.config.backup_channels:
            async for chat_key, name, chat_dir, entity in self._discover_channels():
                chat_plan[chat_key] = (chat_key, name, chat_dir, entity)

        plan_list = list(chat_plan.values())
        total = len(plan_list)
        logger.info("Discovered %d chats to back up.", total)
        print(f"\nDiscovered {total} chat(s) to back up.")

        for idx, (chat_key, name, chat_dir, entity) in enumerate(plan_list, start=1):
            print(f"\n[{idx}/{total}] Processing: {name}")
            # IMPORTANT: We do NOT skip completed chats. A completed chat means
            # "all messages known during the previous run were processed
            # successfully" — NOT "never process this chat again." New
            # messages may have arrived since the last run, and we must
            # incrementally scan for them. _backup_chat() handles this
            # efficiently by resuming from jsonl_max (the highest message
            # id already in the JSONL), so only genuinely new messages
            # are fetched from Telegram.
            try:
                await self._backup_chat(chat_key, name, chat_dir, entity)
            except FloodWaitError as e:
                # This can happen if FloodWait occurs during chat_info write
                # or final JSON rebuild. Sleep and retry the whole chat once.
                await self._handle_flood_wait(e)
                try:
                    await self._backup_chat(chat_key, name, chat_dir, entity)
                except Exception as e2:  # noqa: BLE001
                    self.error_logger.error(
                        "Chat %s failed after FloodWait retry: %s: %s",
                        chat_key, type(e2).__name__, e2,
                    )
                    self.state.mark_chat_error(chat_key, str(e2))
            except KeyboardInterrupt:
                logger.info("Interrupted by user. Saving state and exiting.")
                self.state.save()
                raise
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Chat %s failed: %s: %s", chat_key, type(e).__name__, e,
                )
                self.state.mark_chat_error(chat_key, str(e))
                logger.exception("Chat %s failed", chat_key)

            # Checkpoint after every chat
            self.state.save()

        self._merge_media_stats()
        self._print_summary()
        self._write_metadata()

    # --------------------------------------------------------------- chat discovery

    async def _resilient_dialogs(self):
        """Iterate dialogs with FloodWait handling.

        If Telegram returns FloodWaitError while paginating dialogs, we sleep
        the requested duration and resume the iteration. This is important
        because dialog discovery happens BEFORE the per-chat loop, so an
        unhandled FloodWait here would abort the entire backup.

        Telethon's ``iter_dialogs`` doesn't support a ``min_id``-style resume,
        so on FloodWait we restart the iteration from the beginning. Dialogs
        are deduplicated by ``chat_key`` in the caller's ``chat_plan`` dict,
        so re-yielding already-seen dialogs is harmless.
        """
        while True:
            try:
                async for dialog in self.client.iter_dialogs():
                    yield dialog
                return  # iteration completed normally
            except FloodWaitError as e:
                logger.warning("FloodWait during iter_dialogs: sleeping %ss", e.seconds)
                print(f"  FloodWait during chat discovery: sleeping {e.seconds}s...")
                await asyncio.sleep(int(e.seconds) + 1)
                continue  # restart iteration from the beginning
            except Exception as e:  # noqa: BLE001
                # Non-FloodWait errors during dialog discovery: log and
                # retry with backoff. If persistent, propagate up.
                logger.warning("Error during iter_dialogs: %s", e)
                await asyncio.sleep(min(2 ** 3, 30))
                continue

    def _display_name_for_user(self, entity: User) -> str:
        """Build a human-readable display name for a user entity.

        Handles deleted users (no first/last name) gracefully by falling
        back to ``Deleted_User_<id>``.
        """
        # dialog.name is usually the best display name
        parts = [
            getattr(entity, "first_name", None),
            getattr(entity, "last_name", None),
        ]
        name = " ".join(p for p in parts if p).strip()
        if not name:
            # Deleted user or bot without a name
            username = getattr(entity, "username", None)
            if username:
                name = f"@{username}"
            else:
                name = f"User_{entity.id}"
        return name

    async def _discover_private_chats(self):
        """Yield accessible one-to-one private chats.

        Skips the user's own "Saved Messages" (handled separately).
        Bots are included by default unless INCLUDE_BOTS=false in .env.
        Deleted users are included (their messages are still part of your
        history) but get a fallback display name.
        """
        async for dialog in self._resilient_dialogs():
            entity = dialog.entity
            if not isinstance(entity, User):
                continue
            if getattr(entity, "is_self", False) or getattr(entity, "self", False):
                continue  # this is Saved Messages - handled separately
            # Skip bots if configured to exclude them
            if getattr(entity, "bot", False) and not self.config.include_bots:
                continue
            name = dialog.name or self._display_name_for_user(entity)
            chat_key = f"private_{entity.id}"
            chat_dir = self.config.private_chats_dir / safe_chat_dir_name(entity.id, name)
            yield chat_key, name, chat_dir, entity

    async def _discover_groups(self):
        """Yield small group chats (Chat, not Channel)."""
        async for dialog in self._resilient_dialogs():
            entity = dialog.entity
            if isinstance(entity, Chat) and not isinstance(entity, Channel):
                name = dialog.name or f"group_{entity.id}"
                chat_key = f"group_{entity.id}"
                chat_dir = self.config.groups_dir / safe_chat_dir_name(entity.id, name)
                yield chat_key, name, chat_dir, entity

    async def _discover_channels(self):
        """Yield channels and supergroups (both are Channel entities).

        We treat any megagroup/supergroup/broadcast channel here.
        """
        async for dialog in self._resilient_dialogs():
            entity = dialog.entity
            if isinstance(entity, Channel):
                name = dialog.name or f"channel_{entity.id}"
                kind = "supergroup" if getattr(entity, "megagroup", False) else "channel"
                chat_key = f"{kind}_{entity.id}"
                target_dir = self.config.groups_dir if kind == "supergroup" else self.config.channels_dir
                chat_dir = target_dir / safe_chat_dir_name(entity.id, name)
                yield chat_key, name, chat_dir, entity

    # --------------------------------------------------------------- per-chat backup

    async def _backup_chat(
        self,
        chat_key: str,
        name: str,
        chat_dir: Path,
        entity: Any,
    ) -> None:
        """Back up a single chat (initial + incremental).

        CRITICAL DESIGN (read carefully):

        1. **Retry failed messages first.** Before processing new messages,
           we retry any messages that failed in a previous run. This ensures
           failed older messages are retried BEFORE the resume cursor
           advances past them.

        2. **Resume point = jsonl_max.** We resume from the highest message
           id present in messages.jsonl. This is both safe and efficient
           (see INVARIANT 2 in the module docstring). Only genuinely new
           messages (id > jsonl_max) are fetched from Telegram.

        3. **If a message fails, STOP processing this chat.** We do NOT
           continue to the next message. The failed message is recorded in
           ``failed_messages.jsonl`` and will be retried on the next run.
           This prevents the resume cursor from advancing past a failed
           message and permanently skipping it.

           Exception: FloodWait is NOT a failure — we sleep and retry the
           SAME message repeatedly until it succeeds or a non-FloodWait
           error occurs.

        4. **Mark message processed ONLY after JSONL append succeeds.**
           ``state.last_message_id`` is a hint, not authoritative.

        5. **Completed chats are NOT skipped.** A completed chat means "all
           messages known during the previous run were processed successfully"
           — NOT "never process this chat again." On every run, we
           incrementally scan for new messages (id > jsonl_max). If no new
           messages exist, the iter_messages loop exits immediately (efficient).
        """
        chat_dir.mkdir(parents=True, exist_ok=True)
        media_dir = chat_dir / "media"
        media_dir.mkdir(exist_ok=True)

        # Write chat_info.json (non-critical if this fails)
        try:
            await self._write_chat_info(entity, chat_dir, name)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not write chat_info.json for %s: %s", chat_key, e)

        exporter = MessageExporter(chat_dir, self.config.base_dir)

        # Step 1a: Retry messages that failed to export in a previous run.
        await self._retry_failed_messages(chat_key, name, chat_dir, entity, exporter)

        # Step 1b: Retry media that failed to download in a previous run.
        await self._retry_failed_media(chat_key, name, chat_dir, entity)

        # Step 2: Compute resume point.
        #
        # INVARIANT (read carefully):
        #
        #   resume_from = jsonl_max
        #
        # We resume from the highest message_id present in messages.jsonl.
        # This is both SAFE and EFFICIENT:
        #
        # SAFE: Every message with id <= jsonl_max is in one of two states:
        #   (a) Present in messages.jsonl → already exported, has_message()
        #       will skip it if re-fetched.
        #   (b) Present in failed_messages.jsonl → will be retried by
        #       _retry_failed_messages() BEFORE this main loop runs.
        # No message below jsonl_max can be silently skipped.
        #
        # EFFICIENT: iter_messages(min_id=jsonl_max) only fetches messages
        # with id > jsonl_max — i.e., only genuinely NEW messages. We do
        # NOT re-fetch the entire chat history on every run.
        #
        # state_hint (state.last_message_id) is NOT used for resume. It is
        # only used as a sanity check: if state_hint > jsonl_max, it
        # indicates a previous inconsistency (state saved but JSONL write
        # failed), and we log a warning. The resume point is still
        # jsonl_max, which is always safe.
        #
        # Why not min(state_hint, jsonl_max)? Because that would re-fetch
        # all messages between state_hint and jsonl_max on every run, even
        # though they're already in the JSONL. With jsonl_max, we skip
        # straight to new messages.
        #
        # Why not max(state_hint, jsonl_max)? Because if state_hint >
        # jsonl_max (state ahead of JSONL due to failures), max would
        # resume from state_hint and SKIP the missing messages forever.
        # jsonl_max is always safe.
        state_hint = self.state.get_state_hint(chat_key)
        jsonl_max = exporter.get_max_message_id()
        resume_from = jsonl_max

        already_done = exporter.count()
        if resume_from > 0:
            logger.info(
                "Incremental scan for %s from message id > %d (state_hint=%d, jsonl_max=%d, %d already exported)",
                name, resume_from, state_hint, jsonl_max, already_done,
            )
            print(f"  Incremental scan from message id > {resume_from} ({already_done} already exported)")
        else:
            logger.info("Starting fresh backup of %s", name)

        # Sanity check: if state_hint > jsonl_max, log a warning.
        # This can happen if the state was saved but a JSONL write failed
        # (shouldn't happen with our ordering, but defensive).
        if state_hint > jsonl_max:
            logger.warning(
                "State hint (%d) is ahead of JSONL max (%d) for %s. "
                "This indicates a previous inconsistency. Resuming from "
                "jsonl_max (%d) to ensure no messages are skipped.",
                state_hint, jsonl_max, chat_key, jsonl_max,
            )
            print(f"  Warning: state checkpoint was ahead of exported records; "
                  f"resuming from {jsonl_max} to ensure safety.")

        scanned_this_run = 0
        last_saved_at = 0
        chat_aborted = False

        # Step 3: Iterate oldest -> newest, only fetching ids > resume_from.
        async for message in self._iter_messages_resilient(entity, min_id=resume_from):
            scanned_this_run += 1
            current_id = message.id

            # Skip if already in JSONL (idempotent safety net for resume).
            if exporter.has_message(current_id):
                continue

            # Process this message with FloodWait retry loop.
            # A message either completes successfully, or we record it as
            # failed and STOP processing this chat.
            success = False
            while True:
                try:
                    await self._process_single_message(
                        message, exporter, media_dir, chat_key
                    )
                    success = True
                    break
                except FloodWaitError as e:
                    # Sleep and retry the SAME message. FloodWait is not
                    # a failure — Telegram is just asking us to slow down.
                    await self._handle_flood_wait(e)
                    continue
                except KeyboardInterrupt:
                    raise
                except Exception as e:  # noqa: BLE001
                    # Genuine failure. Record it and STOP this chat.
                    self.error_logger.error(
                        "Failed to process message chat=%s msg_id=%s: %s: %s",
                        chat_key, current_id, type(e).__name__, e,
                    )
                    self.state.mark_message_failed(chat_key, current_id)
                    self.stats["failed_items"] += 1
                    chat_aborted = True
                    break

            if not success:
                # Message failed. Stop processing this chat to avoid
                # advancing the resume cursor past the failed message.
                # The failed message will be retried on the next run.
                logger.warning(
                    "Stopping chat %s at failed message %d. "
                    "Will retry on next run.",
                    chat_key, current_id,
                )
                print(f"  Message {current_id} failed; stopping chat (will retry next run).")
                break

            self.stats["messages_exported"] += 1

            # Periodic progress + checkpoint
            if scanned_this_run % 100 == 0:
                media_total = sum(
                    self.media_downloader.stats.get(k, 0)
                    for k in ("photos", "videos", "documents", "audio", "voice")
                )
                print(
                    f"  Messages scanned: {scanned_this_run} | "
                    f"Media downloaded: {media_total} | "
                    f"Current msg_id: {current_id}"
                )
                print(f"    Latest text: {truncate_text(message.text, 80)}")

            if scanned_this_run - last_saved_at >= self.config.checkpoint_every:
                self.state.save()
                last_saved_at = scanned_this_run

        # Finalize chat
        try:
            count = exporter.write_json()
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not rebuild messages.json for %s: %s", chat_key, e)
            count = exporter.count()

        # Only mark chat as completed if we didn't abort due to a failure.
        if not chat_aborted:
            self.state.mark_chat_completed(chat_key)
        else:
            # Record the abort reason in state
            self.state.mark_chat_error(
                chat_key,
                f"Aborted due to failed message(s); will retry on next run",
            )
        self.state.save()
        if not chat_aborted:
            self.stats["chats_processed"] += 1
        print(f"  Done. Scanned {scanned_this_run} message(s); chat total now {count}.")

    async def _process_single_message(
        self,
        message: Any,
        exporter: MessageExporter,
        media_dir: Path,
        chat_key: str,
    ) -> None:
        """Process a single message: download media, build record, append to JSONL.

        Order of operations (CRITICAL - see module docstring):
          1. Download media (if any). FloodWait is retried in-place.
          2. Build the message record (including media metadata).
          3. Append to JSONL (atomic + fsync).
          4. ONLY THEN mark message as processed in state.

        If media download raises FloodWait that exhausts in-place retries,
        we re-raise it so the chat-level handler can sleep longer and
        retry. The message is NOT marked as processed.
        """
        current_id = message.id

        # Step 1: Download media
        media_info: Optional[dict[str, Any]] = None
        if message.media is not None:
            try:
                media_info = await self.media_downloader.download_media_for_message(
                    message, media_dir, chat_key
                )
            except FloodWaitError:
                # Propagate up - caller will sleep and retry this message
                raise
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Media error in chat=%s msg_id=%s: %s: %s",
                    chat_key, current_id, type(e).__name__, e,
                )
                media_info = {
                    "type": "other",
                    "local_path": None,
                    "error": f"{type(e).__name__}: {e}",
                }

        # Step 2: Build the record
        record = self._build_message_record(message, media_info)

        # Step 3: Append to JSONL (atomic + fsync)
        exporter.append_message(record)

        # Step 4: Mark as processed (advances state's last_message_id in memory)
        self.state.mark_message_processed(chat_key, current_id)

    # --------------------------------------------------------------- iter with FloodWait

    async def _iter_messages_resilient(
        self,
        entity: Any,
        min_id: int = 0,
    ) -> AsyncIterator[Any]:
        """Iterate messages oldest-first, transparently handling FloodWait.

        Yields messages one by one. If a FloodWaitError occurs, we sleep the
        requested duration and resume iteration from the SAME ``min_id``.
        Already-processed messages (those in the JSONL) are skipped by
        ``exporter.has_message()`` in the caller, so re-fetching them is
        harmless and cheap.

        We deliberately do NOT advance the in-iterator cursor based on
        yielded messages. This ensures that if the caller crashes during
        processing, on restart we re-fetch from the original ``min_id``
        and the crashed message is included in the re-fetch.
        """
        current_min_id = min_id
        retries = 0
        max_retries = self.config.max_retries * 5  # generous for FloodWait

        while True:
            try:
                async for message in self.client.iter_messages(
                    entity,
                    reverse=True,        # oldest first
                    min_id=current_min_id,
                    limit=None,
                ):
                    yield message
                    retries = 0
                return
            except FloodWaitError as e:
                logger.warning(
                    "FloodWait during iter_messages for entity=%s: sleeping %ss",
                    entity, e.seconds,
                )
                print(f"  Telegram requested FloodWait: sleeping {e.seconds} seconds...")
                await asyncio.sleep(int(e.seconds) + 1)
                # Resume from the SAME min_id. Already-processed messages
                # will be skipped by exporter.has_message in the caller.
                continue
            except Exception as e:  # noqa: BLE001
                retries += 1
                if retries > max_retries:
                    logger.error(
                        "iter_messages exhausted retries for entity=%s: %s",
                        entity, e,
                    )
                    raise
                wait = min(2 ** retries, 30)
                logger.warning(
                    "iter_messages error (attempt %d/%d): %s. Sleeping %ss.",
                    retries, max_retries, e, wait,
                )
                await asyncio.sleep(wait)
                continue

    async def _handle_flood_wait(self, e: FloodWaitError) -> None:
        """Sleep for the requested FloodWait duration."""
        seconds = int(getattr(e, "seconds", 60))
        logger.warning("FloodWait: sleeping %s seconds.", seconds)
        print(f"  FloodWait: sleeping {seconds} seconds before retrying...")
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

    # --------------------------------------------------------------- retry passes

    async def _retry_failed_messages(
        self,
        chat_key: str,
        name: str,
        chat_dir: Path,
        entity: Any,
        exporter: MessageExporter,
    ) -> None:
        """Retry messages that failed to export in a previous run.

        This runs BEFORE the main message loop. It re-fetches each failed
        message from Telegram and attempts to export it.

        SEMANTICS (critical):
          - Successfully exported → remove from failed set.
          - Still fails (non-FloodWait exception) → keep in failed set,
            increment retry count.
          - get_messages returns None (message not found) → this COULD mean
            the message was deleted, OR it could be a transient API issue.
            We increment the retry count and keep the message in the failed
            set. Only after MAX_NONE_RETRIES (5) consecutive None returns
            do we consider the message "confirmed unavailable" and remove
            it from the failed set (with an explicit log entry).
          - FloodWait → sleep and retry the SAME message.

        This ensures a temporary network/API failure does NOT cause a
        failed message to be silently forgotten.
        """
        failed_ids = self.state.get_failed_message_ids(chat_key)
        if not failed_ids:
            return

        logger.info("Retrying %d failed messages for %s", len(failed_ids), chat_key)
        print(f"  Retrying {len(failed_ids)} previously-failed message(s)...")

        media_dir = chat_dir / "media"
        media_dir.mkdir(exist_ok=True)
        retried = 0
        succeeded = 0
        still_failed = 0
        confirmed_unavailable = 0

        MAX_NONE_RETRIES = 5  # consecutive None returns before "confirmed unavailable"

        for mid in sorted(failed_ids):
            # Skip if already in JSONL (was exported by some other path)
            if exporter.has_message(mid):
                self.state._clear_failed_message(chat_key, mid)
                succeeded += 1
                continue

            try:
                message = await self._fetch_single_message(entity, mid)
                if message is None:
                    # Message not returned by Telegram. This could mean:
                    #   (a) message was deleted, OR
                    #   (b) transient API issue.
                    # Increment retry count; only remove after MAX_NONE_RETRIES
                    # consecutive None returns.
                    count = self.state.increment_failed_message_retry(chat_key, mid)
                    if count >= MAX_NONE_RETRIES:
                        logger.warning(
                            "Message %d in %s returned None %d consecutive times; "
                            "marking as confirmed unavailable (likely deleted).",
                            mid, chat_key, count,
                        )
                        self.error_logger.warning(
                            "Message %d in %s confirmed unavailable after %d retries; "
                            "removing from failed set (likely deleted on Telegram).",
                            mid, chat_key, count,
                        )
                        self.state._clear_failed_message(chat_key, mid)
                        confirmed_unavailable += 1
                    else:
                        logger.info(
                            "Message %d in %s returned None (attempt %d/%d); "
                            "keeping in failed set for retry.",
                            mid, chat_key, count, MAX_NONE_RETRIES,
                        )
                        still_failed += 1
                    continue

                # Process with FloodWait retry loop
                while True:
                    try:
                        await self._process_single_message(
                            message, exporter, media_dir, chat_key
                        )
                        # Success — _process_single_message already called
                        # mark_message_processed which clears the failed flag
                        succeeded += 1
                        retried += 1
                        break
                    except FloodWaitError as e:
                        await self._handle_flood_wait(e)
                        continue
                    except Exception as e:  # noqa: BLE001
                        self.error_logger.error(
                            "Failed message retry for chat=%s msg_id=%s: %s: %s",
                            chat_key, mid, type(e).__name__, e,
                        )
                        # Increment retry count for non-None failures too
                        self.state.increment_failed_message_retry(chat_key, mid)
                        still_failed += 1
                        retried += 1
                        break
            except FloodWaitError as e:
                await self._handle_flood_wait(e)
                continue
            except Exception as e:  # noqa: BLE001
                self.error_logger.error(
                    "Failed to refetch message chat=%s msg_id=%s: %s: %s",
                    chat_key, mid, type(e).__name__, e,
                )
                # Transient fetch failure — keep in failed set
                self.state.increment_failed_message_retry(chat_key, mid)
                still_failed += 1
                continue

        self.state.save()
        print(f"  Message retry: {succeeded} succeeded, {still_failed} still failed, "
              f"{confirmed_unavailable} confirmed unavailable.")

    async def _fetch_single_message(self, entity: Any, message_id: int) -> Any:
        """Fetch a single message by ID, with FloodWait handling."""
        while True:
            try:
                return await self.client.get_messages(entity, ids=message_id)
            except FloodWaitError as e:
                await self._handle_flood_wait(e)
                continue

    async def _retry_failed_media(
        self,
        chat_key: str,
        name: str,
        chat_dir: Path,
        entity: Any,
    ) -> None:
        """Retry media downloads that failed in a previous run.

        Scans the JSONL for messages whose ``media.error`` is set, looks up
        the corresponding message on Telegram, and retries the download.
        Updates the JSONL record with the new media metadata on success.

        This is a best-effort pass: if the message can no longer be found
        on Telegram (e.g. deleted), we skip it silently.
        """
        failed_keys = self.state.get_failed_media_keys(chat_key)
        if not failed_keys:
            return

        logger.info("Retrying %d failed media items for %s", len(failed_keys), chat_key)
        print(f"  Retrying {len(failed_keys)} previously-failed media item(s)...")

        media_dir = chat_dir / "media"
        media_dir.mkdir(exist_ok=True)
        retried = 0
        succeeded = 0

        exporter = MessageExporter(chat_dir, self.config.base_dir)

        for mid, record in list(exporter.iter_messages()):
            media = record.get("media")
            if not media or not media.get("error"):
                continue

            # FloodWait retry loop for this media
            while True:
                try:
                    message = await self._fetch_single_message(entity, mid)
                    if message is None or message.media is None:
                        break
                    # Clear the failed state before retrying
                    media_key = f"{mid}:{self.media_downloader._media_id(message)}"
                    self.state.clear_failed_media(chat_key, media_key)
                    new_media_info = await self.media_downloader.download_media_for_message(
                        message, media_dir, chat_key
                    )
                    if new_media_info and new_media_info.get("local_path"):
                        # Success! Update the record in the JSONL.
                        record["media"] = self._normalize_media_record(new_media_info)
                        exporter.update_record(mid, record)
                        succeeded += 1
                    retried += 1
                    break
                except FloodWaitError as e:
                    await self._handle_flood_wait(e)
                    continue
                except Exception as e:  # noqa: BLE001
                    self.error_logger.error(
                        "Failed media retry for chat=%s msg_id=%s: %s: %s",
                        chat_key, mid, type(e).__name__, e,
                    )
                    break

        print(f"  Media retry: {succeeded}/{retried} succeeded.")

    def _normalize_media_record(self, media_info: dict[str, Any]) -> dict[str, Any]:
        """Normalize a media_info dict for storage in the message record."""
        result = {
            "type": media_info.get("type"),
            "local_path": media_info.get("local_path"),
            "file_size": media_info.get("file_size"),
            "skipped": media_info.get("skipped", False),
            "reason": media_info.get("reason"),
            "error": media_info.get("error"),
        }
        return {k: v for k, v in result.items() if v is not None}

    # --------------------------------------------------------------- record building

    def _build_message_record(
        self,
        message: Any,
        media_info: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build the structured message record persisted to JSONL/JSON.

        Captures all accessible metadata:
          - message_id, date, edited_date
          - sender (id, username, name)
          - text AND caption (Telethon exposes caption via message.text for
            media messages, but we also capture message.message for safety)
          - reply chain (reply_to_message_id, reply_to_top_id)
          - forward info (from_id, from_name, date)
          - service message action
          - media metadata
          - via_bot, post, ttl
        """
        sender_id = None
        sender_username = None
        sender_name = None

        sender = getattr(message, "sender", None)
        if sender is not None:
            sender_id = getattr(sender, "id", None)
            sender_username = getattr(sender, "username", None)
            sender_name = (
                " ".join(
                    p for p in [
                        getattr(sender, "first_name", None),
                        getattr(sender, "last_name", None),
                    ] if p
                )
                or getattr(sender, "title", None)
                or (str(sender.id) if sender_id is not None else None)
            )

        # Reply info
        reply_to_message_id = None
        reply_to_top_id = None
        reply = getattr(message, "reply_to", None)
        if reply is not None:
            reply_to_message_id = getattr(reply, "reply_to_msg_id", None)
            reply_to_top_id = getattr(reply, "reply_to_top_id", None)

        # Forward info
        forwarded_from = None
        forward = getattr(message, "forward", None)
        if forward is not None:
            forwarded_from = {
                "from_id": _serialize_peer(getattr(forward, "from_id", None)),
                "from_name": getattr(forward, "from_name", None),
                "from_username": getattr(forward, "from_username", None),
                "date": forward.date.isoformat() if getattr(forward, "date", None) else None,
                "channel_post": getattr(forward, "channel_post", None),
                "saved_from_message_id": getattr(forward, "saved_from_msg_id", None),
            }

        # Service messages (e.g. "user joined", "photo updated")
        action_info = None
        action = getattr(message, "action", None)
        if action is not None:
            action_info = {
                "type": type(action).__name__,
                "data": _safe_action_data(action),
            }

        # Media info normalization
        media_record: Optional[dict[str, Any]] = None
        if media_info is not None:
            media_record = self._normalize_media_record(media_info)

        # Text: Telethon's message.text includes caption for media messages.
        # We also capture message.message (the raw text) for completeness.
        text = message.text or ""
        raw_message = getattr(message, "message", None) or ""

        # Grouped media (album) info
        grouped_id = getattr(message, "grouped_id", None)

        # Reactions (if present)
        reactions_info = None
        reactions = getattr(message, "reactions", None)
        if reactions is not None:
            try:
                recent = getattr(reactions, "recent_reactions", None) or []
                reactions_info = {
                    "total_count": getattr(reactions, "results", None)
                    and sum(getattr(r, "count", 0) for r in (reactions.results or []))
                    or 0,
                    "types": [
                        getattr(r, "emoticon", None)
                        for r in recent
                        if hasattr(r, "emoticon")
                    ] if recent else [],
                }
            except Exception:  # noqa: BLE001
                reactions_info = None

        return {
            "message_id": message.id,
            "date": message.date.isoformat() if message.date else None,
            "sender_id": sender_id,
            "sender_username": sender_username,
            "sender_name": sender_name,
            "text": text,
            "raw_message": raw_message if raw_message != text else None,
            "reply_to_message_id": reply_to_message_id,
            "reply_to_top_id": reply_to_top_id,
            "is_reply": reply is not None,
            "forwarded_from": forwarded_from,
            "action": action_info,
            "media": media_record,
            "grouped_id": str(grouped_id) if grouped_id else None,
            "edited_date": message.edit_date.isoformat() if message.edit_date else None,
            "post": bool(getattr(message, "post", False)),
            "via_bot_id": _safe_id(getattr(message, "via_bot_id", None)),
            "ttl_seconds": getattr(message, "ttl_period", None),
            "reactions": reactions_info,
            "mentions_count": getattr(message, "mentions", None) if hasattr(message, "mentions") else None,
            "restriction_reason": _safe_str(getattr(message, "restriction_reason", None)),
        }

    # --------------------------------------------------------------- chat info

    async def _write_chat_info(self, entity: Any, chat_dir: Path, display_name: str) -> None:
        """Write chat_info.json for the given chat."""
        info: dict[str, Any] = {
            "display_name": display_name,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        if entity == "me":
            me = await self.client.get_me()
            info.update({
                "type": "saved_messages",
                "user_id": me.id,
                "username": getattr(me, "username", None),
                "first_name": getattr(me, "first_name", None),
                "last_name": getattr(me, "last_name", None),
                # Intentionally do NOT record the phone number in chat_info.json
            })
        elif isinstance(entity, User):
            info.update({
                "type": "private_chat",
                "chat_id": entity.id,
                "username": getattr(entity, "username", None),
                "first_name": getattr(entity, "first_name", None),
                "last_name": getattr(entity, "last_name", None),
                "is_bot": bool(getattr(entity, "bot", False)),
                "is_deleted": bool(getattr(entity, "deleted", False)),
                "is_verified": bool(getattr(entity, "verified", False)),
                "is_restricted": bool(getattr(entity, "restricted", False)),
                "is_support": bool(getattr(entity, "support", False)),
                "is_scam": bool(getattr(entity, "scam", False)),
                "is_fake": bool(getattr(entity, "fake", False)),
                "phone": None,  # Intentionally never recorded
            })
        elif isinstance(entity, Channel):
            info.update({
                "type": "supergroup" if getattr(entity, "megagroup", False) else "channel",
                "chat_id": entity.id,
                "username": getattr(entity, "username", None),
                "title": getattr(entity, "title", None),
                "participants_count": getattr(entity, "participants_count", None),
                "is_verified": bool(getattr(entity, "verified", False)),
                "is_megagroup": bool(getattr(entity, "megagroup", False)),
                "is_creator": bool(getattr(entity, "creator", False)),
            })
        elif isinstance(entity, Chat):
            info.update({
                "type": "group",
                "chat_id": entity.id,
                "title": getattr(entity, "title", None),
                "participants_count": getattr(entity, "participants_count", None),
                "is_creator": bool(getattr(entity, "creator", False)),
            })
        else:
            info.update({
                "type": type(entity).__name__,
                "chat_id": getattr(entity, "id", None),
            })

        info_path = chat_dir / "chat_info.json"
        safe_write_json(info_path, info)

    # --------------------------------------------------------------- summary

    def _merge_media_stats(self) -> None:
        s = self.media_downloader.stats
        self.stats["photos_downloaded"] += s.get("photos", 0)
        self.stats["videos_downloaded"] += s.get("videos", 0)
        self.stats["files_downloaded"] += s.get("documents", 0)
        self.stats["voice_downloaded"] += s.get("voice", 0)
        self.stats["audio_downloaded"] += s.get("audio", 0)
        self.stats["failed_items"] += s.get("failed", 0)
        self.stats["media_retried_success"] += s.get("retried_success", 0)

    def _print_summary(self) -> None:
        s = self.stats
        print()
        print("=" * 60)
        print("  BACKUP COMPLETE")
        print("=" * 60)
        print(f"  Chats processed:       {s['chats_processed']}")
        print(f"  Messages exported:     {s['messages_exported']}")
        print(f"  Photos downloaded:     {s['photos_downloaded']}")
        print(f"  Videos downloaded:     {s['videos_downloaded']}")
        print(f"  Files downloaded:      {s['files_downloaded']}")
        print(f"  Voice downloaded:      {s['voice_downloaded']}")
        print(f"  Audio downloaded:      {s['audio_downloaded']}")
        print(f"  Media retried OK:      {s['media_retried_success']}")
        print(f"  Failed items:          {s['failed_items']}")
        print("=" * 60)

    def _write_metadata(self) -> None:
        """Write backup_metadata.json summarizing the run."""
        metadata = {
            "tool_version": "2.0.0",
            "schema_version": 2,
            "backup_started": self.state.state.get("started_at"),
            "backup_completed": datetime.now(timezone.utc).isoformat(),
            "last_run": self.state.state.get("last_run"),
            "stats": self.stats,
            "state_summary": self.state.summary(),
            "config": self.config.safe_summary(),
            "notes": [
                "All data is stored locally. No data is uploaded anywhere.",
                "Session file and .env contain credentials; keep them private.",
                "messages.jsonl is the authoritative source of truth per chat.",
                "Failed media is tracked in <chat_key>.failed_media.jsonl and retried on rerun.",
            ],
        }
        safe_write_json(self.config.metadata_file, metadata)
        logger.info("Metadata written to %s", self.config.metadata_file)


# ----------------------------------------------------------------- helpers


def _serialize_peer(peer: Any) -> Optional[dict[str, Any]]:
    """Convert a Telethon Peer* object into a JSON-safe dict."""
    if peer is None:
        return None
    peer_id = (
        getattr(peer, "user_id", None)
        or getattr(peer, "chat_id", None)
        or getattr(peer, "channel_id", None)
    )
    return {
        "type": type(peer).__name__,
        "id": int(peer_id) if peer_id else None,
    }


def _safe_id(value: Any) -> Optional[int]:
    """Convert a value to int if possible, else None."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> Optional[str]:
    """Convert a value to string if it's not None, else None."""
    if value is None:
        return None
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return None


def _safe_action_data(action: Any) -> dict[str, Any]:
    """Extract a safe, JSON-serializable subset of a MessageAction."""
    data: dict[str, Any] = {}
    for attr in ("title", "text", "users", "photo_id", "channel_id", "chat_id",
                 "message", "currency", "total_amount", "phone_number",
                 "first_name", "last_name", "inviter_id", "user_id"):
        value = getattr(action, attr, None)
        if value is not None:
            data[attr] = value
    return data
