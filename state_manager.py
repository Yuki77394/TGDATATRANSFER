"""Resume-state manager.

Persists per-chat progress so that interrupted runs can continue from where
they left off. Designed around these invariants:

INVARIANT 1 (JSONL is source of truth):
    The set of message_ids present in ``messages.jsonl`` is the authoritative
    record of what has been exported. ``state.last_message_id`` is ONLY a
    performance hint and may be stale, corrupted, or ahead of the JSONL.

INVARIANT 2 (Failed messages are tracked separately):
    A message that failed to export is recorded in
    ``<chat_key>.failed_messages.jsonl`` and MUST be retried before the
    resume cursor can advance past it. The resume cursor is defined as:
    ``min(state.last_message_id, max(exported_ids))`` — we NEVER resume
    from a point higher than what's actually in the JSONL.

INVARIANT 3 (Failed media is tracked separately):
    Media failures do NOT block message export. A message with failed media
    is still written to JSONL (with ``media.error`` set), and the media key
    is recorded in ``<chat_key>.failed_media.jsonl`` for later retry.

INVARIANT 4 (Atomic writes):
    All state files are written atomically (temp + rename) or appended
    line-by-line with fsync. A crash never leaves a corrupt file.

Resume math (CRITICAL):
    On restart, for a chat, we compute:
        jsonl_max = max(message_id for all records in messages.jsonl)
        state_hint = state.last_message_id  # may be stale/corrupt
        resume_from = min(state_hint, jsonl_max)  # JSONL wins
    We then iterate ``iter_messages(min_id=resume_from)`` and let
    ``exporter.has_message(id)`` skip already-exported records.

    If ``state_hint > jsonl_max`` (state is ahead of JSONL — happens if
    messages failed after state was saved), we resume from ``jsonl_max``
    so the failed messages are re-fetched and retried.

    Failed messages from a previous run are retried BEFORE the main loop
    via ``_retry_failed_messages()``.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils import safe_write_json

logger = logging.getLogger("telegram_backup.state")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateManager:
    """Manages the on-disk backup_state.json file and per-chat logs."""

    SCHEMA_VERSION = 3

    def __init__(self, state_file: Path) -> None:
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict[str, Any] = self._load()
        self._dirty: bool = False
        # Caches for per-chat sets, keyed by chat_key.
        # Lazily populated from JSONL logs on first access.
        self._media_sets: dict[str, set[str]] = {}
        self._failed_media_sets: dict[str, set[str]] = {}
        self._failed_message_sets: dict[str, set[int]] = {}

    # ------------------------------------------------------------------ load/save

    def _load(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return self._fresh_state()
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "chats" not in data:
                logger.warning("State file is malformed; starting fresh.")
                return self._fresh_state()
            # Migrate older schemas
            old_version = data.get("version", 1)
            if old_version < self.SCHEMA_VERSION:
                logger.info(
                    "Migrating state file from v%d to v%d.",
                    old_version, self.SCHEMA_VERSION,
                )
                for chat_state in data.get("chats", {}).values():
                    # v1 -> v2: drop old downloaded_media_keys list
                    chat_state.pop("downloaded_media_keys", None)
                    # v2 -> v3: add failed_message_count
                    chat_state.setdefault("failed_message_count", 0)
                    chat_state.setdefault("failed_media_count", 0)
                data["version"] = self.SCHEMA_VERSION
                self._dirty = True
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not load state file (%s); starting fresh.", e)
            return self._fresh_state()

    def _fresh_state(self) -> dict[str, Any]:
        return {
            "version": self.SCHEMA_VERSION,
            "started_at": _utcnow_iso(),
            "last_run": None,
            "chats": {},
        }

    def save(self) -> None:
        """Persist state to disk atomically. Safe to call repeatedly."""
        self._state["last_run"] = _utcnow_iso()
        try:
            safe_write_json(self.state_file, self._state)
            self._dirty = False
        except OSError as e:
            logger.error("Could not save state file: %s", e)

    # ------------------------------------------------------------------ chat-level

    def _chat_state(self, chat_key: str) -> dict[str, Any]:
        """Return (creating if needed) the per-chat state dict."""
        chats = self._state.setdefault("chats", {})
        if chat_key not in chats:
            chats[chat_key] = {
                "last_message_id": 0,
                "message_count": 0,
                "media_count": 0,
                "failed_media_count": 0,
                "failed_message_count": 0,
                "completed": False,
                "started_at": _utcnow_iso(),
                "completed_at": None,
                "last_error": None,
            }
        return chats[chat_key]

    def get_state_hint(self, chat_key: str) -> int:
        """Return the state file's ``last_message_id`` for a chat.

        This is ONLY a hint. The caller must compute the actual resume point
        as ``min(state_hint, jsonl_max)`` to guarantee JSONL wins.
        """
        return int(self._chat_state(chat_key).get("last_message_id", 0))

    def mark_message_processed(self, chat_key: str, message_id: int) -> None:
        """Update the high-water mark for a chat.

        MUST only be called AFTER the message record has been successfully
        appended to messages.jsonl. This preserves the invariant:
        ``state.last_message_id`` never exceeds the max id actually in JSONL
        (in memory; on disk it may be stale due to delayed save, but that's
        safe because resume uses min(state, jsonl_max)).

        We use a strict monotonic assumption: messages are processed in
        ascending id order. If a later id is seen, it becomes the new mark.
        """
        cs = self._chat_state(chat_key)
        if message_id > cs.get("last_message_id", 0):
            cs["last_message_id"] = message_id
        cs["message_count"] = cs.get("message_count", 0) + 1
        # If this message was previously marked failed, clear it
        if self._is_message_failed(chat_key, message_id):
            self._clear_failed_message(chat_key, message_id)
        self._dirty = True

    # ------------------------------------------------------------------ failed message tracking

    def _failed_messages_log_path(self, chat_key: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in chat_key)
        return self.state_file.parent / f"{safe}.failed_messages.jsonl"

    def _load_failed_message_set(self, chat_key: str) -> set[int]:
        """Lazily load the set of failed message IDs for a chat."""
        if chat_key in self._failed_message_sets:
            return self._failed_message_sets[chat_key]
        path = self._failed_messages_log_path(chat_key)
        result: set[int] = set()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            mid = json.loads(line)
                            if isinstance(mid, int):
                                result.add(mid)
                        except json.JSONDecodeError:
                            continue
            except OSError as e:
                logger.warning("Could not read failed messages log %s: %s", path, e)
        self._failed_message_sets[chat_key] = result
        return result

    def _append_failed_message(self, chat_key: str, message_id: int) -> None:
        path = self._failed_messages_log_path(chat_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(message_id)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass

    def _is_message_failed(self, chat_key: str, message_id: int) -> bool:
        return message_id in self._load_failed_message_set(chat_key)

    def mark_message_failed(self, chat_key: str, message_id: int) -> None:
        """Record a message ID as failed (export failed).

        The message will be retried on the next run via
        ``_retry_failed_messages()``. The resume cursor is NOT advanced past
        this message.
        """
        failed_set = self._load_failed_message_set(chat_key)
        if message_id in failed_set:
            return
        failed_set.add(message_id)
        self._append_failed_message(chat_key, message_id)
        cs = self._chat_state(chat_key)
        cs["failed_message_count"] = cs.get("failed_message_count", 0) + 1
        # Track retry counts per message. Used to distinguish transient
        # failures from permanently-unavailable messages.
        cs.setdefault("failed_message_retries", {})
        cs["failed_message_retries"][str(message_id)] = 0
        self._dirty = True

    def increment_failed_message_retry(self, chat_key: str, message_id: int) -> int:
        """Increment the retry count for a failed message.

        Returns the new retry count. Used by _retry_failed_messages to
        track how many times a message has been retried. After a threshold
        (e.g. 5 consecutive retries returning None), the message is
        considered "confirmed unavailable" and removed from the failed set.
        """
        cs = self._chat_state(chat_key)
        retries = cs.setdefault("failed_message_retries", {})
        key = str(message_id)
        retries[key] = retries.get(key, 0) + 1
        self._dirty = True
        return retries[key]

    def get_failed_message_retry_count(self, chat_key: str, message_id: int) -> int:
        """Return the number of times a failed message has been retried."""
        cs = self._chat_state(chat_key)
        retries = cs.get("failed_message_retries", {})
        return retries.get(str(message_id), 0)

    def _clear_failed_message(self, chat_key: str, message_id: int) -> None:
        """Remove a message from the failed set (after successful export
        or confirmed permanent unavailability)."""
        failed_set = self._load_failed_message_set(chat_key)
        if message_id not in failed_set:
            return
        failed_set.discard(message_id)
        self._rewrite_failed_messages_log(chat_key)
        cs = self._chat_state(chat_key)
        cs["failed_message_count"] = max(0, cs.get("failed_message_count", 0) - 1)
        # Also remove the retry count entry
        cs.get("failed_message_retries", {}).pop(str(message_id), None)
        self._dirty = True

    def _rewrite_failed_messages_log(self, chat_key: str) -> None:
        """Rewrite the failed-messages JSONL from the in-memory set."""
        path = self._failed_messages_log_path(chat_key)
        failed_set = self._load_failed_message_set(chat_key)
        tmp_path = path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for mid in sorted(failed_set):
                f.write(json.dumps(mid))
                f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, path)

    def get_failed_message_ids(self, chat_key: str) -> set[int]:
        """Return a copy of the failed-message ID set for a chat."""
        return set(self._load_failed_message_set(chat_key))

    # ------------------------------------------------------------------ media tracking

    def _media_log_path(self, chat_key: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in chat_key)
        return self.state_file.parent / f"{safe}.media.jsonl"

    def _failed_media_log_path(self, chat_key: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in chat_key)
        return self.state_file.parent / f"{safe}.failed_media.jsonl"

    def _load_media_set(self, chat_key: str, failed: bool = False) -> set[str]:
        """Lazily load the media-key set for a chat from its JSONL log."""
        cache = self._failed_media_sets if failed else self._media_sets
        if chat_key in cache:
            return cache[chat_key]
        path = self._failed_media_log_path(chat_key) if failed else self._media_log_path(chat_key)
        result: set[str] = set()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            key = json.loads(line)
                            if isinstance(key, str):
                                result.add(key)
                        except json.JSONDecodeError:
                            continue
            except OSError as e:
                logger.warning("Could not read media log %s: %s", path, e)
        cache[chat_key] = result
        return result

    def _append_media_log(self, chat_key: str, media_key: str, failed: bool) -> None:
        path = self._failed_media_log_path(chat_key) if failed else self._media_log_path(chat_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(media_key, ensure_ascii=False)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass

    def is_media_downloaded(self, chat_key: str, media_key: str) -> bool:
        return media_key in self._load_media_set(chat_key, failed=False)

    def mark_media_downloaded(self, chat_key: str, media_key: str) -> None:
        media_set = self._load_media_set(chat_key, failed=False)
        if media_key in media_set:
            return
        media_set.add(media_key)
        self._append_media_log(chat_key, media_key, failed=False)
        cs = self._chat_state(chat_key)
        cs["media_count"] = cs.get("media_count", 0) + 1
        # If this media was previously marked failed, clear that
        failed_set = self._load_media_set(chat_key, failed=True)
        if media_key in failed_set:
            failed_set.discard(media_key)
            self._rewrite_failed_media_log(chat_key)
            cs["failed_media_count"] = max(0, cs.get("failed_media_count", 0) - 1)
        self._dirty = True

    def is_media_failed(self, chat_key: str, media_key: str) -> bool:
        return media_key in self._load_media_set(chat_key, failed=True)

    def mark_media_failed(self, chat_key: str, media_key: str) -> None:
        failed_set = self._load_media_set(chat_key, failed=True)
        if media_key in failed_set:
            return
        failed_set.add(media_key)
        self._append_media_log(chat_key, media_key, failed=True)
        cs = self._chat_state(chat_key)
        cs["failed_media_count"] = cs.get("failed_media_count", 0) + 1
        self._dirty = True

    def get_failed_media_keys(self, chat_key: str) -> set[str]:
        return set(self._load_media_set(chat_key, failed=True))

    def clear_failed_media(self, chat_key: str, media_key: str) -> None:
        failed_set = self._load_media_set(chat_key, failed=True)
        if media_key not in failed_set:
            return
        failed_set.discard(media_key)
        self._rewrite_failed_media_log(chat_key)
        cs = self._chat_state(chat_key)
        cs["failed_media_count"] = max(0, cs.get("failed_media_count", 0) - 1)
        self._dirty = True

    def _rewrite_failed_media_log(self, chat_key: str) -> None:
        path = self._failed_media_log_path(chat_key)
        failed_set = self._load_media_set(chat_key, failed=True)
        tmp_path = path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for key in sorted(failed_set):
                f.write(json.dumps(key, ensure_ascii=False))
                f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, path)

    # ------------------------------------------------------------------ chat status

    def mark_chat_completed(self, chat_key: str) -> None:
        cs = self._chat_state(chat_key)
        cs["completed"] = True
        cs["completed_at"] = _utcnow_iso()
        self._dirty = True

    def is_chat_completed(self, chat_key: str) -> bool:
        return bool(self._chat_state(chat_key).get("completed", False))

    def mark_chat_error(self, chat_key: str, error: str) -> None:
        cs = self._chat_state(chat_key)
        cs["last_error"] = error
        cs["last_error_at"] = _utcnow_iso()
        self._dirty = True

    def reset_chat(self, chat_key: str) -> None:
        """Forget all progress for a chat."""
        self._state.setdefault("chats", {}).pop(chat_key, None)
        for path in (
            self._media_log_path(chat_key),
            self._failed_media_log_path(chat_key),
            self._failed_messages_log_path(chat_key),
        ):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        self._media_sets.pop(chat_key, None)
        self._failed_media_sets.pop(chat_key, None)
        self._failed_message_sets.pop(chat_key, None)
        self._dirty = True

    # ------------------------------------------------------------------ summary

    def summary(self) -> dict[str, Any]:
        chats = self._state.get("chats", {})
        return {
            "total_chats_seen": len(chats),
            "completed_chats": sum(1 for c in chats.values() if c.get("completed")),
            "total_messages": sum(int(c.get("message_count", 0)) for c in chats.values()),
            "total_media": sum(int(c.get("media_count", 0)) for c in chats.values()),
            "total_failed_media": sum(int(c.get("failed_media_count", 0)) for c in chats.values()),
            "total_failed_messages": sum(int(c.get("failed_message_count", 0)) for c in chats.values()),
        }
