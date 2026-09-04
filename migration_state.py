"""Persistent migration state manager.

Tracks the status of every source message being migrated to the target
account's Saved Messages. State is persisted to ``DATA_DIR/state/`` so it
survives Railway restarts, crashes, and redeployments.

State machine per message
-------------------------
    pending  → downloaded  → uploading  → uploaded
        ↓           ↓             ↓
      failed     failed        failed

- ``pending``   : message discovered, not yet downloaded
- ``downloaded``: media downloaded to DATA_DIR/media/ (or text-only, no media)
- ``uploading`` : upload to target started but not confirmed (crash window)
- ``uploaded``  : target upload confirmed, TARGET_MESSAGE_ID recorded
- ``failed``    : permanent failure after max retries; needs manual attention

CRASH SAFETY (critical invariant)
---------------------------------
The authoritative resume position is the set of SOURCE_MESSAGE_IDs in the
``uploaded`` state. We resume from the highest uploaded source message ID,
fetching only newer messages from the source.

Messages in ``uploading`` state are treated as NOT complete on restart —
they must be re-uploaded (Telegram deduplication may or may not catch the
duplicate; we prioritize no data loss over exactly-once).

Messages in ``failed`` state are retried separately before the main loop.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils import safe_write_json

logger = logging.getLogger("telegram_backup.migration_state")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Valid status transitions
VALID_STATUSES = {"pending", "downloaded", "uploading", "uploaded", "failed"}


class MigrationStateManager:
    """Manages the persistent migration state file.

    State is stored as a JSON file with this structure::

        {
            "version": 1,
            "started_at": "...",
            "last_run": "...",
            "completed": false,
            "highest_source_id_processed": 0,
            "messages": {
                "12345": {
                    "status": "uploaded",
                    "source_message_id": 12345,
                    "target_message_id": 67890,
                    "date": "...",
                    "has_media": true,
                    "media_path": "media/msg_12345.jpg",
                    "attempts": 1,
                    "last_error": null,
                    "updated_at": "..."
                }
            }
        }

    For large migrations, the ``messages`` dict can grow large. If this
    becomes a memory issue, we can switch to a SQLite database — but for
    typical Saved Messages (thousands to tens of thousands of messages),
    a JSON file loaded into memory is practical.
    """

    SCHEMA_VERSION = 1

    def __init__(self, state_file: Path) -> None:
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict[str, Any] = self._load()
        self._dirty: bool = False

    # ---------------------------------------------------------------- load/save

    def _load(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return self._fresh_state()
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "messages" not in data:
                logger.warning("State file malformed; starting fresh.")
                return self._fresh_state()
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not load state file (%s); starting fresh.", e)
            return self._fresh_state()

    def _fresh_state(self) -> dict[str, Any]:
        return {
            "version": self.SCHEMA_VERSION,
            "started_at": _utcnow_iso(),
            "last_run": None,
            "completed": False,
            "highest_source_id_processed": 0,
            "messages": {},
        }

    def save(self) -> None:
        """Persist state to disk atomically."""
        self._state["last_run"] = _utcnow_iso()
        try:
            safe_write_json(self.state_file, self._state)
            self._dirty = False
        except OSError as e:
            logger.error("Could not save state file: %s", e)

    # ---------------------------------------------------------------- queries

    def get_status(self, source_message_id: int) -> str | None:
        """Return the status of a message, or None if not tracked."""
        msg = self._state["messages"].get(str(source_message_id))
        return msg.get("status") if msg else None

    def is_uploaded(self, source_message_id: int) -> bool:
        return self.get_status(source_message_id) == "uploaded"

    def is_failed(self, source_message_id: int) -> bool:
        return self.get_status(source_message_id) == "failed"

    def get_target_message_id(self, source_message_id: int) -> int | None:
        msg = self._state["messages"].get(str(source_message_id))
        if msg:
            return msg.get("target_message_id")
        return None

    def get_highest_uploaded_source_id(self) -> int:
        """Return the highest source message ID in 'uploaded' state.

        Used to compute the resume point for incremental scanning.
        Returns 0 if no messages have been uploaded.
        """
        highest = 0
        for msg in self._state["messages"].values():
            if msg.get("status") == "uploaded":
                sid = msg.get("source_message_id", 0)
                if sid > highest:
                    highest = sid
        return highest

    def get_highest_processed_source_id(self) -> int:
        """Return the highest source message ID in ANY tracked state."""
        return self._state.get("highest_source_id_processed", 0)

    def get_failed_message_ids(self) -> list[int]:
        """Return list of source message IDs in 'failed' state."""
        result = []
        for msg in self._state["messages"].values():
            if msg.get("status") == "failed":
                sid = msg.get("source_message_id")
                if isinstance(sid, int):
                    result.append(sid)
        return sorted(result)

    def get_uploading_message_ids(self) -> list[int]:
        """Return list of source message IDs in 'uploading' state.

        These are messages that were mid-upload when a crash occurred.
        They must be re-uploaded (or verified) on restart.
        """
        result = []
        for msg in self._state["messages"].values():
            if msg.get("status") == "uploading":
                sid = msg.get("source_message_id")
                if isinstance(sid, int):
                    result.append(sid)
        return sorted(result)

    def get_downloaded_message_ids(self) -> list[int]:
        """Return list of source message IDs in 'downloaded' state."""
        result = []
        for msg in self._state["messages"].values():
            if msg.get("status") == "downloaded":
                sid = msg.get("source_message_id")
                if isinstance(sid, int):
                    result.append(sid)
        return sorted(result)

    # ---------------------------------------------------------------- mutations

    def _get_or_create(self, source_message_id: int) -> dict[str, Any]:
        key = str(source_message_id)
        msgs = self._state["messages"]
        if key not in msgs:
            msgs[key] = {
                "status": "pending",
                "source_message_id": source_message_id,
                "target_message_id": None,
                "date": None,
                "has_media": False,
                "media_path": None,
                "attempts": 0,
                "last_error": None,
                "updated_at": _utcnow_iso(),
            }
        return msgs[key]

    def mark_pending(self, source_message_id: int, date: str | None = None) -> None:
        msg = self._get_or_create(source_message_id)
        msg["status"] = "pending"
        if date:
            msg["date"] = date
        msg["updated_at"] = _utcnow_iso()
        self._update_highest(source_message_id)
        self._dirty = True

    def mark_downloaded(
        self,
        source_message_id: int,
        media_path: str | None = None,
        has_media: bool = False,
    ) -> None:
        msg = self._get_or_create(source_message_id)
        msg["status"] = "downloaded"
        msg["media_path"] = media_path
        msg["has_media"] = has_media
        msg["updated_at"] = _utcnow_iso()
        self._dirty = True

    def mark_uploading(self, source_message_id: int) -> None:
        """Mark a message as mid-upload. CRASH WINDOW: if we crash after
        this but before mark_uploaded, the message will be re-uploaded
        on restart (potential duplicate, but no data loss)."""
        msg = self._get_or_create(source_message_id)
        msg["status"] = "uploading"
        msg["attempts"] = msg.get("attempts", 0) + 1
        msg["updated_at"] = _utcnow_iso()
        self._dirty = True

    def mark_uploaded(
        self, source_message_id: int, target_message_id: int
    ) -> None:
        """Mark a message as successfully uploaded. Only call this AFTER
        the target upload is confirmed."""
        msg = self._get_or_create(source_message_id)
        msg["status"] = "uploaded"
        msg["target_message_id"] = target_message_id
        msg["last_error"] = None
        msg["updated_at"] = _utcnow_iso()
        self._update_highest(source_message_id)
        self._dirty = True

    def mark_failed(
        self, source_message_id: int, error: str
    ) -> None:
        msg = self._get_or_create(source_message_id)
        msg["status"] = "failed"
        msg["last_error"] = error
        msg["updated_at"] = _utcnow_iso()
        self._dirty = True

    def clear_failed(self, source_message_id: int) -> None:
        """Remove a message from the failed set (before retrying)."""
        msg = self._get_or_create(source_message_id)
        msg["status"] = "pending"
        msg["last_error"] = None
        msg["updated_at"] = _utcnow_iso()
        self._dirty = True

    def _update_highest(self, source_message_id: int) -> None:
        if source_message_id > self._state.get("highest_source_id_processed", 0):
            self._state["highest_source_id_processed"] = source_message_id

    def mark_completed(self) -> None:
        """Mark the entire migration as completed."""
        self._state["completed"] = True
        self._state["completed_at"] = _utcnow_iso()
        self._dirty = True

    def is_completed(self) -> bool:
        return bool(self._state.get("completed", False))

    # ---------------------------------------------------------------- summary

    def summary(self) -> dict[str, Any]:
        msgs = self._state.get("messages", {})
        counts = {"pending": 0, "downloaded": 0, "uploading": 0,
                  "uploaded": 0, "failed": 0}
        for m in msgs.values():
            s = m.get("status", "pending")
            if s in counts:
                counts[s] += 1
        return {
            "total_tracked": len(msgs),
            "counts": counts,
            "highest_source_id_processed": self._state.get("highest_source_id_processed", 0),
            "highest_uploaded": self.get_highest_uploaded_source_id(),
            "completed": self._state.get("completed", False),
        }
