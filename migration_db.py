"""SQLite-based persistent migration state manager.

Replaces the fragile JSON state with a transactional SQLite database stored
at ``DATA_DIR/state/migration.db``.

Schema
------
The ``messages`` table tracks per-source-message migration state:

    source_message_id  INTEGER PRIMARY KEY
    status             TEXT NOT NULL          -- pending|downloading|downloaded|uploading|uploaded|failed
    retry_count        INTEGER DEFAULT 0
    last_error         TEXT
    media_path         TEXT                    -- relative to DATA_DIR
    has_media          INTEGER DEFAULT 0
    target_message_id  INTEGER                 -- set after successful upload
    source_date        TEXT
    source_text        TEXT                    -- stored for reconciliation
    expected_size      INTEGER                 -- for media integrity validation
    upload_attempt_hash TEXT                   -- used for crash-window reconciliation
    created_at         TEXT
    updated_at         TEXT

Indexes:
    - idx_messages_status (status)             -- fast recovery queries
    - idx_messages_target (target_message_id)  -- reconciliation lookups

Contiguous checkpoint
---------------------
The ``meta`` table stores a single row with ``contiguous_checkpoint`` — the
highest source_message_id such that ALL messages 1..N are in ``uploaded``
state. This is NOT the same as ``MAX(source_message_id) WHERE status='uploaded'``.

Example:
    100 uploaded
    101 failed
    102 uploaded

The contiguous checkpoint is 100, NOT 102. This ensures message 101 is never
silently skipped on resume.

CRASH SAFETY
------------
All mutations use SQLite transactions (auto-committed). A crash never leaves
the database in a corrupt state. The WAL journal mode is enabled for better
concurrency and crash resilience.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("telegram_backup.migration_db")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


VALID_STATUSES = {
    "pending", "downloading", "downloaded", "uploading", "uploaded", "failed"
}


class MigrationDB:
    """SQLite-backed migration state manager."""

    SCHEMA_VERSION = 1

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection = sqlite3.connect(
            str(db_path),
            isolation_level=None,  # autocommit mode; we manage transactions explicitly
            timeout=30,
        )
        self._conn.row_factory = sqlite3.Row
        # Enable WAL for better crash resilience
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=30000")
        except sqlite3.OperationalError:
            pass  # WAL may not be available on all filesystems
        self._init_schema()
        self._migrate_from_json_if_exists()

    # ------------------------------------------------------------------ schema

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                    source_message_id   INTEGER PRIMARY KEY,
                    status              TEXT NOT NULL DEFAULT 'pending',
                    retry_count         INTEGER NOT NULL DEFAULT 0,
                    last_error          TEXT,
                    media_path          TEXT,
                    has_media           INTEGER NOT NULL DEFAULT 0,
                    target_message_id   INTEGER,
                    source_date         TEXT,
                    source_text         TEXT,
                    expected_size       INTEGER DEFAULT 0,
                    upload_attempt_hash TEXT,
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_messages_status
                    ON messages(status);

                CREATE INDEX IF NOT EXISTS idx_messages_target
                    ON messages(target_message_id);

                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );

                INSERT OR IGNORE INTO meta(key, value) VALUES
                    ('schema_version', '1'),
                    ('contiguous_checkpoint', '0'),
                    ('migration_completed', '0'),
                    ('started_at', ''),
                    ('last_run', '');
            """)

    # ------------------------------------------------------------------ JSON migration

    def _migrate_from_json_if_exists(self) -> None:
        """If an old migration_state.json exists, import its data into SQLite.

        Preserves the old file as migration_state.json.bak for safety.
        Only runs if the JSON file exists AND the SQLite DB has no messages yet.
        """
        json_path = self.db_path.parent / "migration_state.json"
        if not json_path.exists():
            return

        # Check if SQLite already has data
        count = self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        if count > 0:
            logger.info(
                "SQLite DB already has %d messages; skipping JSON migration "
                "(keeping JSON as backup).", count,
            )
            return

        logger.info("Found old JSON state file: %s. Migrating to SQLite...", json_path)
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            messages = data.get("messages", {})
            migrated = 0
            with self._conn:
                for key, msg in messages.items():
                    try:
                        sid = int(key)
                        status = msg.get("status", "pending")
                        if status not in VALID_STATUSES:
                            status = "pending"
                        self._conn.execute(
                            """INSERT OR REPLACE INTO messages
                               (source_message_id, status, retry_count, last_error,
                                media_path, has_media, target_message_id, source_date,
                                source_text, expected_size, upload_attempt_hash,
                                created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                sid,
                                status,
                                msg.get("attempts", 0),
                                msg.get("last_error"),
                                msg.get("media_path"),
                                1 if msg.get("has_media") else 0,
                                msg.get("target_message_id"),
                                msg.get("date"),
                                None,  # source_text not in old format
                                0,     # expected_size not in old format
                                None,
                                msg.get("updated_at", _utcnow_iso()),
                                msg.get("updated_at", _utcnow_iso()),
                            ),
                        )
                        migrated += 1
                    except (ValueError, TypeError, sqlite3.Error) as e:
                        logger.warning("Could not migrate message %s: %s", key, e)

                # Migrate meta
                if data.get("completed"):
                    self._conn.execute(
                        "UPDATE meta SET value='1' WHERE key='migration_completed'"
                    )
                self._conn.execute(
                    "UPDATE meta SET value=? WHERE key='last_run'",
                    (data.get("last_run", _utcnow_iso()),),
                )

            # Recompute contiguous checkpoint
            self._recompute_contiguous_checkpoint()

            # Backup the old JSON file
            bak_path = json_path.with_suffix(".json.bak")
            try:
                json_path.rename(bak_path)
                logger.info(
                    "Migrated %d messages from JSON to SQLite. "
                    "Old file backed up as %s", migrated, bak_path,
                )
            except OSError:
                logger.warning(
                    "Could not rename old JSON file; leaving it in place."
                )
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not read old JSON state file (%s); starting fresh.", e)

    # ------------------------------------------------------------------ queries

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        if row is None:
            return None  # type: ignore
        d = dict(row)
        d["has_media"] = bool(d.get("has_media", 0))
        return d

    def get_message(self, source_message_id: int) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM messages WHERE source_message_id = ?",
            (source_message_id,),
        ).fetchone()
        return self._row_to_dict(row)

    def get_status(self, source_message_id: int) -> Optional[str]:
        row = self._conn.execute(
            "SELECT status FROM messages WHERE source_message_id = ?",
            (source_message_id,),
        ).fetchone()
        return row["status"] if row else None

    def is_uploaded(self, source_message_id: int) -> bool:
        return self.get_status(source_message_id) == "uploaded"

    def is_failed(self, source_message_id: int) -> bool:
        return self.get_status(source_message_id) == "failed"

    def get_target_message_id(self, source_message_id: int) -> Optional[int]:
        row = self._conn.execute(
            "SELECT target_message_id FROM messages WHERE source_message_id = ?",
            (source_message_id,),
        ).fetchone()
        return row["target_message_id"] if row else None

    def get_contiguous_checkpoint(self) -> int:
        """Return the highest source_message_id such that ALL discovered
        messages with id <= N are in 'uploaded' state.

        IMPORTANT: This is based on the ordered sequence of messages actually
        DISCOVERED from the source, NOT on every integer ID existing. Real
        Telegram Saved Messages can have gaps (deleted/inaccessible messages).

        Example:
            Discovered IDs: 10, 11, 15, 16
            If 10 and 11 are uploaded, 15 is failed:
              checkpoint = 11 (stops before the failed message)
            If 10, 11, 15, 16 are ALL uploaded:
              checkpoint = 16 (all discovered messages up to 16 are uploaded)

        The checkpoint is stored in the ``meta`` table and updated incrementally
        by ``_maybe_advance_checkpoint()`` after each successful upload.
        """
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'contiguous_checkpoint'"
        ).fetchone()
        return int(row["value"]) if row else 0

    def _recompute_contiguous_checkpoint(self) -> int:
        """Recompute the contiguous checkpoint from scratch.

        Walks the ordered list of ALL discovered message IDs (regardless of
        status) and finds the highest N such that every discovered ID <= N
        is in 'uploaded' state. This correctly handles gaps in Telegram IDs.

        Example:
            Discovered IDs: 10, 11, 15, 16
            Status: 10=uploaded, 11=uploaded, 15=failed, 16=uploaded
            Checkpoint = 11 (stops at the first non-uploaded discovered message)
        """
        # Get ALL discovered message IDs in ascending order
        rows = self._conn.execute(
            "SELECT source_message_id, status FROM messages ORDER BY source_message_id"
        ).fetchall()

        if not rows:
            checkpoint = 0
        else:
            checkpoint = 0
            for row in rows:
                if row["status"] == "uploaded":
                    checkpoint = row["source_message_id"]
                else:
                    break  # First non-uploaded discovered message stops the checkpoint

        with self._conn:
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'contiguous_checkpoint'",
                (str(checkpoint),),
            )
        return checkpoint

    def get_failed_message_ids(self) -> list[int]:
        rows = self._conn.execute(
            "SELECT source_message_id FROM messages WHERE status = 'failed' ORDER BY source_message_id"
        ).fetchall()
        return [r["source_message_id"] for r in rows]

    def get_incomplete_message_ids(self) -> list[int]:
        """Return IDs of messages in non-terminal states (not uploaded, not failed).

        These are messages that were mid-processing when a crash occurred:
        pending, downloading, downloaded, uploading.
        """
        rows = self._conn.execute(
            """SELECT source_message_id FROM messages
               WHERE status IN ('pending', 'downloading', 'downloaded', 'uploading')
               ORDER BY source_message_id"""
        ).fetchall()
        return [r["source_message_id"] for r in rows]

    def get_downloaded_message_ids(self) -> list[int]:
        """Return IDs of messages in 'downloaded' state (media ready, upload not started)."""
        rows = self._conn.execute(
            "SELECT source_message_id FROM messages WHERE status = 'downloaded' ORDER BY source_message_id"
        ).fetchall()
        return [r["source_message_id"] for r in rows]

    def get_uploading_message_ids(self) -> list[int]:
        rows = self._conn.execute(
            "SELECT source_message_id FROM messages WHERE status = 'uploading' ORDER BY source_message_id"
        ).fetchall()
        return [r["source_message_id"] for r in rows]

    def get_retryable_failed_ids(self, max_retries: int) -> list[int]:
        """Return IDs of failed messages that haven't exceeded max_retries."""
        rows = self._conn.execute(
            """SELECT source_message_id FROM messages
               WHERE status = 'failed' AND retry_count < ?
               ORDER BY source_message_id""",
            (max_retries,),
        ).fetchall()
        return [r["source_message_id"] for r in rows]

    # ------------------------------------------------------------------ mutations

    def _upsert_message(
        self,
        source_message_id: int,
        status: str,
        retry_count: Optional[int] = None,
        last_error: Optional[str] = None,
        media_path: Optional[str] = None,
        has_media: Optional[bool] = None,
        target_message_id: Optional[int] = None,
        source_date: Optional[str] = None,
        source_text: Optional[str] = None,
        expected_size: Optional[int] = None,
        upload_attempt_hash: Optional[str] = None,
    ) -> None:
        """Insert or update a message record within a transaction."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}")

        now = _utcnow_iso()
        with self._conn:
            # Check if exists
            existing = self._conn.execute(
                "SELECT source_message_id FROM messages WHERE source_message_id = ?",
                (source_message_id,),
            ).fetchone()

            if existing:
                # Update only provided fields
                updates = []
                params = []
                updates.append("status = ?")
                params.append(status)
                updates.append("updated_at = ?")
                params.append(now)
                if retry_count is not None:
                    updates.append("retry_count = ?")
                    params.append(retry_count)
                if last_error is not None:
                    updates.append("last_error = ?")
                    params.append(last_error)
                if media_path is not None:
                    updates.append("media_path = ?")
                    params.append(media_path)
                if has_media is not None:
                    updates.append("has_media = ?")
                    params.append(1 if has_media else 0)
                if target_message_id is not None:
                    updates.append("target_message_id = ?")
                    params.append(target_message_id)
                if source_date is not None:
                    updates.append("source_date = ?")
                    params.append(source_date)
                if source_text is not None:
                    updates.append("source_text = ?")
                    params.append(source_text)
                if expected_size is not None:
                    updates.append("expected_size = ?")
                    params.append(expected_size)
                if upload_attempt_hash is not None:
                    updates.append("upload_attempt_hash = ?")
                    params.append(upload_attempt_hash)
                params.append(source_message_id)
                self._conn.execute(
                    f"UPDATE messages SET {', '.join(updates)} WHERE source_message_id = ?",
                    params,
                )
            else:
                self._conn.execute(
                    """INSERT INTO messages
                       (source_message_id, status, retry_count, last_error, media_path,
                        has_media, target_message_id, source_date, source_text,
                        expected_size, upload_attempt_hash, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        source_message_id,
                        status,
                        retry_count or 0,
                        last_error,
                        media_path,
                        1 if has_media else 0,
                        target_message_id,
                        source_date,
                        source_text,
                        expected_size or 0,
                        upload_attempt_hash,
                        now,
                        now,
                    ),
                )

    def mark_pending(
        self,
        source_message_id: int,
        source_date: Optional[str] = None,
        source_text: Optional[str] = None,
    ) -> None:
        self._upsert_message(
            source_message_id, status="pending",
            source_date=source_date, source_text=source_text,
        )

    def mark_downloading(self, source_message_id: int) -> None:
        self._upsert_message(source_message_id, status="downloading")

    def mark_downloaded(
        self,
        source_message_id: int,
        media_path: Optional[str] = None,
        has_media: bool = False,
        expected_size: Optional[int] = None,
    ) -> None:
        self._upsert_message(
            source_message_id, status="downloaded",
            media_path=media_path, has_media=has_media,
            expected_size=expected_size,
        )

    def mark_uploading(
        self,
        source_message_id: int,
        upload_attempt_hash: Optional[str] = None,
    ) -> None:
        """Mark as mid-upload and increment retry_count."""
        existing = self.get_message(source_message_id)
        new_count = (existing["retry_count"] + 1) if existing else 1
        self._upsert_message(
            source_message_id, status="uploading",
            retry_count=new_count,
            upload_attempt_hash=upload_attempt_hash,
        )

    def mark_uploaded(
        self, source_message_id: int, target_message_id: int
    ) -> None:
        """Mark as successfully uploaded. Only call AFTER target upload confirmed."""
        self._upsert_message(
            source_message_id, status="uploaded",
            target_message_id=target_message_id,
            last_error=None,
        )
        # Update contiguous checkpoint if applicable
        self._maybe_advance_checkpoint(source_message_id)

    def mark_failed(
        self, source_message_id: int, error: str
    ) -> None:
        """Mark as failed. Does NOT increment retry_count (that happens on mark_uploading)."""
        self._upsert_message(
            source_message_id, status="failed", last_error=error,
        )

    def clear_failed(self, source_message_id: int) -> None:
        """Reset a failed message back to pending (before retrying)."""
        self._upsert_message(
            source_message_id, status="pending", last_error=None,
        )

    def _maybe_advance_checkpoint(self, source_message_id: int) -> None:
        """After marking a message uploaded, advance the contiguous checkpoint
        if this message is the next discovered message after the current checkpoint.

        This handles gaps in Telegram message IDs correctly. The checkpoint
        advances through the ordered sequence of DISCOVERED messages, not
        through every integer.

        Example:
            Discovered IDs: 10, 11, 15, 16
            Current checkpoint: 0
            Mark 10 as uploaded → checkpoint advances to 10 (first discovered)
            Mark 11 as uploaded → checkpoint advances to 11
            Mark 15 as uploaded → checkpoint advances to 15
            Mark 16 as uploaded → checkpoint advances to 16
        """
        current = self.get_contiguous_checkpoint()

        # If the just-uploaded message has ID <= current, checkpoint is already
        # ahead (e.g., message was re-uploaded after being marked failed).
        if source_message_id <= current:
            return

        # Find the next discovered message ID after the current checkpoint
        next_row = self._conn.execute(
            """SELECT source_message_id FROM messages
               WHERE source_message_id > ?
               ORDER BY source_message_id
               LIMIT 1""",
            (current,),
        ).fetchone()

        if next_row is None:
            # No discovered messages after current checkpoint — nothing to advance
            return

        next_id = next_row["source_message_id"]

        # Only advance if the just-uploaded message IS the next discovered one
        # AND it is uploaded (which it is, since we just marked it)
        if source_message_id != next_id:
            # The just-uploaded message is not the next in sequence.
            # This means there's a gap in the discovered sequence (e.g., we
            # uploaded 15 but 11 is still pending/failed). Don't advance.
            return

        # Walk forward through consecutive uploaded discovered messages
        new_checkpoint = current
        rows = self._conn.execute(
            """SELECT source_message_id, status FROM messages
               WHERE source_message_id > ?
               ORDER BY source_message_id""",
            (current,),
        ).fetchall()

        for row in rows:
            if row["status"] == "uploaded":
                new_checkpoint = row["source_message_id"]
            else:
                break  # Stop at first non-uploaded discovered message

        if new_checkpoint > current:
            with self._conn:
                self._conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'contiguous_checkpoint'",
                    (str(new_checkpoint),),
                )

    def mark_completed(self) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE meta SET value = '1' WHERE key = 'migration_completed'"
            )
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'last_run'",
                (_utcnow_iso(),),
            )

    def is_completed(self) -> bool:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'migration_completed'"
        ).fetchone()
        return row and row["value"] == "1"

    def update_last_run(self) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'last_run'",
                (_utcnow_iso(),),
            )

    # ------------------------------------------------------------------ find by target (reconciliation)

    def find_by_upload_hash(self, upload_attempt_hash: str) -> Optional[dict[str, Any]]:
        """Find a message by its upload_attempt_hash (for crash-window reconciliation)."""
        row = self._conn.execute(
            "SELECT * FROM messages WHERE upload_attempt_hash = ?",
            (upload_attempt_hash,),
        ).fetchone()
        return self._row_to_dict(row)

    def find_by_target_message_id(self, target_message_id: int) -> Optional[dict[str, Any]]:
        """Find a source message by its target message ID."""
        row = self._conn.execute(
            "SELECT * FROM messages WHERE target_message_id = ?",
            (target_message_id,),
        ).fetchone()
        return self._row_to_dict(row)

    # ------------------------------------------------------------------ summary

    def summary(self) -> dict[str, Any]:
        counts = {}
        for status in VALID_STATUSES:
            row = self._conn.execute(
                "SELECT COUNT(*) as c FROM messages WHERE status = ?", (status,)
            ).fetchone()
            counts[status] = row["c"]
        total = self._conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"]
        return {
            "total_tracked": total,
            "counts": counts,
            "contiguous_checkpoint": self.get_contiguous_checkpoint(),
            "completed": self.is_completed(),
        }

    # ------------------------------------------------------------------ cleanup

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            pass
