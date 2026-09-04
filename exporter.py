"""Message exporter.

Writes messages to two files per chat:
  - ``messages.jsonl``  (append-only, line-delimited JSON; PRIMARY source of truth)
  - ``messages.json``   (rebuilt from the JSONL on demand; convenient for readers)

Crash safety & corruption recovery
----------------------------------
The JSONL file is append-only. Each ``append_message`` call:

  1. Checks the in-memory set; if present, returns False (idempotent).
  2. Serializes the record to a single JSON line.
  3. Opens the file in append mode, writes line + newline, flushes, fsyncs.
  4. ONLY THEN updates the in-memory ``_existing_ids`` / ``_max_id`` cache.

On startup, ``_load_existing`` scans the JSONL and handles corruption:

  - **Partial trailing line** (crash mid-write): the line fails JSON parsing.
    It is excluded from the in-memory set. ``_partial_tail_offset`` records
    the byte position. Before the next append, the partial line is
    physically truncated.

  - **Malformed line in the middle** (rare — disk corruption): the bad line
    is logged with a WARNING, skipped, but iteration CONTINUES. All
    subsequent valid records are loaded into memory. The bad line is NOT
    removed automatically (to avoid destroying data that might be
    recoverable). A ``compact_jsonl()`` method is available to rewrite
    the file dropping bad lines.

  - **Duplicate message_ids** (from a buggy old version): the LAST
    occurrence wins (overwrites earlier in the in-memory set). On
    ``compact_jsonl()`` duplicates are removed.

This means a crash can NEVER cause:
  - Lost messages (in-memory set is rebuilt from disk on restart).
  - Corrupted JSONL (partial tail is truncated; middle corruption is
    preserved but skipped, and can be compacted later).
  - Duplicate messages (the in-memory set is the dedup authority).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger("telegram_backup.exporter")


class MessageExporter:
    """Append-only JSONL writer with JSON rebuild support."""

    def __init__(self, chat_dir: Path, base_dir: Path) -> None:
        self.chat_dir = chat_dir
        self.base_dir = base_dir
        self.jsonl_path = chat_dir / "messages.jsonl"
        self.json_path = chat_dir / "messages.json"
        self.chat_dir.mkdir(parents=True, exist_ok=True)

        # In-memory set of message ids already in the JSONL.
        self._existing_ids: set[int] = set()
        self._max_id: int = 0
        # Byte offset of the end of the last valid line. Used to truncate
        # a partial TAIL line before appending. Only set if the LAST line
        # is malformed; middle corruption does NOT set this.
        self._partial_tail_offset: int = 0
        # True if we found any malformed line anywhere (for logging).
        self._has_corruption: bool = False
        self._load_existing()

    # --------------------------------------------------------------- load

    def _load_existing(self) -> None:
        """Scan the existing JSONL to recover state (id set + max id).

        Handles three corruption scenarios:
          1. Partial trailing line (crash mid-write): excluded from set,
             ``_partial_tail_offset`` set for later truncation.
          2. Malformed middle line: logged and skipped; iteration continues.
             Subsequent valid records ARE loaded.
          3. Duplicate ids: last occurrence wins (later records overwrite
             earlier in the in-memory set).
        """
        if not self.jsonl_path.exists():
            return

        try:
            with open(self.jsonl_path, "r", encoding="utf-8") as f:
                offset = 0
                last_valid_end = 0
                line_number = 0
                last_line_was_partial = False
                for line in f:
                    line_number += 1
                    line_bytes = line.encode("utf-8")
                    line_stripped = line.rstrip("\n")
                    if not line_stripped:
                        offset += len(line_bytes)
                        continue
                    try:
                        msg = json.loads(line_stripped)
                    except json.JSONDecodeError:
                        # Is this the last line? We need to peek ahead.
                        # Since we're iterating, we'll mark this position
                        # and continue reading. If we find more valid lines
                        # after it, it's middle corruption (we keep the
                        # later valid records). If no more lines, it's a
                        # partial tail.
                        self._has_corruption = True
                        logger.warning(
                            "Malformed JSONL line %d at offset %d in %s: "
                            "skipping (will be preserved on disk unless "
                            "compact_jsonl() is called).",
                            line_number, offset, self.jsonl_path,
                        )
                        offset += len(line_bytes)
                        last_line_was_partial = True
                        continue
                    # Valid line
                    mid = msg.get("message_id")
                    if isinstance(mid, int):
                        self._existing_ids.add(mid)
                        if mid > self._max_id:
                            self._max_id = mid
                    offset += len(line_bytes)
                    last_valid_end = offset
                    last_line_was_partial = False

                # If the very last line was partial, set truncation offset
                if last_line_was_partial:
                    self._partial_tail_offset = last_valid_end
                    logger.warning(
                        "Partial trailing line detected in %s; will truncate "
                        "to offset %d before next append.",
                        self.jsonl_path, last_valid_end,
                    )
        except OSError as e:
            logger.warning("Could not read existing JSONL %s: %s", self.jsonl_path, e)

    def _truncate_partial_tail(self) -> None:
        """If a partial TAIL line was detected, truncate the file to the
        last valid line boundary before appending.

        This ONLY truncates trailing partial lines. Middle corruption is
        preserved on disk (use ``compact_jsonl()`` to clean it up).
        """
        if self._partial_tail_offset <= 0:
            return
        try:
            current_size = self.jsonl_path.stat().st_size
        except OSError:
            return
        if current_size > self._partial_tail_offset:
            try:
                with open(self.jsonl_path, "r+b") as f:
                    f.truncate(self._partial_tail_offset)
                logger.info(
                    "Truncated partial JSONL tail in %s from %d to %d bytes.",
                    self.jsonl_path, current_size, self._partial_tail_offset,
                )
            except OSError as e:
                logger.warning("Could not truncate partial JSONL tail: %s", e)
        self._partial_tail_offset = 0

    # --------------------------------------------------------------- queries

    def has_message(self, message_id: int) -> bool:
        return message_id in self._existing_ids

    def get_max_message_id(self) -> int:
        return self._max_id

    def count(self) -> int:
        return len(self._existing_ids)

    def get_exported_ids(self) -> set[int]:
        """Return a copy of the set of exported message IDs."""
        return set(self._existing_ids)

    def has_corruption(self) -> bool:
        """Return True if any malformed line was found during loading."""
        return self._has_corruption

    # --------------------------------------------------------------- append

    def append_message(self, message_data: dict[str, Any]) -> bool:
        """Append a single message to the JSONL file.

        Returns True if the message was newly written, False if it was
        already present (idempotent no-op).

        Order of operations (CRITICAL for crash safety):
          1. Check in-memory set; if present, return False (no disk write).
          2. Truncate any partial TAIL from a previous crash.
          3. Serialize the record to a single JSON line.
          4. Open file in append mode, write line + newline, flush, fsync.
          5. ONLY THEN update the in-memory set and max_id.

        If we crash during step 4, the file may have a partial line at the
        end. On restart, ``_load_existing`` detects this and truncates it
        before the next append. The in-memory set is unchanged, so we never
        claim a message is present when it isn't on disk.
        """
        mid = message_data.get("message_id")
        if isinstance(mid, int) and mid in self._existing_ids:
            return False

        # Remove any partial tail left by a previous crash
        self._truncate_partial_tail()

        # Serialize BEFORE opening the file
        line = json.dumps(message_data, ensure_ascii=False, default=str)

        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass

        if isinstance(mid, int):
            self._existing_ids.add(mid)
            if mid > self._max_id:
                self._max_id = mid

        return True

    # --------------------------------------------------------------- rebuild json

    def write_json(self) -> int:
        """Rebuild messages.json from the JSONL file.

        Returns the number of messages written. Skips malformed lines.
        Deduplicates by message_id, keeping the LAST occurrence.
        """
        messages = self._read_all_valid_records()

        # Deduplicate by message_id, keeping last occurrence
        by_id: dict[int, dict[str, Any]] = {}
        no_id: list[dict[str, Any]] = []
        for m in messages:
            mid = m.get("message_id")
            if isinstance(mid, int):
                by_id[mid] = m
            else:
                no_id.append(m)
        messages = list(by_id.values()) + no_id
        messages.sort(key=lambda m: m.get("message_id", 0) if isinstance(m.get("message_id"), int) else 0)

        tmp_path = self.json_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(
                {"count": len(messages), "messages": messages},
                f,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, self.json_path)
        return len(messages)

    def _read_all_valid_records(self) -> list[dict[str, Any]]:
        """Read all valid JSON records from the JSONL file.

        Malformed lines are skipped (with a warning logged). Used by
        ``write_json()`` and ``iter_messages()``.
        """
        messages: list[dict[str, Any]] = []
        if not self.jsonl_path.exists():
            return messages
        try:
            with open(self.jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        messages.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError as e:
            logger.warning("Could not read JSONL: %s", e)
        return messages

    # --------------------------------------------------------------- iteration

    def iter_messages(self) -> Iterator[tuple[int, dict[str, Any]]]:
        """Yield (message_id, record) for every valid line in the JSONL.

        Used by the failed-media retry pass to find messages whose media
        failed. Malformed lines are skipped.
        """
        for record in self._read_all_valid_records():
            mid = record.get("message_id")
            if isinstance(mid, int):
                yield mid, record

    # --------------------------------------------------------------- update record

    def update_record(self, message_id: int, new_record: dict[str, Any]) -> bool:
        """Update an existing record in the JSONL by message_id.

        Rewrites the entire JSONL file atomically (temp + rename). Used by
        the failed-media retry pass when a previously-failed media
        download succeeds and we need to update the media metadata in the
        message record.

        Returns True if the record was found and updated, False otherwise.
        """
        records = self._read_all_valid_records()
        found = False
        for i, r in enumerate(records):
            if r.get("message_id") == message_id:
                records[i] = new_record
                found = True
                break
        if not found:
            return False

        # Rewrite atomically
        tmp_path = self.jsonl_path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False, default=str))
                f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, self.jsonl_path)
        return True

    # --------------------------------------------------------------- cleanup

    def compact_jsonl(self) -> int:
        """Rewrite the JSONL file, dropping malformed lines AND duplicates.

        Returns the number of records kept. This is the only method that
        physically removes middle-corruption. Not invoked automatically;
        the user can call it via a maintenance script if needed.
        """
        records = self._read_all_valid_records()

        # Deduplicate by message_id, keeping last occurrence
        by_id: dict[int, dict[str, Any]] = {}
        for r in records:
            mid = r.get("message_id")
            if isinstance(mid, int):
                by_id[mid] = r
        sorted_records = sorted(by_id.values(), key=lambda m: m.get("message_id", 0))

        tmp_path = self.jsonl_path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for record in sorted_records:
                f.write(json.dumps(record, ensure_ascii=False, default=str))
                f.write("\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_path, self.jsonl_path)
        kept = len(sorted_records)
        logger.info("Compacted JSONL: kept %d records in %s", kept, self.jsonl_path)

        # Reload in-memory state
        self._existing_ids = set(by_id.keys())
        self._max_id = max(by_id.keys()) if by_id else 0
        self._has_corruption = False
        self._partial_tail_offset = 0
        return kept
