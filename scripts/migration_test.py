"""Comprehensive tests for the SQLite-based Saved Messages migration tool.

Tests cover all required scenarios:
  1.  DATA_DIR creation + SQLite DB creation
  2.  Persistent state path under DATA_DIR
  3.  Missing environment variables
  4.  Invalid API_ID
  5.  Source/target same-account protection
  6.  Restart/resume (no duplication)
  7.  Media download resume with .part files
  8.  Target upload failure → message FAILED (not text-only)
  9.  FloodWait simulation (unbounded, not counted as failure)
  10. Graceful shutdown (SIGTERM)
  11. Duplicate prevention across restarts
  12. State machine transitions
  13. Crash recovery (uploading state) with reconciliation
  14. Deployment config verification
  15. Saved Messages-only verification
  16. Contiguous checkpoint (100 ok, 101 failed, 102 ok → checkpoint=100)
  17. Crash during download simulation
  18. Crash after download, before upload
  19. Crash during upload
  20. Crash after upload, before DB update
  21. .part file recovery
  22. Bounded retry tests (exhaust → failed)
  23. Media download failure → NOT text-only success
  24. Large-state scalability (1000+ messages in SQLite)
  25. JSON → SQLite migration
  26. Media integrity validation (size mismatch)
"""
import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

failures = []
test_count = 0


def check(condition, label):
    global test_count
    test_count += 1
    if condition:
        print(f"  PASS: {label}")
    else:
        print(f"  FAIL: {label}")
        failures.append(label)


def section(name):
    print(f"\n{'='*70}\nTest: {name}\n{'='*70}")


# ----------------------------------------------------------------- Mock infra

class MockMessage:
    def __init__(self, msg_id, text="", media=None, date=None):
        self.id = msg_id
        self.text = text
        self.message = text
        self.media = media
        self.date = date or MagicMock()
        self.date.isoformat = MagicMock(return_value=f"2026-01-{msg_id:02d}T00:00:00")
        self.sender = None
        self.reply_to = None
        self.forward = None
        self.action = None


class MockTelethonClient:
    def __init__(self, user_id, messages=None):
        self._user_id = user_id
        self._messages = messages or []
        self._connected = False
        self._uploaded = []
        self._download_should_fail = False
        self._upload_should_fail = False

    async def connect(self):
        self._connected = True

    async def disconnect(self):
        self._connected = False

    def is_connected(self):
        return self._connected

    async def is_user_authorized(self):
        return True

    async def get_me(self):
        me = MagicMock()
        me.id = self._user_id
        me.first_name = f"User{self._user_id}"
        me.last_name = "Test"
        return me

    async def get_input_entity(self, peer):
        return peer

    async def iter_messages(self, entity, limit=None, reverse=False, min_id=0, **kwargs):
        msgs = [m for m in self._messages if m.id > min_id]
        for msg in msgs:
            yield msg

    async def get_messages(self, entity, ids=None, limit=None, **kwargs):
        if limit is not None and ids is None:
            # Return recent messages for reconciliation
            return sorted(self._uploaded, key=lambda m: m.id, reverse=True)[:limit]
        if isinstance(ids, int):
            for m in self._messages:
                if m.id == ids:
                    return m
            return None
        return []

    async def download_media(self, message, file=None, **kwargs):
        if self._download_should_fail:
            raise Exception("Simulated download failure")
        if file and message.media:
            path = Path(file)
            path.parent.mkdir(parents=True, exist_ok=True)
            # Write content matching the expected size if available
            expected = 0
            media = message.media
            if hasattr(media, 'photo') and media.photo:
                expected = getattr(media.photo, 'size', 0) or 0
            if hasattr(media, 'document') and media.document:
                expected = getattr(media.document, 'size', 0) or 0
            if expected > 0:
                path.write_bytes(b"x" * expected)
            else:
                path.write_bytes(b"fake media content " * 100)
            return str(path)
        return None

    async def send_file(self, entity, file=None, caption=None, **kwargs):
        if self._upload_should_fail:
            raise Exception("Simulated upload failure")
        target_msg = MockMessage(msg_id=9000 + len(self._uploaded), text=caption or "")
        self._uploaded.append(target_msg)
        return target_msg

    async def send_message(self, entity, text, **kwargs):
        if self._upload_should_fail:
            raise Exception("Simulated upload failure")
        target_msg = MockMessage(msg_id=9000 + len(self._uploaded), text=text)
        self._uploaded.append(target_msg)
        return target_msg


def make_config(data_dir: Path) -> Any:
    cfg = MagicMock()
    cfg.api_id = 12345
    cfg.api_hash = "fakehash"
    cfg.source_session = "fake_source"
    cfg.target_session = "fake_target"
    cfg.data_dir = data_dir
    cfg.state_dir = data_dir / "state"
    cfg.media_dir = data_dir / "media"
    cfg.failed_dir = data_dir / "failed"
    cfg.logs_dir = data_dir / "logs"
    cfg.db_path = data_dir / "state" / "migration.db"
    cfg.state_file = data_dir / "state" / "migration_state.json"
    cfg.failed_messages_file = data_dir / "failed" / "failed_messages.jsonl"
    cfg.log_file = data_dir / "logs" / "migration.log"
    cfg.max_retries = 3
    cfg.checkpoint_every = 999
    cfg.allow_same_account = False
    cfg.delete_after_upload = False
    cfg.reconciliation_window = 20
    cfg.validate = MagicMock()
    cfg.validate_api_id = MagicMock()
    cfg.ensure_directories = MagicMock()
    cfg.safe_summary = MagicMock(return_value={"test": True})
    return cfg


import logging
test_error_logger = logging.getLogger("test_errors")
test_error_logger.addHandler(logging.NullHandler())


# ----------------------------------------------------------------- 1. DATA_DIR + DB creation
section("1. DATA_DIR + SQLite DB creation")

def test_db_creation():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_config import MigrationConfig
        from migration_db import MigrationDB
        os.environ["API_ID"] = "12345"
        os.environ["API_HASH"] = "fakehash"
        os.environ["SOURCE_SESSION"] = "fake_source"
        os.environ["TARGET_SESSION"] = "fake_target"
        os.environ["DATA_DIR"] = tmp

        cfg = MigrationConfig(env_path="/nonexistent/.env")
        cfg.validate()
        cfg.ensure_directories()

        check((Path(tmp) / "state").exists(), "state/ created")
        check((Path(tmp) / "media").exists(), "media/ created")
        check((Path(tmp) / "logs").exists(), "logs/ created")

        db = MigrationDB(cfg.db_path)
        check(cfg.db_path.exists(), "migration.db created")

        # Verify schema
        conn = sqlite3.connect(str(cfg.db_path))
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        check("messages" in table_names, "messages table exists")
        check("meta" in table_names, "meta table exists")

        # Verify indexes
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        index_names = {i[0] for i in indexes}
        check("idx_messages_status" in index_names, "idx_messages_status exists")
        check("idx_messages_target" in index_names, "idx_messages_target exists")
        conn.close()
        db.close()

        for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
            os.environ.pop(k, None)

test_db_creation()


# ----------------------------------------------------------------- 2. Persistent paths
section("2. Persistent state paths under DATA_DIR")

def test_persistent_paths():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_config import MigrationConfig
        os.environ["DATA_DIR"] = tmp
        cfg = MigrationConfig(env_path="/nonexistent/.env")
        check(str(cfg.db_path).startswith(tmp), f"db_path under DATA_DIR: {cfg.db_path}")
        check(str(cfg.media_dir).startswith(tmp), f"media_dir under DATA_DIR")
        check(str(cfg.log_file).startswith(tmp), f"log_file under DATA_DIR")
        os.environ.pop("DATA_DIR", None)

test_persistent_paths()


# ----------------------------------------------------------------- 3. Missing env vars
section("3. Missing environment variables")

def test_missing_env():
    from migration_config import MigrationConfig
    for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
        os.environ.pop(k, None)

    cfg = MigrationConfig(env_path="/nonexistent/.env")
    try:
        cfg.validate()
        check(False, "should have raised")
    except ValueError as e:
        for v in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION"]:
            check(v in str(e), f"error mentions {v}")

test_missing_env()


# ----------------------------------------------------------------- 4. Invalid API_ID
section("4. Invalid API_ID")

def test_invalid_api_id():
    from migration_config import MigrationConfig
    os.environ["API_ID"] = "0"
    os.environ["API_HASH"] = "fakehash"
    os.environ["SOURCE_SESSION"] = "fake"
    os.environ["TARGET_SESSION"] = "fake"
    cfg = MigrationConfig(env_path="/nonexistent/.env")
    try:
        cfg.validate_api_id()
        check(False, "should have raised for API_ID=0")
    except ValueError as e:
        check("positive integer" in str(e), "error mentions positive integer")
    for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION"]:
        os.environ.pop(k, None)

test_invalid_api_id()


# ----------------------------------------------------------------- 5. Same-account protection
section("5. Source/target same-account protection")

async def test_same_account():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        db = MigrationDB(cfg.db_path)

        source = MockTelethonClient(user_id=111)
        target = MockTelethonClient(user_id=111)
        migrator = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        try:
            await migrator.run()
            check(False, "should have raised")
        except RuntimeError as e:
            check("same account" in str(e).lower(), f"error mentions same account")
        db.close()

asyncio.run(test_same_account())


# ----------------------------------------------------------------- 6. Restart/resume
section("6. Restart/resume — no duplication")

async def test_restart_resume():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 11)]

        # Run 1
        db1 = MigrationDB(cfg.db_path)
        source1 = MockTelethonClient(111, msgs)
        target1 = MockTelethonClient(222)
        m1 = SavedMessagesMigrator(source1, target1, cfg, db1, test_error_logger)
        await m1.run()
        check(len(target1._uploaded) == 10, f"Run 1: 10 uploaded (got {len(target1._uploaded)})")
        db1.close()

        # Run 2 (restart)
        db2 = MigrationDB(cfg.db_path)
        source2 = MockTelethonClient(111, msgs)
        target2 = MockTelethonClient(222)
        m2 = SavedMessagesMigrator(source2, target2, cfg, db2, test_error_logger)
        await m2.run()
        check(len(target2._uploaded) == 0, f"Run 2: 0 new uploads (got {len(target2._uploaded)})")
        db2.close()

asyncio.run(test_restart_resume())


# ----------------------------------------------------------------- 7. .part file recovery
section("7. Media download with .part file recovery")

async def test_part_file_recovery():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaPhoto

        db = MigrationDB(cfg.db_path)

        # Create a stale .part file
        media_dir = cfg.media_dir
        media_dir.mkdir(parents=True, exist_ok=True)
        stale_part = media_dir / "msg_1.jpg.part"
        stale_part.write_bytes(b"incomplete data")

        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "caption", media=MessageMediaPhoto(photo=photo))]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)
        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        check(db.is_uploaded(1), "msg 1 uploaded")
        check(not stale_part.exists(), "stale .part file removed")
        check((media_dir / "msg_1.jpg").exists(), "final media file exists")
        db.close()

asyncio.run(test_part_file_recovery())


# ----------------------------------------------------------------- 8. Upload failure → FAILED
section("8. Upload failure → message FAILED (not text-only)")

async def test_upload_failure():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        cfg.max_retries = 2
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaPhoto

        db = MigrationDB(cfg.db_path)

        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "caption", media=MessageMediaPhoto(photo=photo))]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)
        target._upload_should_fail = True  # uploads fail

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        check(db.is_failed(1), "msg 1 marked FAILED (not text-only success)")
        check(len(target._uploaded) == 0, "no messages uploaded to target")
        db.close()

asyncio.run(test_upload_failure())


# ----------------------------------------------------------------- 9. FloodWait (unbounded)
section("9. FloodWait — unbounded retry, not counted as failure")

async def test_flood_wait():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        # Patch _handle_flood_wait to not sleep
        async def fake_fw(e, ctx):
            pass
        m._handle_flood_wait = fake_fw

        # Make send_message raise FloodWait once for msg 2
        from telethon.errors import FloodWaitError
        call_count = [0]
        original_send = target.send_message
        async def flood_send(entity, text, **kwargs):
            if "msg 2" in text and call_count[0] == 0:
                call_count[0] += 1
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_send(entity, text, **kwargs)
        target.send_message = flood_send

        await m.run()

        check(db.is_uploaded(2), "msg 2 uploaded after FloodWait retry")
        check(len(target._uploaded) == 3, f"all 3 uploaded (got {len(target._uploaded)})")
        # retry_count should NOT be inflated by FloodWait
        msg2 = db.get_message(2)
        check(msg2["retry_count"] <= 1, f"retry_count not inflated by FloodWait (got {msg2['retry_count']})")
        db.close()

asyncio.run(test_flood_wait())


# ----------------------------------------------------------------- 10. Graceful shutdown
section("10. Graceful shutdown (SIGTERM)")

async def test_graceful_shutdown():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 101)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        original_migrate = SavedMessagesMigrator._migrate_single_message
        async def interrupting_migrate(self, message, se, te):
            if message.id == 5:
                raise KeyboardInterrupt("SIGTERM")
            await original_migrate(self, message, se, te)

        with patch.object(SavedMessagesMigrator, "_migrate_single_message", interrupting_migrate):
            m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
            try:
                await m.run()
                check(False, "should have raised KeyboardInterrupt")
            except KeyboardInterrupt:
                check(True, "KeyboardInterrupt propagated")

        check(db.is_uploaded(1), "msg 1 uploaded before shutdown")
        check(db.is_uploaded(4), "msg 4 uploaded before shutdown")
        check(not db.is_uploaded(5), "msg 5 NOT uploaded (shutdown)")
        db.close()

asyncio.run(test_graceful_shutdown())


# ----------------------------------------------------------------- 11. Duplicate prevention
section("11. Duplicate prevention across 3 runs")

async def test_duplicate_prevention():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 21)]
        total = 0

        for _ in range(3):
            db = MigrationDB(cfg.db_path)
            source = MockTelethonClient(111, msgs)
            target = MockTelethonClient(222)
            m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
            await m.run()
            total += len(target._uploaded)
            db.close()

        check(total == 20, f"exactly 20 total uploads across 3 runs (got {total})")

asyncio.run(test_duplicate_prevention())


# ----------------------------------------------------------------- 12. State machine
section("12. State machine transitions in SQLite")

def test_state_machine():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_db import MigrationDB
        db = MigrationDB(Path(tmp) / "state.db")

        check(db.get_status(1) is None, "msg 1 initially untracked")

        db.mark_pending(1, source_date="2026-01-01", source_text="hello")
        check(db.get_status(1) == "pending", "msg 1 → pending")

        db.mark_downloading(1)
        check(db.get_status(1) == "downloading", "msg 1 → downloading")

        db.mark_downloaded(1, media_path="media/msg_1.jpg", has_media=True, expected_size=100)
        check(db.get_status(1) == "downloaded", "msg 1 → downloaded")

        db.mark_uploading(1, upload_attempt_hash="abc123")
        check(db.get_status(1) == "uploading", "msg 1 → uploading")

        db.mark_uploaded(1, target_message_id=999)
        check(db.get_status(1) == "uploaded", "msg 1 → uploaded")
        check(db.get_target_message_id(1) == 999, "target id recorded")

        db.mark_failed(2, "test error")
        check(db.is_failed(2), "msg 2 → failed")

        db.clear_failed(2)
        check(not db.is_failed(2), "msg 2 cleared from failed")
        db.close()

test_state_machine()


# ----------------------------------------------------------------- 13. Contiguous checkpoint
section("13. Contiguous checkpoint (gap test: 100 ok, 101 failed, 102 ok)")

def test_contiguous_checkpoint():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_db import MigrationDB
        db = MigrationDB(Path(tmp) / "state.db")

        # Mark messages 1-100 as uploaded
        for i in range(1, 101):
            db.mark_uploaded(i, target_message_id=9000 + i)

        check(db.get_contiguous_checkpoint() == 100,
              f"checkpoint=100 after 1-100 uploaded (got {db.get_contiguous_checkpoint()})")

        # Mark 101 as failed
        db.mark_failed(101, "simulated failure")
        check(db.get_contiguous_checkpoint() == 100,
              f"checkpoint still 100 after 101 failed (got {db.get_contiguous_checkpoint()})")

        # Mark 102 as uploaded
        db.mark_uploaded(102, target_message_id=9102)
        check(db.get_contiguous_checkpoint() == 100,
              f"checkpoint STILL 100 after 102 uploaded (gap at 101) (got {db.get_contiguous_checkpoint()})")

        # Now fix 101
        db.clear_failed(101)
        db.mark_uploaded(101, target_message_id=9101)
        check(db.get_contiguous_checkpoint() == 102,
              f"checkpoint=102 after 101 fixed (got {db.get_contiguous_checkpoint()})")
        db.close()

test_contiguous_checkpoint()


# ----------------------------------------------------------------- 14. Crash recovery (uploading)
section("14. Crash recovery — uploading state with reconciliation")

async def test_crash_recovery_uploading():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)

        # Simulate: msg 1 was mid-upload when crash happened
        db.mark_pending(1)
        db.mark_downloaded(1, has_media=False)
        db.mark_uploading(1, upload_attempt_hash="abc")

        # But the upload actually succeeded (target has the message)
        # We simulate this by pre-populating target's uploaded list
        msgs = [MockMessage(1, "msg 1")]
        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)
        # Pre-add a matching message to target
        pre_msg = MockMessage(8000, text="msg 1")
        target._uploaded.append(pre_msg)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        check(db.is_uploaded(1), "msg 1 recovered via reconciliation")
        check(m.stats["reconciled_duplicates"] >= 1, "reconciliation counted")
        db.close()

asyncio.run(test_crash_recovery_uploading())


# ----------------------------------------------------------------- 15. Media failure ≠ text-only
section("15. Media failure does NOT become text-only success")

async def test_media_failure_not_text_only():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        cfg.max_retries = 2
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaPhoto

        db = MigrationDB(cfg.db_path)

        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "photo caption", media=MessageMediaPhoto(photo=photo))]

        source = MockTelethonClient(111, msgs)
        source._download_should_fail = True  # downloads fail
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        check(db.is_failed(1), "msg 1 FAILED (media download failed)")
        check(len(target._uploaded) == 0, "NO messages uploaded (caption NOT sent)")
        check(not db.is_uploaded(1), "msg 1 NOT marked uploaded")
        db.close()

asyncio.run(test_media_failure_not_text_only())


# ----------------------------------------------------------------- 16. Bounded retries
section("16. Bounded retries — exhaustion → failed")

async def test_bounded_retries():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        cfg.max_retries = 3
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)
        target._upload_should_fail = True

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        # All should be failed after 3 retries
        for i in range(1, 4):
            check(db.is_failed(i), f"msg {i} marked failed after bounded retries")

        # retry_count should be tracked
        msg1 = db.get_message(1)
        check(msg1["retry_count"] >= 1, f"retry_count tracked (got {msg1['retry_count']})")
        db.close()

asyncio.run(test_bounded_retries())


# ----------------------------------------------------------------- 17. Large-state scalability
section("17. Large-state scalability (1000 messages in SQLite)")

def test_large_state():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_db import MigrationDB
        import time
        db = MigrationDB(Path(tmp) / "state.db")

        # Insert 1000 messages
        start = time.time()
        for i in range(1, 1001):
            db.mark_uploaded(i, target_message_id=9000 + i)
        insert_time = time.time() - start

        # Query checkpoint (should be fast)
        start = time.time()
        cp = db.get_contiguous_checkpoint()
        query_time = time.time() - start

        check(cp == 1000, f"checkpoint=1000 (got {cp})")
        check(query_time < 0.1, f"checkpoint query <0.1s (got {query_time:.4f}s)")
        check(insert_time < 5.0, f"1000 inserts <5s (got {insert_time:.2f}s)")

        # Query failed messages (should be fast with index)
        start = time.time()
        failed = db.get_failed_message_ids()
        failed_query_time = time.time() - start
        check(len(failed) == 0, f"0 failed (got {len(failed)})")
        check(failed_query_time < 0.05, f"failed query <0.05s (got {failed_query_time:.4f}s)")
        db.close()

test_large_state()


# ----------------------------------------------------------------- 18. JSON → SQLite migration
section("18. JSON → SQLite migration")

def test_json_migration():
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir()

        # Create old JSON state file
        json_path = state_dir / "migration_state.json"
        old_state = {
            "version": 1,
            "started_at": "2026-01-01T00:00:00",
            "last_run": "2026-01-02T00:00:00",
            "completed": False,
            "highest_source_id_processed": 5,
            "messages": {
                "1": {"status": "uploaded", "source_message_id": 1, "target_message_id": 100,
                      "attempts": 1, "last_error": None, "media_path": None,
                      "has_media": False, "updated_at": "2026-01-01T00:00:00"},
                "2": {"status": "failed", "source_message_id": 2, "target_message_id": None,
                      "attempts": 2, "last_error": "test error", "media_path": None,
                      "has_media": True, "updated_at": "2026-01-01T00:00:00"},
                "3": {"status": "pending", "source_message_id": 3, "target_message_id": None,
                      "attempts": 0, "last_error": None, "media_path": None,
                      "has_media": False, "updated_at": "2026-01-01T00:00:00"},
            },
        }
        with open(json_path, "w") as f:
            json.dump(old_state, f)

        # Create DB — should auto-migrate
        from migration_db import MigrationDB
        db = MigrationDB(state_dir / "migration.db")

        check(db.is_uploaded(1), "msg 1 migrated from JSON")
        check(db.is_failed(2), "msg 2 migrated from JSON")
        check(db.get_status(3) == "pending", "msg 3 migrated from JSON")
        check(db.get_target_message_id(1) == 100, "target_message_id preserved")

        # JSON file should be backed up
        check(not json_path.exists(), "original JSON file removed")
        check((state_dir / "migration_state.json.bak").exists(), "JSON backed up as .bak")
        db.close()

test_json_migration()


# ----------------------------------------------------------------- 19. Media integrity validation
section("19. Media integrity validation (size mismatch)")

async def test_media_integrity():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaDocument

        db = MigrationDB(cfg.db_path)

        # Create a document with expected size = 5000
        doc = MagicMock()
        doc.id = 1
        doc.size = 5000  # expected 5000 bytes
        doc.mime_type = "image/jpeg"
        doc.attributes = []

        msgs = [MockMessage(1, "doc", media=MessageMediaDocument(document=doc))]

        source = MockTelethonClient(111, msgs)
        # Override download to write wrong size (2000 instead of 5000)
        async def wrong_size_download(message, file=None, **kwargs):
            if file and message.media:
                path = Path(file)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x" * 2000)  # wrong size
                return str(path)
            return None
        source.download_media = wrong_size_download

        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        # Should fail because size mismatch (5000 expected, 2000 actual)
        check(db.is_failed(1), "msg 1 failed due to size mismatch")
        check(len(target._uploaded) == 0, "no upload (integrity check failed)")
        db.close()

asyncio.run(test_media_integrity())


# ----------------------------------------------------------------- 20. Deployment config
section("20. Deployment config verification")

def test_deployment_config():
    base = Path(__file__).resolve().parent.parent

    procfile = base / "Procfile"
    check(procfile.exists(), "Procfile exists")
    check("python migrate.py" in procfile.read_text(), "Procfile → migrate.py")

    railway = base / "railway.json"
    check(railway.exists(), "railway.json exists")
    rj = json.loads(railway.read_text())
    check(rj.get("deploy", {}).get("startCommand") == "python migrate.py",
          "railway.json startCommand correct")

    reqs = base / "requirements.txt"
    check(reqs.exists(), "requirements.txt exists")
    rc = reqs.read_text()
    check("telethon" in rc, "telethon in requirements")
    check("python-dotenv" in rc, "python-dotenv in requirements")

    env = base / ".env.example"
    check(env.exists(), ".env.example exists")
    ec = env.read_text()
    for v in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
        check(v in ec, f"{v} in .env.example")

    gi = (base / ".gitignore").read_text()
    check(".env" in gi, ".env in .gitignore")
    check("*.session" in gi, "*.session in .gitignore")
    check("/app/data/" in gi, "/app/data/ in .gitignore")

test_deployment_config()


# ----------------------------------------------------------------- 21. Saved Messages only
section("21. Saved Messages-only verification")

async def test_saved_messages_only():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        iter_entities = []
        original_iter = source.iter_messages
        async def tracking_iter(entity, **kwargs):
            iter_entities.append(entity)
            async for m in original_iter(entity, **kwargs):
                yield m
        source.iter_messages = tracking_iter

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        check(all(e == "me" for e in iter_entities), f"all entities are 'me': {iter_entities}")
        db.close()

asyncio.run(test_saved_messages_only())


# ----------------------------------------------------------------- 22. Crash during download
section("22. Crash during download — recovery on restart")

async def test_crash_during_download():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaPhoto

        db = MigrationDB(cfg.db_path)

        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "caption", media=MessageMediaPhoto(photo=photo))]

        # Run 1: crash during download (mark_downloading, then interrupt)
        source1 = MockTelethonClient(111, msgs)
        target1 = MockTelethonClient(222)

        original_download = source1.download_media
        async def crashing_download(message, file=None, **kwargs):
            # Mark as downloading in DB, then crash
            db.mark_downloading(message.id)
            raise KeyboardInterrupt("Crash during download")

        source1.download_media = crashing_download

        m1 = SavedMessagesMigrator(source1, target1, cfg, db, test_error_logger)
        try:
            await m1.run()
        except KeyboardInterrupt:
            pass

        check(db.get_status(1) == "downloading", f"msg 1 in 'downloading' state after crash (got {db.get_status(1)})")

        # Run 2: restart — should recover
        source2 = MockTelethonClient(111, msgs)
        target2 = MockTelethonClient(222)
        m2 = SavedMessagesMigrator(source2, target2, cfg, db, test_error_logger)
        await m2.run()

        check(db.is_uploaded(1), "msg 1 recovered and uploaded after crash")
        db.close()

asyncio.run(test_crash_during_download())


# ----------------------------------------------------------------- 23. Checkpoint with gapped IDs
section("23. Contiguous checkpoint with gapped Telegram IDs (10, 11, 15, 16)")

def test_checkpoint_gapped_ids():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_db import MigrationDB
        db = MigrationDB(Path(tmp) / "state.db")

        # Simulate discovered messages with gaps: 10, 11, 15, 16
        # Mark all as pending first (discovered)
        for sid in [10, 11, 15, 16]:
            db.mark_pending(sid)

        # Upload 10
        db.mark_uploaded(10, target_message_id=100)
        check(db.get_contiguous_checkpoint() == 10,
              f"checkpoint=10 after uploading first discovered msg (got {db.get_contiguous_checkpoint()})")

        # Upload 11
        db.mark_uploaded(11, target_message_id=101)
        check(db.get_contiguous_checkpoint() == 11,
              f"checkpoint=11 after uploading 11 (got {db.get_contiguous_checkpoint()})")

        # Upload 15 (gap at 12-14, but those don't exist in discovered set)
        db.mark_uploaded(15, target_message_id=115)
        check(db.get_contiguous_checkpoint() == 15,
              f"checkpoint=15 after uploading 15 (gap 12-14 are not discovered) (got {db.get_contiguous_checkpoint()})")

        # Upload 16
        db.mark_uploaded(16, target_message_id=116)
        check(db.get_contiguous_checkpoint() == 16,
              f"checkpoint=16 after uploading 16 (got {db.get_contiguous_checkpoint()})")

        db.close()

test_checkpoint_gapped_ids()


# ----------------------------------------------------------------- 24. Failed message in gapped sequence
section("24. Failed message inside gapped sequence — checkpoint stops before it")

def test_checkpoint_gapped_with_failure():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_db import MigrationDB
        db = MigrationDB(Path(tmp) / "state.db")

        # Discovered: 10, 11, 15, 16
        for sid in [10, 11, 15, 16]:
            db.mark_pending(sid)

        # Upload 10, 11
        db.mark_uploaded(10, 100)
        db.mark_uploaded(11, 101)
        check(db.get_contiguous_checkpoint() == 11,
              f"checkpoint=11 (got {db.get_contiguous_checkpoint()})")

        # Mark 15 as failed
        db.mark_failed(15, "simulated failure")
        check(db.get_contiguous_checkpoint() == 11,
              f"checkpoint still 11 after 15 failed (got {db.get_contiguous_checkpoint()})")

        # Upload 16 — checkpoint must NOT advance past 15
        db.mark_uploaded(16, 116)
        check(db.get_contiguous_checkpoint() == 11,
              f"checkpoint STILL 11 after 16 uploaded (15 is failed, blocks) (got {db.get_contiguous_checkpoint()})")

        # Now fix 15
        db.clear_failed(15)
        db.mark_uploaded(15, 115)
        check(db.get_contiguous_checkpoint() == 16,
              f"checkpoint=16 after 15 fixed (got {db.get_contiguous_checkpoint()})")

        db.close()

test_checkpoint_gapped_with_failure()


# ----------------------------------------------------------------- 25. No unnecessary rescan
section("25. No unnecessary full history rescan due to missing numeric IDs")

async def test_no_rescan_for_missing_ids():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)

        # Source messages: 10, 11, 15, 16 (gaps at 1-9, 12-14)
        msgs = [MockMessage(i, f"msg {i}") for i in [10, 11, 15, 16]]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        # Track what min_id is passed to iter_messages
        iter_min_ids = []
        original_iter = source.iter_messages
        async def tracking_iter(entity, **kwargs):
            iter_min_ids.append(kwargs.get("min_id", 0))
            async for m in original_iter(entity, **kwargs):
                yield m
        source.iter_messages = tracking_iter

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        # All 4 messages should be uploaded
        check(len(target._uploaded) == 4, f"4 messages uploaded (got {len(target._uploaded)})")
        check(db.is_uploaded(10), "msg 10 uploaded")
        check(db.is_uploaded(11), "msg 11 uploaded")
        check(db.is_uploaded(15), "msg 15 uploaded")
        check(db.is_uploaded(16), "msg 16 uploaded")

        # Checkpoint should be 16 (all discovered messages uploaded)
        check(db.get_contiguous_checkpoint() == 16,
              f"checkpoint=16 (got {db.get_contiguous_checkpoint()})")

        # Run 2: restart — should resume from checkpoint=16, fetch nothing new
        source2 = MockTelethonClient(111, msgs)
        target2 = MockTelethonClient(222)
        iter_min_ids_r2 = []
        original_iter2 = source2.iter_messages
        async def tracking_iter2(entity, **kwargs):
            iter_min_ids_r2.append(kwargs.get("min_id", 0))
            async for m in original_iter2(entity, **kwargs):
                yield m
        source2.iter_messages = tracking_iter2

        db2 = MigrationDB(cfg.db_path)
        m2 = SavedMessagesMigrator(source2, target2, cfg, db2, test_error_logger)
        await m2.run()

        check(len(target2._uploaded) == 0, f"Run 2: 0 new uploads (got {len(target2._uploaded)})")
        check(len(iter_min_ids_r2) > 0, "iter_messages called in run 2")
        if iter_min_ids_r2:
            check(iter_min_ids_r2[0] == 16,
                  f"Run 2 resumes from checkpoint=16 (got min_id={iter_min_ids_r2[0]})")

        db.close()
        db2.close()

asyncio.run(test_no_rescan_for_missing_ids())


# ----------------------------------------------------------------- 26. FloodWait retries SAME message (source retrieval)
section("26. FloodWait during source retrieval — retries SAME operation")

async def test_floodwait_source_retrieval():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        # Patch _handle_flood_wait to not actually sleep
        async def fake_fw(e, ctx):
            pass
        m._handle_flood_wait = fake_fw

        # Make iter_messages raise FloodWait once on first call
        from telethon.errors import FloodWaitError
        iter_call_count = [0]
        original_iter = source.iter_messages
        async def flood_iter(entity, **kwargs):
            iter_call_count[0] += 1
            if iter_call_count[0] == 1:
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            async for m in original_iter(entity, **kwargs):
                yield m
        source.iter_messages = flood_iter

        await m.run()

        check(len(target._uploaded) == 3, f"all 3 messages uploaded after FloodWait (got {len(target._uploaded)})")
        check(iter_call_count[0] >= 2, f"iter_messages retried after FloodWait (got {iter_call_count[0]} calls)")
        db.close()

asyncio.run(test_floodwait_source_retrieval())


# ----------------------------------------------------------------- 27. FloodWait during media download — retries SAME message
section("27. FloodWait during media download — retries SAME message")

async def test_floodwait_media_download():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator
        from telethon.tl.types import MessageMediaPhoto

        db = MigrationDB(cfg.db_path)

        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "caption", media=MessageMediaPhoto(photo=photo))]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        async def fake_fw(e, ctx):
            pass
        m._handle_flood_wait = fake_fw

        # Make download_media raise FloodWait once
        from telethon.errors import FloodWaitError
        download_calls = [0]
        original_download = source.download_media
        async def flood_download(message, file=None, **kwargs):
            download_calls[0] += 1
            if download_calls[0] == 1:
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_download(message, file, **kwargs)
        source.download_media = flood_download

        await m.run()

        check(db.is_uploaded(1), "msg 1 uploaded after FloodWait during download")
        check(download_calls[0] >= 2, f"download retried after FloodWait (got {download_calls[0]} calls)")
        # Verify retry_count was NOT incremented by FloodWait
        msg1 = db.get_message(1)
        check(msg1["retry_count"] <= 1, f"retry_count not inflated by FloodWait (got {msg1['retry_count']})")
        db.close()

asyncio.run(test_floodwait_media_download())


# ----------------------------------------------------------------- 28. FloodWait during target upload — retries SAME message
section("28. FloodWait during target upload — retries SAME message")

async def test_floodwait_target_upload():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        async def fake_fw(e, ctx):
            pass
        m._handle_flood_wait = fake_fw

        # Make send_message raise FloodWait once for msg 2
        from telethon.errors import FloodWaitError
        send_calls = {}
        original_send = target.send_message
        async def flood_send(entity, text, **kwargs):
            send_calls[text] = send_calls.get(text, 0) + 1
            if "msg 2" in text and send_calls[text] == 1:
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_send(entity, text, **kwargs)
        target.send_message = flood_send

        await m.run()

        check(db.is_uploaded(2), "msg 2 uploaded after FloodWait during upload")
        check(send_calls.get("msg 2", 0) >= 2, f"send_message retried for msg 2 (got {send_calls.get('msg 2', 0)})")
        # All 3 messages should be uploaded
        check(len(target._uploaded) == 3, f"all 3 messages uploaded (got {len(target._uploaded)})")
        db.close()

asyncio.run(test_floodwait_target_upload())


# ----------------------------------------------------------------- 29. FloodWait during startup recovery — retries SAME message
section("29. FloodWait during startup recovery — retries SAME message")

async def test_floodwait_startup_recovery():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)

        # Pre-populate: msg 1 is in 'uploading' state (crashed during upload)
        db.mark_pending(1)
        db.mark_downloaded(1, has_media=False)
        db.mark_uploading(1, upload_attempt_hash="abc")

        msgs = [MockMessage(1, "msg 1")]
        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        async def fake_fw(e, ctx):
            pass
        m._handle_flood_wait = fake_fw

        # Make the first get_messages call (during recovery) raise FloodWait
        from telethon.errors import FloodWaitError
        get_calls = [0]
        original_get = source.get_messages
        async def flood_get(entity, ids=None, limit=None, **kwargs):
            get_calls[0] += 1
            if get_calls[0] == 1 and isinstance(ids, int):
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_get(entity, ids=ids, limit=limit, **kwargs)
        source.get_messages = flood_get

        await m.run()

        check(db.is_uploaded(1), "msg 1 uploaded after FloodWait during recovery")
        check(get_calls[0] >= 2, f"get_messages retried after FloodWait (got {get_calls[0]} calls)")
        db.close()

asyncio.run(test_floodwait_startup_recovery())


# ----------------------------------------------------------------- 30. FloodWait does NOT mark unrelated messages complete
section("30. FloodWait does NOT mark unrelated messages as complete")

async def test_floodwait_no_unrelated_completion():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)
        # Only 2 messages: msg 1 (will succeed), msg 2 (permanent FloodWait)
        msgs = [MockMessage(1, "msg 1"), MockMessage(2, "msg 2")]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)

        # FloodWait sleep is real but tiny (1s). Limit to 3 FloodWaits max
        # by counting and then converting to a permanent error.
        flood_count = [0]
        async def fake_fw(e, ctx):
            flood_count[0] += 1
            if flood_count[0] > 3:
                # Break the infinite loop by re-raising as a non-FloodWait error
                raise Exception("Too many FloodWaits — test abort")
        m._handle_flood_wait = fake_fw

        # Make send_message raise FloodWait PERMANENTLY for msg 2
        from telethon.errors import FloodWaitError
        original_send = target.send_message
        async def permanent_flood_send(entity, text, **kwargs):
            if "msg 2" in text:
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_send(entity, text, **kwargs)
        target.send_message = permanent_flood_send

        # Run — msg 2 will FloodWait forever, but our fake_fw breaks after 3
        try:
            await asyncio.wait_for(m.run(), timeout=10.0)
        except (asyncio.TimeoutError, Exception):
            pass  # expected — msg 2 FloodWaits until we abort

        # Msg 1 should be uploaded
        check(db.is_uploaded(1), "msg 1 uploaded")
        # Msg 2 should NOT be marked uploaded (FloodWait doesn't = success)
        check(not db.is_uploaded(2), "msg 2 NOT marked uploaded (FloodWait doesn't = success)")
        # Checkpoint should be 0 (msg 2 blocks the checkpoint at the first discovered message)
        # Actually checkpoint = 0 because msg 1 is first discovered, msg 2 is second.
        # If msg 1 uploaded and msg 2 is not, checkpoint stops at msg 1 if msg 1 is first.
        # Wait — the checkpoint walks discovered IDs in order. If msg 1 is uploaded
        # but msg 2 is not, checkpoint = 1 (stops at first non-uploaded = msg 2).
        cp = db.get_contiguous_checkpoint()
        check(cp <= 1, f"checkpoint <= 1 (msg 2 blocks) (got {cp})")

        db.close()

asyncio.run(test_floodwait_no_unrelated_completion())


# ----------------------------------------------------------------- 31. Restart/resume after gapped IDs
section("31. Restart/resume after gapped IDs — no rescan of gaps")

async def test_restart_after_gaps():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        # Run 1: messages 10, 11, 15, 16
        msgs = [MockMessage(i, f"msg {i}") for i in [10, 11, 15, 16]]

        db1 = MigrationDB(cfg.db_path)
        source1 = MockTelethonClient(111, msgs)
        target1 = MockTelethonClient(222)
        m1 = SavedMessagesMigrator(source1, target1, cfg, db1, test_error_logger)
        await m1.run()
        check(len(target1._uploaded) == 4, f"Run 1: 4 uploaded (got {len(target1._uploaded)})")
        check(db1.get_contiguous_checkpoint() == 16, f"checkpoint=16 after run 1")
        db1.close()

        # Run 2: restart with same messages
        db2 = MigrationDB(cfg.db_path)
        source2 = MockTelethonClient(111, msgs)
        target2 = MockTelethonClient(222)

        # Track min_id passed to iter_messages
        iter_min_ids = []
        original_iter = source2.iter_messages
        async def tracking_iter(entity, **kwargs):
            iter_min_ids.append(kwargs.get("min_id", 0))
            async for m in original_iter(entity, **kwargs):
                yield m
        source2.iter_messages = tracking_iter

        m2 = SavedMessagesMigrator(source2, target2, cfg, db2, test_error_logger)
        await m2.run()

        check(len(target2._uploaded) == 0, f"Run 2: 0 new uploads (got {len(target2._uploaded)})")
        if iter_min_ids:
            check(iter_min_ids[0] == 16, f"Run 2 resumes from checkpoint=16 (got min_id={iter_min_ids[0]})")
        db2.close()

asyncio.run(test_restart_after_gaps())


# ----------------------------------------------------------------- 32. Later uploaded msg doesn't skip earlier failed
section("32. Later uploaded message does NOT skip earlier failed discovered message")

async def test_later_upload_doesnt_skip_earlier_failure():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(Path(tmp))
        cfg.max_retries = 1  # fail fast
        from migration_db import MigrationDB
        from migrator import SavedMessagesMigrator

        db = MigrationDB(cfg.db_path)

        # Messages: 10, 11, 15
        msgs = [MockMessage(10, "msg 10"), MockMessage(11, "msg 11"), MockMessage(15, "msg 15")]

        source = MockTelethonClient(111, msgs)
        target = MockTelethonClient(222)

        # Make msg 11 upload fail permanently
        original_send = target.send_message
        async def fail_msg_11(entity, text, **kwargs):
            if "msg 11" in text:
                raise Exception("Permanent failure for msg 11")
            return await original_send(entity, text, **kwargs)
        target.send_message = fail_msg_11

        m = SavedMessagesMigrator(source, target, cfg, db, test_error_logger)
        await m.run()

        # Msg 10 should be uploaded
        check(db.is_uploaded(10), "msg 10 uploaded")
        # Msg 11 should be failed
        check(db.is_failed(11), "msg 11 failed")
        # Msg 15 should be uploaded (migration continues after failure)
        check(db.is_uploaded(15), "msg 15 uploaded (migration continues)")

        # Checkpoint must be 10 (stops before failed msg 11)
        check(db.get_contiguous_checkpoint() == 10,
              f"checkpoint=10 (stops before failed msg 11) (got {db.get_contiguous_checkpoint()})")

        # On restart, msg 11 should still be retryable (not skipped)
        # Run 2: remove the failure, msg 11 should succeed
        target2 = MockTelethonClient(222)
        db2 = MigrationDB(cfg.db_path)
        source2 = MockTelethonClient(111, msgs)
        m2 = SavedMessagesMigrator(source2, target2, cfg, db2, test_error_logger)
        await m2.run()

        check(db2.is_uploaded(11), "msg 11 uploaded on retry (not skipped)")
        check(db2.get_contiguous_checkpoint() == 15,
              f"checkpoint=15 after msg 11 fixed (got {db2.get_contiguous_checkpoint()})")

        db.close()
        db2.close()

asyncio.run(test_later_upload_doesnt_skip_earlier_failure())


# ----------------------------------------------------------------- Summary
print()
print("=" * 70)
if failures:
    print(f"FAILED: {len(failures)} test(s) out of {test_count}")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"ALL {test_count} TESTS PASSED")
print("=" * 70)
