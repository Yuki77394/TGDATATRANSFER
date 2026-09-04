"""Comprehensive tests for the Saved Messages migration tool.

Tests cover:
  1.  DATA_DIR creation
  2.  Persistent state path
  3.  Missing environment variables
  4.  Invalid session configuration handling
  5.  Source/target same-account protection
  6.  Restart/resume simulation
  7.  Media download resume simulation
  8.  Target upload failure simulation
  9.  FloodWait simulation
  10. Graceful shutdown simulation
  11. Duplicate prevention
  12. Saved Messages-only verification
  13. Deployment command verification

These tests use MOCK Telethon clients — no real Telegram network access.
"""
import asyncio
import json
import os
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
    print(f"\n{'='*70}\nMigration Test: {name}\n{'='*70}")


# ----------------------------------------------------------------- Mock infrastructure

class MockMessage:
    """Mock Telethon Message."""
    def __init__(self, msg_id, text="", media=None):
        self.id = msg_id
        self.text = text
        self.message = text
        self.media = media
        self.date = MagicMock()
        self.date.isoformat = MagicMock(return_value=f"2026-01-{msg_id:02d}T00:00:00")
        self.sender = None
        self.reply_to = None
        self.forward = None
        self.action = None


class MockTelethonClient:
    """Mock Telethon client for migration tests."""
    def __init__(self, user_id, messages=None):
        self._user_id = user_id
        self._messages = messages or []
        self._connected = False
        self._uploaded = []  # list of messages uploaded to this client

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
        return peer  # "me" stays "me"

    async def iter_messages(self, entity, limit=None, reverse=False, min_id=0, **kwargs):
        """Yield messages with id > min_id, oldest first."""
        from telethon.errors import FloodWaitError
        msgs = [m for m in self._messages if m.id > min_id]
        for msg in msgs:
            yield msg

    async def get_messages(self, entity, ids=None, **kwargs):
        if isinstance(ids, int):
            for m in self._messages:
                if m.id == ids:
                    return m
            return None
        return []

    async def download_media(self, message, file=None, **kwargs):
        """Simulate media download — write a fake file."""
        if file and message.media:
            path = Path(file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"fake media content")
            return str(path)
        return None

    async def send_file(self, entity, file=None, caption=None, **kwargs):
        """Simulate upload — return a mock target message."""
        target_msg = MockMessage(msg_id=9000 + len(self._uploaded), text=caption or "")
        self._uploaded.append(target_msg)
        return target_msg

    async def send_message(self, entity, text, **kwargs):
        target_msg = MockMessage(msg_id=9000 + len(self._uploaded), text=text)
        self._uploaded.append(target_msg)
        return target_msg


def make_mock_config(data_dir: Path) -> Any:
    """Create a mock MigrationConfig."""
    cfg = MagicMock()
    cfg.api_id = 12345
    cfg.api_hash = "fakehash"
    cfg.source_session = "fake_source_session"
    cfg.target_session = "fake_target_session"
    cfg.data_dir = data_dir
    cfg.state_dir = data_dir / "state"
    cfg.media_dir = data_dir / "media"
    cfg.failed_dir = data_dir / "failed"
    cfg.logs_dir = data_dir / "logs"
    cfg.state_file = data_dir / "state" / "migration_state.json"
    cfg.failed_messages_file = data_dir / "failed" / "failed_messages.jsonl"
    cfg.log_file = data_dir / "logs" / "migration.log"
    cfg.max_retries = 2
    cfg.checkpoint_every = 999
    cfg.allow_same_account = False
    cfg.delete_after_upload = False
    cfg.validate = MagicMock()
    cfg.validate_api_id = MagicMock()
    cfg.ensure_directories = MagicMock()
    cfg.safe_summary = MagicMock(return_value={"test": True})
    return cfg


# ----------------------------------------------------------------- 1. DATA_DIR creation
section("1. DATA_DIR creation")

def test_data_dir_creation():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_config import MigrationConfig
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
        check((Path(tmp) / "failed").exists(), "failed/ created")
        check((Path(tmp) / "logs").exists(), "logs/ created")

        for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
            os.environ.pop(k, None)

test_data_dir_creation()


# ----------------------------------------------------------------- 2. Persistent state path
section("2. Persistent state path under DATA_DIR")

def test_state_path():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_config import MigrationConfig
        os.environ["DATA_DIR"] = tmp
        cfg = MigrationConfig(env_path="/nonexistent/.env")
        check(str(cfg.state_file).startswith(tmp), f"state_file under DATA_DIR: {cfg.state_file}")
        check(str(cfg.media_dir).startswith(tmp), f"media_dir under DATA_DIR: {cfg.media_dir}")
        check(str(cfg.log_file).startswith(tmp), f"log_file under DATA_DIR: {cfg.log_file}")
        os.environ.pop("DATA_DIR", None)

test_state_path()


# ----------------------------------------------------------------- 3. Missing env vars
section("3. Missing environment variables")

def test_missing_env_vars():
    from migration_config import MigrationConfig
    # Clear all
    for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
        os.environ.pop(k, None)

    cfg = MigrationConfig(env_path="/nonexistent/.env")
    try:
        cfg.validate()
        check(False, "should have raised ValueError")
    except ValueError as e:
        check("API_ID" in str(e), "error mentions API_ID")
        check("API_HASH" in str(e), "error mentions API_HASH")
        check("SOURCE_SESSION" in str(e), "error mentions SOURCE_SESSION")
        check("TARGET_SESSION" in str(e), "error mentions TARGET_SESSION")

test_missing_env_vars()


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
        check("positive integer" in str(e), f"error mentions positive integer: {e}")

    for k in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION"]:
        os.environ.pop(k, None)

test_invalid_api_id()


# ----------------------------------------------------------------- 5. Same-account protection
section("5. Source/target same-account protection")

async def test_same_account_protection():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = False

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        # Both clients have the same user_id
        source = MockTelethonClient(user_id=111)
        target = MockTelethonClient(user_id=111)

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)

        try:
            await migrator.run()
            check(False, "should have raised RuntimeError")
        except RuntimeError as e:
            check("same account" in str(e).lower(), f"error mentions same account: {e}")
        except Exception as e:
            check(False, f"unexpected exception: {e}")

asyncio.run(test_same_account_protection())


# ----------------------------------------------------------------- 6. Restart/resume
section("6. Restart/resume — no duplication of uploaded messages")

async def test_restart_resume():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True  # allow for testing

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        # Run 1: messages 1-10
        msgs_r1 = [MockMessage(i, f"msg {i}") for i in range(1, 11)]
        source = MockTelethonClient(user_id=111, messages=msgs_r1)
        target = MockTelethonClient(user_id=222)

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
        await migrator.run()

        uploaded_r1 = len(target._uploaded)
        check(uploaded_r1 == 10, f"Run 1: 10 messages uploaded (got {uploaded_r1})")

        # Run 2: same messages (simulate restart)
        source2 = MockTelethonClient(user_id=111, messages=msgs_r1)
        target2 = MockTelethonClient(user_id=222)
        state2 = MigrationStateManager(cfg.state_file)  # reload state
        migrator2 = SavedMessagesMigrator(source2, target2, cfg, state2, error_logger)
        await migrator2.run()

        uploaded_r2 = len(target2._uploaded)
        check(uploaded_r2 == 0, f"Run 2: 0 new uploads (all skipped, got {uploaded_r2})")

asyncio.run(test_restart_resume())


# ----------------------------------------------------------------- 7. Media download resume
section("7. Media download resume")

async def test_media_download_resume():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        from telethon.tl.types import MessageMediaPhoto
        photo = MagicMock()
        photo.id = 1
        photo.sizes = []
        photo.size = 100
        msgs = [MockMessage(1, "photo caption", media=MessageMediaPhoto(photo=photo))]

        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
        await migrator.run()

        # Check media file was downloaded
        media_files = list(cfg.media_dir.iterdir()) if cfg.media_dir.exists() else []
        check(len(media_files) == 1, f"media file downloaded (got {len(media_files)})")

        # Check state shows uploaded
        check(state.is_uploaded(1), "message 1 marked uploaded")

asyncio.run(test_media_download_resume())


# ----------------------------------------------------------------- 8. Target upload failure
section("8. Target upload failure — message marked failed, not uploaded")

async def test_upload_failure():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True
        cfg.max_retries = 1  # fail fast

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        msgs = [MockMessage(1, "msg 1"), MockMessage(2, "msg 2")]
        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        # Make send_message fail for msg 1
        original_send = target.send_message
        call_count = [0]
        async def failing_send(entity, text, **kwargs):
            call_count[0] += 1
            if "msg 1" in text:
                raise Exception("Simulated upload failure")
            return await original_send(entity, text, **kwargs)
        target.send_message = failing_send

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
        await migrator.run()

        check(state.is_failed(1), "message 1 marked failed")
        check(state.is_uploaded(2), "message 2 still uploaded (continues after failure)")

asyncio.run(test_upload_failure())


# ----------------------------------------------------------------- 9. FloodWait simulation
section("9. FloodWait handling")

async def test_flood_wait():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 6)]
        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)

        # Patch _handle_flood_wait to not actually sleep
        async def fake_flood_wait(e, context):
            pass
        migrator._handle_flood_wait = fake_flood_wait

        # Make send_message raise FloodWait once for msg 3
        from telethon.errors import FloodWaitError
        send_count = [0]
        original_send = target.send_message
        async def flood_send(entity, text, **kwargs):
            if "msg 3" in text and send_count[0] == 0:
                send_count[0] += 1
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1
                raise e
            return await original_send(entity, text, **kwargs)
        target.send_message = flood_send

        await migrator.run()

        check(state.is_uploaded(3), "message 3 uploaded after FloodWait retry")
        check(len(target._uploaded) == 5, f"all 5 messages uploaded (got {len(target._uploaded)})")

asyncio.run(test_flood_wait())


# ----------------------------------------------------------------- 10. Graceful shutdown
section("10. Graceful shutdown on SIGTERM")

async def test_graceful_shutdown():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        # Simulate SIGTERM by raising KeyboardInterrupt mid-migration
        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 101)]
        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        from migrator import SavedMessagesMigrator

        # Patch _migrate_single_message to raise KeyboardInterrupt on msg 5
        original_migrate = SavedMessagesMigrator._migrate_single_message
        async def interrupting_migrate(self, message, source_entity, target_entity):
            if message.id == 5:
                raise KeyboardInterrupt("Simulated SIGTERM")
            await original_migrate(self, message, source_entity, target_entity)

        with patch.object(SavedMessagesMigrator, "_migrate_single_message", interrupting_migrate):
            migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
            try:
                await migrator.run()
                check(False, "should have raised KeyboardInterrupt")
            except KeyboardInterrupt:
                check(True, "KeyboardInterrupt propagated (graceful shutdown)")

        # State should be saved (messages 1-4 uploaded)
        check(state.is_uploaded(1), "msg 1 uploaded before shutdown")
        check(state.is_uploaded(4), "msg 4 uploaded before shutdown")
        check(not state.is_uploaded(5), "msg 5 NOT uploaded (shutdown happened)")

asyncio.run(test_graceful_shutdown())


# ----------------------------------------------------------------- 11. Duplicate prevention
section("11. Duplicate prevention across restarts")

async def test_duplicate_prevention():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 21)]

        # Run 3 times
        total_uploads = 0
        for run in range(3):
            source = MockTelethonClient(user_id=111, messages=msgs)
            target = MockTelethonClient(user_id=222)
            state_r = MigrationStateManager(cfg.state_file)
            from migrator import SavedMessagesMigrator
            migrator = SavedMessagesMigrator(source, target, cfg, state_r, error_logger)
            await migrator.run()
            total_uploads += len(target._uploaded)

        check(total_uploads == 20, f"exactly 20 total uploads across 3 runs (got {total_uploads})")

asyncio.run(test_duplicate_prevention())


# ----------------------------------------------------------------- 12. State machine transitions
section("12. State machine transitions")

def test_state_machine():
    with tempfile.TemporaryDirectory() as tmp:
        from migration_state import MigrationStateManager
        state = MigrationStateManager(Path(tmp) / "state.json")

        # Initially no status
        check(state.get_status(1) is None, "msg 1 initially untracked")

        # pending
        state.mark_pending(1)
        check(state.get_status(1) == "pending", "msg 1 → pending")

        # downloaded
        state.mark_downloaded(1, has_media=True, media_path="media/msg_1.jpg")
        check(state.get_status(1) == "downloaded", "msg 1 → downloaded")

        # uploading
        state.mark_uploading(1)
        check(state.get_status(1) == "uploading", "msg 1 → uploading")

        # uploaded
        state.mark_uploaded(1, target_message_id=999)
        check(state.get_status(1) == "uploaded", "msg 1 → uploaded")
        check(state.get_target_message_id(1) == 999, "target message id recorded")
        check(state.is_uploaded(1), "is_uploaded returns True")

        # failed
        state.mark_failed(2, "test error")
        check(state.is_failed(2), "msg 2 → failed")
        check(state.get_status(2) == "failed", "msg 2 status is failed")

        # clear failed
        state.clear_failed(2)
        check(not state.is_failed(2), "msg 2 cleared from failed")

test_state_machine()


# ----------------------------------------------------------------- 13. Crash recovery (uploading state)
section("13. Crash recovery — message in 'uploading' state")

async def test_crash_recovery_uploading():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        # Pre-populate state: msg 1 is mid-upload (crashed)
        state.mark_pending(1)
        state.mark_downloaded(1, has_media=False)
        state.mark_uploading(1)
        state.save()

        # Run migrator — should recover msg 1
        msgs = [MockMessage(1, "msg 1")]
        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
        await migrator.run()

        check(state.is_uploaded(1), "msg 1 recovered from 'uploading' and marked uploaded")
        check(migrator.stats["retried_crash"] == 1, f"retried_crash stat = 1 (got {migrator.stats['retried_crash']})")

asyncio.run(test_crash_recovery_uploading())


# ----------------------------------------------------------------- 14. Deployment config verification
section("14. Deployment config verification")

def test_deployment_config():
    # Procfile
    procfile = Path(__file__).resolve().parent.parent / "Procfile"
    check(procfile.exists(), "Procfile exists")
    content = procfile.read_text().strip()
    check("python migrate.py" in content, f"Procfile points to migrate.py (got: {content})")

    # railway.json
    railway = Path(__file__).resolve().parent.parent / "railway.json"
    check(railway.exists(), "railway.json exists")
    rj = json.loads(railway.read_text())
    check(rj.get("deploy", {}).get("startCommand") == "python migrate.py",
          f"railway.json startCommand = python migrate.py (got: {rj.get('deploy', {}).get('startCommand')})")

    # requirements.txt
    reqs = Path(__file__).resolve().parent.parent / "requirements.txt"
    check(reqs.exists(), "requirements.txt exists")
    req_content = reqs.read_text()
    check("telethon" in req_content, "telethon in requirements.txt")
    check("python-dotenv" in req_content, "python-dotenv in requirements.txt")

    # .env.example
    env_example = Path(__file__).resolve().parent.parent / ".env.example"
    check(env_example.exists(), ".env.example exists")
    env_content = env_example.read_text()
    for var in ["API_ID", "API_HASH", "SOURCE_SESSION", "TARGET_SESSION", "DATA_DIR"]:
        check(var in env_content, f"{var} in .env.example")

    # .gitignore
    gitignore = Path(__file__).resolve().parent.parent / ".gitignore"
    check(gitignore.exists(), ".gitignore exists")
    gi = gitignore.read_text()
    check(".env" in gi, ".env in .gitignore")
    check("*.session" in gi, "*.session in .gitignore")

test_deployment_config()


# ----------------------------------------------------------------- 15. Saved Messages only
section("15. Saved Messages-only verification (get_input_entity('me'))")

async def test_saved_messages_only():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_mock_config(Path(tmp))
        cfg.allow_same_account = True

        from migration_state import MigrationStateManager
        state = MigrationStateManager(cfg.state_file)

        import logging
        error_logger = logging.getLogger("test_errors")
        error_logger.addHandler(logging.NullHandler())

        msgs = [MockMessage(i, f"msg {i}") for i in range(1, 4)]
        source = MockTelethonClient(user_id=111, messages=msgs)
        target = MockTelethonClient(user_id=222)

        # Track what entity is passed to iter_messages
        iter_entities = []
        original_iter = source.iter_messages
        async def tracking_iter(entity, **kwargs):
            iter_entities.append(entity)
            async for m in original_iter(entity, **kwargs):
                yield m
        source.iter_messages = tracking_iter

        from migrator import SavedMessagesMigrator
        migrator = SavedMessagesMigrator(source, target, cfg, state, error_logger)
        await migrator.run()

        # Verify 'me' was used (Saved Messages)
        check(len(iter_entities) > 0, "iter_messages was called")
        check(all(e == "me" for e in iter_entities), f"all entities are 'me' (Saved Messages): {iter_entities}")

asyncio.run(test_saved_messages_only())


# ----------------------------------------------------------------- Summary
print()
print("=" * 70)
if failures:
    print(f"FAILED: {len(failures)} test(s) out of {test_count}")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"ALL {test_count} MIGRATION TESTS PASSED")
print("=" * 70)
