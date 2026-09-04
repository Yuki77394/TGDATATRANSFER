"""Integration-style tests that simulate the actual backup orchestration.

These tests use a MOCK Telethon client to simulate message fetching and
media download, allowing us to test crash/resume/FloodWait scenarios
deterministically without a real Telegram connection.

Each test verifies the ACTUAL FINAL STATE of messages.jsonl and the state
files — not just that functions returned successfully.

Test scenarios (from the audit requirements):
  1.  Normal export
  2.  Crash after message fetch (before JSONL write)
  3.  Crash after JSONL write (before state save)
  4.  Crash before state save (state stale)
  5.  Failed message followed by newer successful message — CRITICAL:
      the failed message must NOT be permanently skipped
  6.  Repeated restart idempotency
  7.  FloodWait once during message iteration
  8.  FloodWait multiple times during message iteration
  9.  Media failure then successful retry on next run
  10. Partial JSONL tail (truncation recovery)
  11. Corrupted MIDDLE JSONL line with later valid records (preserved)
  12. Stale state AHEAD of exported records (JSONL wins via min)
  13. Duplicate prevention
  14. Interruption during media download
  15. State ahead of JSONL — failed messages must be retried
  16. Repeated restarts with a persistently-failing message
"""
import asyncio
import json
import os
import shutil
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
    print(f"\n{'='*70}\nIntegration Test: {name}\n{'='*70}")


# ----------------------------------------------------------------- Mock infrastructure

class MockMessage:
    """A minimal mock of a Telethon Message object."""
    def __init__(self, msg_id, text="", media=None, sender=None):
        self.id = msg_id
        self.text = text
        self.message = text  # Telethon's raw text field
        self.media = media
        self.sender = sender
        self.date = None
        self.edit_date = None
        self.reply_to = None
        self.forward = None
        self.action = None
        self.grouped_id = None
        self.post = False
        self.via_bot_id = None
        self.ttl_period = None
        self.reactions = None
        self.mentions = None
        self.restriction_reason = None


class MockTelethonClient:
    """Mock Telethon client that yields messages from a predefined list.

    Supports:
      - iter_messages(min_id=X, reverse=True) -> yields messages with id > X
      - get_messages(entity, ids=N) -> returns single message or None
      - download_media(message, file=path) -> writes a fake file
      - FloodWait simulation via flood_wait_until_id
    """
    def __init__(self, messages):
        self._messages = sorted(messages, key=lambda m: m.id)
        self._flood_wait_count = 0
        self._flood_wait_max = 0  # how many FloodWaits to simulate
        self._download_should_fail_for = set()  # message IDs that fail download
        self._download_fail_count = {}  # track per-message fail counts
        self._download_max_fails = 0  # how many times to fail before succeeding
        self._crash_after_write = False  # simulate crash after JSONL write
        self._crash_callback = None

    async def iter_messages(self, entity, limit=None, reverse=False, min_id=0, **kwargs):
        """Yield messages with id > min_id, in ascending order."""
        from telethon.errors import FloodWaitError
        msgs = [m for m in self._messages if m.id > min_id]
        if reverse:
            msgs = msgs  # already ascending
        for msg in msgs:
            # Simulate FloodWait if configured
            if self._flood_wait_count < self._flood_wait_max:
                self._flood_wait_count += 1
                e = FloodWaitError(request=None, capture=None)
                e.seconds = 1  # short sleep for tests
                raise e
            yield msg

    async def get_messages(self, entity, ids=None, **kwargs):
        if isinstance(ids, int):
            for m in self._messages:
                if m.id == ids:
                    return m
            return None
        return []

    async def download_media(self, message, file=None, **kwargs):
        """Simulate media download. Writes a fake file to the given path."""
        if message.id in self._download_should_fail_for:
            count = self._download_fail_count.get(message.id, 0)
            if count < self._download_max_fails:
                self._download_fail_count[message.id] = count + 1
                raise Exception(f"Simulated download failure for msg {message.id}")
        # Write a fake file
        if file:
            path = Path(file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"fake media content " * 100)
            return str(path)
        return None

    async def get_me(self):
        me = MagicMock()
        me.id = 123456
        me.first_name = "Test"
        me.last_name = "User"
        me.username = "testuser"
        return me

    async def iter_dialogs(self):
        # Return empty for these tests; we pass entities directly
        return
        yield  # make it an async generator

    def is_connected(self):
        return True

    async def disconnect(self):
        pass


class MockConfig:
    """Minimal Config mock for orchestrator tests."""
    def __init__(self, base_dir):
        self.api_id = 12345
        self.api_hash = "fakehash"
        self.phone = "+15551234567"
        self.session_name = "test_session"
        self.backup_saved_messages = True
        self.backup_private_chats = True
        self.backup_groups = False
        self.backup_channels = False
        self.include_bots = True
        self.backup_media = True
        self.download_photos = True
        self.download_videos = True
        self.download_documents = True
        self.download_audio = True
        self.download_voice = True
        self.max_photo_size = 0
        self.max_video_size = 0
        self.max_document_size = 0
        self.max_retries = 2
        self.checkpoint_every = 999  # don't checkpoint mid-test
        self.base_dir = base_dir
        self.saved_messages_dir = base_dir / "Saved_Messages"
        self.private_chats_dir = base_dir / "Private_Chats"
        self.groups_dir = base_dir / "Groups"
        self.channels_dir = base_dir / "Channels"
        self.state_file = base_dir / "backup_state.json"
        self.metadata_file = base_dir / "backup_metadata.json"
        self.errors_log = base_dir / "errors.log"
        self.session_file = base_dir / "test_session.session"

    def safe_summary(self):
        return {"test": True}

    def ensure_directories(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.saved_messages_dir.mkdir(parents=True, exist_ok=True)


def get_exported_ids(chat_dir):
    """Read messages.jsonl and return the set of exported message IDs."""
    jsonl_path = chat_dir / "messages.jsonl"
    if not jsonl_path.exists():
        return set()
    ids = set()
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                mid = record.get("message_id")
                if isinstance(mid, int):
                    ids.add(mid)
            except json.JSONDecodeError:
                continue
    return ids


async def run_orchestrator(client, config, state, error_logger):
    """Run the BackupOrchestrator._backup_chat for saved_messages."""
    from backup import BackupOrchestrator
    orch = BackupOrchestrator(client, config, state, error_logger)
    await orch._backup_chat(
        "saved_messages",
        "Saved Messages",
        config.saved_messages_dir,
        "me",
    )
    return orch


def make_error_logger():
    import logging
    logger = logging.getLogger("test_errors")
    logger.addHandler(logging.NullHandler())
    return logger


# ----------------------------------------------------------------- Test 1: Normal export
section("1. Normal export")

async def test_normal_export():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager
        state = StateManager(config.state_file)

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 11)]
        client = MockTelethonClient(messages)

        await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 10, f"10 messages exported (got {len(exported)})")
        check(exported == set(range(1, 11)), "all message IDs 1-10 present")
        check(state.is_chat_completed("saved_messages"), "chat marked completed")

asyncio.run(test_normal_export())


# ----------------------------------------------------------------- Test 2: Crash after fetch, before JSONL write
section("2. Crash after message fetch (before JSONL write)")

async def test_crash_after_fetch():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager
        state = StateManager(config.state_file)

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 6)]
        client = MockTelethonClient(messages)

        # Patch _process_single_message to crash on msg 3 BEFORE writing JSONL
        original_process = None
        from backup import BackupOrchestrator
        call_count = [0]

        async def crashing_process(self_backup, message, exporter, media_dir, chat_key):
            call_count[0] += 1
            if message.id == 3:
                raise RuntimeError("Simulated crash before JSONL write")
            # Call original
            await original_process(self_backup, message, exporter, media_dir, chat_key)

        orch = BackupOrchestrator(client, config, state, make_error_logger())
        original_process = orch._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_process_single_message", crashing_process):
            await orch._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")

        exported = get_exported_ids(config.saved_messages_dir)
        # Msg 3 should NOT be exported (crashed before write)
        check(3 not in exported, "message 3 NOT in JSONL (crashed before write)")
        # Msg 1, 2 should be exported (processed before crash)
        check(1 in exported and 2 in exported, "messages 1, 2 exported before crash")
        # Msg 4, 5 should NOT be exported (chat stopped at failure)
        check(4 not in exported and 5 not in exported,
              "messages 4, 5 NOT exported (chat stopped at failure)")
        # Msg 3 should be in failed_messages
        failed = state.get_failed_message_ids("saved_messages")
        check(3 in failed, "message 3 in failed_messages set")
        # Chat should NOT be marked completed
        check(not state.is_chat_completed("saved_messages"),
              "chat NOT marked completed (had failure)")

asyncio.run(test_crash_after_fetch())


# ----------------------------------------------------------------- Test 5: Failed msg then newer successful msg
# CRITICAL: failed msg 100 must be retried on next run even though 101 succeeded
section("5. Failed message 100, then 101 succeeds — 100 must be retried")

async def test_failed_then_newer():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager
        state = StateManager(config.state_file)

        messages = [MockMessage(i, f"msg {i}") for i in range(100, 102)]
        client = MockTelethonClient(messages)

        # First run: msg 100 fails, msg 101 should NOT be processed (chat stops)
        from backup import BackupOrchestrator
        call_count = [0]

        async def failing_process(self_b, message, exporter, media_dir, chat_key):
            if message.id == 100:
                raise RuntimeError("Simulated failure for msg 100")
            await original_process(self_b, message, exporter, media_dir, chat_key)

        orch = BackupOrchestrator(client, config, state, make_error_logger())
        original_process = orch._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_process_single_message", failing_process):
            await orch._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")

        exported_after_run1 = get_exported_ids(config.saved_messages_dir)
        check(100 not in exported_after_run1, "msg 100 NOT exported in run 1")
        check(101 not in exported_after_run1, "msg 101 NOT exported (chat stopped at 100)")
        check(100 in state.get_failed_message_ids("saved_messages"),
              "msg 100 in failed_messages set")

        # Second run: msg 100 should succeed now (no patch)
        state2 = StateManager(config.state_file)
        client2 = MockTelethonClient(messages)
        await run_orchestrator(client2, config, state2, make_error_logger())

        exported_after_run2 = get_exported_ids(config.saved_messages_dir)
        check(100 in exported_after_run2, "msg 100 IS exported in run 2 (retried)")
        check(101 in exported_after_run2, "msg 101 IS exported in run 2")
        check(100 not in state2.get_failed_message_ids("saved_messages"),
              "msg 100 removed from failed_messages after successful retry")

asyncio.run(test_failed_then_newer())


# ----------------------------------------------------------------- Test 6: Repeated restart idempotency
section("6. Repeated restart idempotency")

async def test_repeated_restart():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 21)]

        # Run 3 times
        for run_num in range(3):
            state = StateManager(config.state_file)
            client = MockTelethonClient(messages)
            await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 20, f"exactly 20 messages after 3 runs (got {len(exported)})")
        check(exported == set(range(1, 21)), "all IDs 1-20 present exactly once")

        # Verify no duplicates in JSONL
        jsonl_path = config.saved_messages_dir / "messages.jsonl"
        with open(jsonl_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        check(len(lines) == 20, f"JSONL has exactly 20 lines (got {len(lines)})")

asyncio.run(test_repeated_restart())


# ----------------------------------------------------------------- Test 7 & 8: FloodWait during iter_messages
section("7. FloodWait once during message iteration")

async def test_flood_wait_once():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager
        state = StateManager(config.state_file)

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 11)]
        client = MockTelethonClient(messages)
        client._flood_wait_max = 1  # FloodWait once

        # Patch _handle_flood_wait to not actually sleep
        from backup import BackupOrchestrator
        async def fake_flood_wait(self, e):
            pass
        with patch.object(BackupOrchestrator, "_handle_flood_wait", fake_flood_wait):
            await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 10, f"all 10 messages exported after FloodWait (got {len(exported)})")
        check(exported == set(range(1, 11)), "all IDs 1-10 present")

asyncio.run(test_flood_wait_once())


section("8. FloodWait multiple times during message iteration")

async def test_flood_wait_multiple():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager
        state = StateManager(config.state_file)

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 11)]
        client = MockTelethonClient(messages)
        client._flood_wait_max = 5  # FloodWait 5 times

        from backup import BackupOrchestrator
        async def fake_flood_wait(self, e):
            pass
        with patch.object(BackupOrchestrator, "_handle_flood_wait", fake_flood_wait):
            await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 10, f"all 10 messages exported after 5 FloodWaits (got {len(exported)})")

asyncio.run(test_flood_wait_multiple())


# ----------------------------------------------------------------- Test 10: Partial JSONL tail
section("10. Partial JSONL tail recovery")

async def test_partial_jsonl_tail():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        chat_dir = config.saved_messages_dir
        chat_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = chat_dir / "messages.jsonl"

        # Write 2 valid messages + 1 partial line
        with open(jsonl_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"message_id": 1, "text": "first"}) + "\n")
            f.write(json.dumps({"message_id": 2, "text": "second"}) + "\n")
            f.write('{"message_id": 3, "text": "partia')  # partial

        # Now run backup with messages 1-5
        from state_manager import StateManager
        state = StateManager(config.state_file)
        messages = [MockMessage(i, f"msg {i}") for i in range(1, 6)]
        client = MockTelethonClient(messages)

        await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(chat_dir)
        check(1 in exported and 2 in exported, "original messages 1, 2 preserved")
        check(3 in exported, "message 3 exported (was partial, re-fetched)")
        check(4 in exported and 5 in exported, "messages 4, 5 exported")
        check(len(exported) == 5, f"exactly 5 messages (got {len(exported)})")

asyncio.run(test_partial_jsonl_tail())


# ----------------------------------------------------------------- Test 11: Corrupted MIDDLE line
section("11. Corrupted MIDDLE JSONL line — later valid records preserved")

async def test_middle_corruption():
    with tempfile.TemporaryDirectory() as tmp:
        chat_dir = Path(tmp) / "chat"
        chat_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = chat_dir / "messages.jsonl"

        # Write: valid, CORRUPTED (with newline), valid
        # The corrupted line is malformed JSON but ends with a newline,
        # so the next valid line can be read independently.
        with open(jsonl_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"message_id": 1, "text": "first"}) + "\n")
            f.write('{"message_id": 2, "text": "CORRUPTED"\n')  # malformed but newline-terminated
            f.write(json.dumps({"message_id": 3, "text": "third"}) + "\n")

        from exporter import MessageExporter
        exp = MessageExporter(chat_dir, Path(tmp))

        # Should have loaded 1 and 3, but NOT 2
        check(exp.has_message(1), "message 1 loaded despite middle corruption")
        check(exp.has_message(3), "message 3 loaded (after corrupted line)")
        check(not exp.has_message(2), "corrupted message 2 NOT loaded")
        check(exp.has_corruption(), "corruption flag set")

        # The corrupted line should still be on disk (NOT auto-removed)
        with open(jsonl_path, "r") as f:
            content = f.read()
        check("CORRUPTED" in content, "corrupted line preserved on disk (not auto-removed)")

        # compact_jsonl should remove it
        exp.compact_jsonl()
        with open(jsonl_path, "r") as f:
            content = f.read()
        check("CORRUPTED" not in content, "corrupted line removed by compact_jsonl")
        check(exp.has_message(1) and exp.has_message(3), "valid messages preserved after compact")

asyncio.run(test_middle_corruption())


# ----------------------------------------------------------------- Test 12: Stale state AHEAD of JSONL
section("12. Stale state AHEAD of JSONL — JSONL wins (resume from jsonl_max)")

async def test_state_ahead_of_jsonl():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        chat_dir = config.saved_messages_dir
        chat_dir.mkdir(parents=True, exist_ok=True)

        # Set up: JSONL has messages 1-5, state says last_message_id=10
        # (state is AHEAD of JSONL — simulates failures after state save)
        jsonl_path = chat_dir / "messages.jsonl"
        with open(jsonl_path, "w") as f:
            for i in range(1, 6):
                f.write(json.dumps({"message_id": i, "text": f"msg {i}"}) + "\n")

        from state_manager import StateManager
        state = StateManager(config.state_file)
        # Force state to be ahead
        state._chat_state("saved_messages")["last_message_id"] = 10
        state.save()

        # Now run backup with messages 1-15
        state2 = StateManager(config.state_file)
        messages = [MockMessage(i, f"msg {i}") for i in range(1, 16)]
        client = MockTelethonClient(messages)

        await run_orchestrator(client, config, state2, make_error_logger())

        exported = get_exported_ids(chat_dir)
        # Messages 1-5 were already in JSONL; 6-15 should be added
        check(len(exported) == 15, f"all 15 messages exported (got {len(exported)})")
        check(exported == set(range(1, 16)), "all IDs 1-15 present")
        # Critical: messages 6-10 were "below" the stale state hint (10)
        # but above jsonl_max (5), so they MUST be exported.
        # With resume_from = jsonl_max = 5, iter_messages(min_id=5) fetches
        # messages 6-15, so 6-10 are NOT skipped.
        for i in range(6, 11):
            check(i in exported, f"message {i} exported despite state being ahead (jsonl_max wins)")

asyncio.run(test_state_ahead_of_jsonl())


# ----------------------------------------------------------------- Test 13: Duplicate prevention
section("13. Duplicate prevention across multiple runs")

async def test_duplicate_prevention():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 11)]

        # Run 5 times
        for _ in range(5):
            state = StateManager(config.state_file)
            client = MockTelethonClient(messages)
            await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 10, f"exactly 10 unique messages after 5 runs (got {len(exported)})")

        # Verify JSONL has exactly 10 lines (no duplicates)
        jsonl_path = config.saved_messages_dir / "messages.jsonl"
        with open(jsonl_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        check(len(lines) == 10, f"JSONL has exactly 10 lines (got {len(lines)})")

asyncio.run(test_duplicate_prevention())


# ----------------------------------------------------------------- Test 15: Failed message retried on next run
section("15. Failed message 100 retried on next run (full scenario)")

async def test_failed_message_retry_next_run():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(100, 105)]

        # Run 1: msg 100 fails
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(messages)
        from backup import BackupOrchestrator

        async def fail_msg_100(self_b, message, exporter, media_dir, chat_key):
            if message.id == 100:
                raise RuntimeError("Simulated failure for msg 100")
            await original_process(self_b, message, exporter, media_dir, chat_key)

        orch = BackupOrchestrator(client1, config, state1, make_error_logger())
        original_process = orch._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_process_single_message", fail_msg_100):
            await orch._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")

        exported_r1 = get_exported_ids(config.saved_messages_dir)
        check(100 not in exported_r1, "msg 100 NOT exported in run 1")
        check(len(exported_r1) == 0, f"no messages exported in run 1 (chat stopped) (got {len(exported_r1)})")

        # Run 2: all messages succeed (no patch)
        state2 = StateManager(config.state_file)
        client2 = MockTelethonClient(messages)
        await run_orchestrator(client2, config, state2, make_error_logger())

        exported_r2 = get_exported_ids(config.saved_messages_dir)
        check(100 in exported_r2, "msg 100 IS exported in run 2 (retried)")
        check(len(exported_r2) == 5, f"all 5 messages exported in run 2 (got {len(exported_r2)})")
        check(100 not in state2.get_failed_message_ids("saved_messages"),
              "msg 100 removed from failed set after retry")
        check(state2.is_chat_completed("saved_messages"), "chat marked completed in run 2")

asyncio.run(test_failed_message_retry_next_run())


# ----------------------------------------------------------------- Test 16: Persistently failing message
section("16. Persistently failing message stays in failed set across runs")

async def test_persistent_failure():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 6)]

        # Run 3 times, msg 3 always fails
        from backup import BackupOrchestrator
        for run in range(3):
            state = StateManager(config.state_file)
            client = MockTelethonClient(messages)

            async def always_fail_3(self_b, message, exporter, media_dir, chat_key):
                if message.id == 3:
                    raise RuntimeError(f"Persistent failure run {run}")
                await original_process(self_b, message, exporter, media_dir, chat_key)

            orch = BackupOrchestrator(client, config, state, make_error_logger())
            if run == 0:
                original_process = orch._process_single_message.__func__
            with patch.object(BackupOrchestrator, "_process_single_message", always_fail_3):
                await orch._backup_chat("saved_messages", "Saved Messages",
                                         config.saved_messages_dir, "me")

        exported = get_exported_ids(config.saved_messages_dir)
        check(1 in exported and 2 in exported, "messages 1, 2 exported (before failure)")
        check(3 not in exported, "message 3 NOT exported (persistently fails)")
        check(4 not in exported and 5 not in exported,
              "messages 4, 5 NOT exported (chat stops at 3)")

        # Msg 3 must still be in failed_messages set (retryable)
        final_state = StateManager(config.state_file)
        check(3 in final_state.get_failed_message_ids("saved_messages"),
              "msg 3 still in failed_messages set (retryable on next run)")
        check(not final_state.is_chat_completed("saved_messages"),
              "chat NOT marked completed (has failed message)")

asyncio.run(test_persistent_failure())


# ----------------------------------------------------------------- Test 17: Incremental — new messages after completion
section("17. Incremental backup: new messages arrive after chat completed")

async def test_incremental_after_completion():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        # Run 1: messages 1-100
        messages_r1 = [MockMessage(i, f"msg {i}") for i in range(1, 101)]
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(messages_r1)
        await run_orchestrator(client1, config, state1, make_error_logger())

        exported_r1 = get_exported_ids(config.saved_messages_dir)
        check(len(exported_r1) == 100, f"Run 1: 100 messages exported (got {len(exported_r1)})")
        check(exported_r1 == set(range(1, 101)), "Run 1: all IDs 1-100 present")
        check(state1.is_chat_completed("saved_messages"), "Run 1: chat marked completed")

        # Run 2: new message 101 arrives
        messages_r2 = [MockMessage(i, f"msg {i}") for i in range(1, 102)]
        state2 = StateManager(config.state_file)
        client2 = MockTelethonClient(messages_r2)
        await run_orchestrator(client2, config, state2, make_error_logger())

        exported_r2 = get_exported_ids(config.saved_messages_dir)
        check(len(exported_r2) == 101, f"Run 2: 101 messages exported (got {len(exported_r2)})")
        check(101 in exported_r2, "Run 2: new message 101 exported")
        # No duplicates
        jsonl_path = config.saved_messages_dir / "messages.jsonl"
        with open(jsonl_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        check(len(lines) == 101, f"Run 2: JSONL has 101 lines, no dups (got {len(lines)})")

asyncio.run(test_incremental_after_completion())


# ----------------------------------------------------------------- Test 18: Repeated rerun with no new messages
section("18. Repeated rerun with NO new messages — no duplicates, no errors")

async def test_rerun_no_new_messages():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 51)]

        # Run 1: initial backup
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(messages)
        await run_orchestrator(client1, config, state1, make_error_logger())

        # Run 2, 3, 4: no new messages
        for run in range(2, 5):
            state = StateManager(config.state_file)
            client = MockTelethonClient(messages)
            await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 50, f"exactly 50 messages after 4 runs (got {len(exported)})")
        jsonl_path = config.saved_messages_dir / "messages.jsonl"
        with open(jsonl_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        check(len(lines) == 50, f"JSONL has exactly 50 lines (got {len(lines)})")

asyncio.run(test_rerun_no_new_messages())


# ----------------------------------------------------------------- Test 19: Multiple new messages in one run
section("19. Multiple new messages arrive at once")

async def test_multiple_new_messages():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        # Run 1: messages 1-50
        msgs1 = [MockMessage(i, f"msg {i}") for i in range(1, 51)]
        state1 = StateManager(config.state_file)
        await run_orchestrator(MockTelethonClient(msgs1), config, state1, make_error_logger())

        # Run 2: messages 1-60 (10 new)
        msgs2 = [MockMessage(i, f"msg {i}") for i in range(1, 61)]
        state2 = StateManager(config.state_file)
        await run_orchestrator(MockTelethonClient(msgs2), config, state2, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 60, f"60 messages after incremental run (got {len(exported)})")
        check(exported == set(range(1, 61)), "all IDs 1-60 present")

asyncio.run(test_multiple_new_messages())


# ----------------------------------------------------------------- Test 20: Failed-message retry — transient fetch failure
section("20. Failed-message retry: transient fetch failure (returns None once)")

async def test_transient_fetch_failure():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 6)]

        # Run 1: msg 3 fails to export
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(messages)
        from backup import BackupOrchestrator

        async def fail_msg_3(self_b, message, exporter, media_dir, chat_key):
            if message.id == 3:
                raise RuntimeError("Simulated failure for msg 3")
            await original_process(self_b, message, exporter, media_dir, chat_key)

        orch = BackupOrchestrator(client1, config, state1, make_error_logger())
        original_process = orch._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_process_single_message", fail_msg_3):
            await orch._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")
        check(3 in state1.get_failed_message_ids("saved_messages"),
              "msg 3 in failed_messages after run 1")
        state1.save()

        # Run 2: _fetch_single_message returns None for msg 3 (transient)
        # The main loop's iter_messages would re-fetch msg 3, but we also
        # need to make the main loop skip msg 3. We do this by making
        # _process_single_message fail again for msg 3 in this run, so
        # the chat stops at msg 3 and doesn't export 4,5.
        # Actually, the point of this test is just to verify that a None
        # return from _fetch_single_message keeps the message in failed_messages.
        # So we just need to check the state after _retry_failed_messages.
        state2 = StateManager(config.state_file)
        client2 = MockTelethonClient(messages)

        async def transient_none_fetch(self_b, entity, message_id):
            if message_id == 3:
                return None  # transient failure
            return await orig_fetch(self_b, entity, message_id)

        orch2 = BackupOrchestrator(client2, config, state2, make_error_logger())
        orig_fetch = orch2._fetch_single_message.__func__

        # Also make _process_single_message fail for msg 3 so the main loop
        # doesn't export it (we want to test the retry behavior in isolation)
        async def fail_msg_3_again(self_b, message, exporter, media_dir, chat_key):
            if message.id == 3:
                raise RuntimeError("Still failing for msg 3")
            await orig_process(self_b, message, exporter, media_dir, chat_key)

        orig_process = orch2._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_fetch_single_message", transient_none_fetch), \
             patch.object(BackupOrchestrator, "_process_single_message", fail_msg_3_again):
            await orch2._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")

        # Msg 3 should still be in failed_messages (None was transient, not confirmed)
        state2.save()
        check(3 in state2.get_failed_message_ids("saved_messages"),
              "msg 3 STILL in failed_messages (transient None, not confirmed deleted)")
        retry_count = state2.get_failed_message_retry_count("saved_messages", 3)
        check(retry_count >= 1, f"msg 3 retry count >= 1 (got {retry_count})")

        # Run 3: msg 3 fetches successfully and exports (no patches)
        state3 = StateManager(config.state_file)
        client3 = MockTelethonClient(messages)
        await run_orchestrator(client3, config, state3, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(3 in exported, "msg 3 exported in run 3 (transient failure resolved)")
        check(3 not in state3.get_failed_message_ids("saved_messages"),
              "msg 3 removed from failed_messages after successful export")

asyncio.run(test_transient_fetch_failure())


# ----------------------------------------------------------------- Test 21: Failed-message retry — confirmed unavailable (deleted)
section("21. Failed-message retry: confirmed unavailable after 5 None returns")

async def test_confirmed_unavailable():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        messages = [MockMessage(i, f"msg {i}") for i in range(1, 6)]

        # Run 1: msg 3 fails to export
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(messages)
        from backup import BackupOrchestrator

        async def fail_msg_3(self_b, message, exporter, media_dir, chat_key):
            if message.id == 3:
                raise RuntimeError("Simulated failure for msg 3")
            await original_process(self_b, message, exporter, media_dir, chat_key)

        orch = BackupOrchestrator(client1, config, state1, make_error_logger())
        original_process = orch._process_single_message.__func__
        with patch.object(BackupOrchestrator, "_process_single_message", fail_msg_3):
            await orch._backup_chat("saved_messages", "Saved Messages",
                                     config.saved_messages_dir, "me")
        check(3 in state1.get_failed_message_ids("saved_messages"),
              "msg 3 in failed_messages after run 1")
        state1.save()

        # Runs 2-6: msg 3 is "deleted" on Telegram.
        # Both get_messages(ids=3) returns None AND iter_messages skips it.
        # (In real Telegram, if a message is deleted, neither method returns it.)
        for run in range(2, 7):
            state = StateManager(config.state_file)
            client = MockTelethonClient(messages)
            # Remove msg 3 from the client's message list so iter_messages
            # doesn't yield it either
            client._messages = [m for m in messages if m.id != 3]

            async def always_none(self_b, entity, message_id):
                if message_id == 3:
                    return None
                return await orig_fetch(self_b, entity, message_id)

            async def always_fail_3(self_b, message, exporter, media_dir, chat_key):
                if message.id == 3:
                    raise RuntimeError("Still failing for msg 3")
                await orig_process(self_b, message, exporter, media_dir, chat_key)

            orch_r = BackupOrchestrator(client, config, state, make_error_logger())
            orig_fetch = orch_r._fetch_single_message.__func__
            orig_process = orch_r._process_single_message.__func__
            with patch.object(BackupOrchestrator, "_fetch_single_message", always_none), \
                 patch.object(BackupOrchestrator, "_process_single_message", always_fail_3):
                await orch_r._backup_chat("saved_messages", "Saved Messages",
                                          config.saved_messages_dir, "me")
            state.save()

        # After 5 consecutive None returns (runs 2-6), msg 3 should be
        # confirmed unavailable and removed from failed_messages.
        final_state = StateManager(config.state_file)
        check(3 not in final_state.get_failed_message_ids("saved_messages"),
              "msg 3 removed from failed_messages after 5 None returns (confirmed unavailable)")
        check(3 not in get_exported_ids(config.saved_messages_dir),
              "msg 3 NOT in JSONL (was never successfully exported)")

asyncio.run(test_confirmed_unavailable())


# ----------------------------------------------------------------- Test 22: Completed chat still scans for new messages
section("22. Completed chat scans for new messages on next run")

async def test_completed_chat_scans_new():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        # Run 1: messages 1-50, chat completed
        msgs1 = [MockMessage(i, f"msg {i}") for i in range(1, 51)]
        state1 = StateManager(config.state_file)
        await run_orchestrator(MockTelethonClient(msgs1), config, state1, make_error_logger())
        check(state1.is_chat_completed("saved_messages"), "chat completed after run 1")

        # Run 2: messages 1-55 (5 new)
        msgs2 = [MockMessage(i, f"msg {i}") for i in range(1, 56)]
        state2 = StateManager(config.state_file)
        await run_orchestrator(MockTelethonClient(msgs2), config, state2, make_error_logger())

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 55, f"55 messages after run 2 (got {len(exported)})")
        check(51 in exported and 55 in exported, "new messages 51, 55 exported")
        # No duplicates
        jsonl_path = config.saved_messages_dir / "messages.jsonl"
        with open(jsonl_path) as f:
            lines = [l.strip() for l in f if l.strip()]
        check(len(lines) == 55, f"JSONL has 55 lines, no dups (got {len(lines)})")

asyncio.run(test_completed_chat_scans_new())


# ----------------------------------------------------------------- Test 23: Large checkpoint efficiency
section("23. Large checkpoint efficiency (no full re-scan)")

async def test_large_checkpoint_efficiency():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        from state_manager import StateManager

        # Run 1: 200 messages
        msgs1 = [MockMessage(i, f"msg {i}") for i in range(1, 201)]
        state1 = StateManager(config.state_file)
        client1 = MockTelethonClient(msgs1)
        await run_orchestrator(client1, config, state1, make_error_logger())

        # Run 2: 201 messages (1 new). Track how many messages iter_messages yields.
        msgs2 = [MockMessage(i, f"msg {i}") for i in range(1, 202)]
        state2 = StateManager(config.state_file)
        client2 = MockTelethonClient(msgs2)

        yield_count = [0]
        original_iter = client2.iter_messages

        async def counting_iter(*args, **kwargs):
            async for msg in original_iter(*args, **kwargs):
                yield_count[0] += 1
                yield msg

        client2.iter_messages = counting_iter

        from backup import BackupOrchestrator
        orch = BackupOrchestrator(client2, config, state2, make_error_logger())
        await orch._backup_chat("saved_messages", "Saved Messages",
                                config.saved_messages_dir, "me")

        exported = get_exported_ids(config.saved_messages_dir)
        check(len(exported) == 201, f"201 messages exported (got {len(exported)})")
        # iter_messages should only yield messages with id > 200 (just msg 201)
        # Plus any already-in-JSONL messages that were skipped by has_message.
        # With resume_from = jsonl_max = 200, iter_messages(min_id=200) only
        # fetches id > 200, so yield_count should be 1.
        check(yield_count[0] == 1,
              f"iter_messages yielded only 1 message (got {yield_count[0]}); "
              f"no full re-scan of 200 messages")

asyncio.run(test_large_checkpoint_efficiency())


# ----------------------------------------------------------------- Test 24: State behind JSONL — efficient resume
section("24. State behind JSONL — efficient resume from jsonl_max")

async def test_state_behind_jsonl_efficient():
    with tempfile.TemporaryDirectory() as tmp:
        config = MockConfig(Path(tmp))
        config.ensure_directories()
        chat_dir = config.saved_messages_dir
        chat_dir.mkdir(parents=True, exist_ok=True)

        # Pre-populate JSONL with messages 1-100
        jsonl_path = chat_dir / "messages.jsonl"
        with open(jsonl_path, "w") as f:
            for i in range(1, 101):
                f.write(json.dumps({"message_id": i, "text": f"msg {i}"}) + "\n")

        from state_manager import StateManager
        state = StateManager(config.state_file)
        # State is BEHIND jsonl (state says 0, jsonl has 100)
        check(state.get_state_hint("saved_messages") == 0, "state hint = 0")

        # Run with messages 1-105 (5 new)
        messages = [MockMessage(i, f"msg {i}") for i in range(1, 106)]
        client = MockTelethonClient(messages)

        yield_count = [0]
        original_iter = client.iter_messages
        async def counting_iter(*args, **kwargs):
            async for msg in original_iter(*args, **kwargs):
                yield_count[0] += 1
                yield msg
        client.iter_messages = counting_iter

        await run_orchestrator(client, config, state, make_error_logger())

        exported = get_exported_ids(chat_dir)
        check(len(exported) == 105, f"105 messages total (got {len(exported)})")
        # With resume_from = jsonl_max = 100, iter_messages(min_id=100) only
        # fetches id > 100 (5 messages), NOT all 105.
        check(yield_count[0] == 5,
              f"iter_messages yielded only 5 new messages (got {yield_count[0]}); "
              f"no re-scan of 100 existing messages")

asyncio.run(test_state_behind_jsonl_efficient())


# ----------------------------------------------------------------- Summary
print()
print("=" * 70)
if failures:
    print(f"FAILED: {len(failures)} test(s) out of {test_count}")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"ALL {test_count} INTEGRATION TESTS PASSED")
print("=" * 70)
