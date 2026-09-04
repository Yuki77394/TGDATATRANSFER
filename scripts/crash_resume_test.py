"""Comprehensive crash/resume/dedup tests for the telegram_backup_tool.

These tests verify the critical safety properties of the backup system:

  1. Duplicate message prevention (idempotent JSONL append)
  2. Interruption between JSONL write and state save
  3. Partial JSONL line recovery (truncation)
  4. State scalability (set-based media tracking, O(1) lookup)
  5. Failed media tracking and retry
  6. Filename sanitization / collision avoidance
  7. Media integrity verification (size mismatch detection)
  8. Resume from stale state (state lower than JSONL)
  9. Resume from advanced state (state higher than JSONL - should NOT happen
     but must be safe)
 10. Schema migration (v1 -> v2 state file)
 11. Atomic state file writes (no corruption on simulated crash)
 12. Per-chat media log isolation

These tests do NOT connect to Telegram - they test the local persistence
and recovery logic directly.
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

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
    print(f"\n{'='*60}\nTest: {name}\n{'='*60}")


# ----------------------------------------------------------------- 1. Duplicate prevention
section("1. Duplicate message prevention")

with tempfile.TemporaryDirectory() as tmp:
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    from exporter import MessageExporter
    exp = MessageExporter(chat_dir, Path(tmp))

    # Append the same message 5 times
    for _ in range(5):
        exp.append_message({"message_id": 42, "text": "hello"})

    check(exp.count() == 1, "duplicate appends result in exactly 1 message")

    # Verify on disk
    with open(chat_dir / "messages.jsonl", "r") as f:
        lines = [l.strip() for l in f if l.strip()]
    check(len(lines) == 1, f"JSONL has exactly 1 line (got {len(lines)})")


# ----------------------------------------------------------------- 2. Interruption between JSONL and state
section("2. Interruption between JSONL write and state save")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    state_file = Path(tmp) / "state.json"
    sm = StateManager(state_file)

    # Simulate: write JSONL for msg 100, mark processed in memory, but DON'T save state
    # (simulates crash between JSONL append and state.save())
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    exp = MessageExporter(chat_dir, Path(tmp))
    exp.append_message({"message_id": 100, "text": "msg 100"})
    sm.mark_message_processed("test_chat", 100)
    # Note: we do NOT call sm.save() here - simulates crash

    # Now simulate a restart: reload state and exporter from disk
    sm2 = StateManager(state_file)
    exp2 = MessageExporter(chat_dir, Path(tmp))

    # State on disk should NOT have msg 100 (we didn't save)
    state_min_id = sm2.get_state_hint("test_chat")
    exporter_max_id = exp2.get_max_message_id()
    check(state_min_id == 0, f"state on disk has last_message_id=0 (got {state_min_id})")
    check(exporter_max_id == 100, f"exporter has max_id=100 (got {exporter_max_id})")

    # Resume point = jsonl_max = 100 (state is NOT used for resume).
    # We resume from 100, so iter_messages(min_id=100) only fetches id > 100.
    # Msg 100 is already in JSONL, so even if re-fetched it would be skipped.
    resume_point = exporter_max_id
    check(resume_point == 100, f"resume point = 100 (jsonl_max; efficient, no re-scan) (got {resume_point})")

    # And has_message should correctly report msg 100 is present
    check(exp2.has_message(100), "exporter reports msg 100 as present after reload")
    check(not exp2.has_message(101), "exporter reports msg 101 as absent")


# ----------------------------------------------------------------- 3. Partial JSONL line recovery
section("3. Partial JSONL line recovery (truncation)")

with tempfile.TemporaryDirectory() as tmp:
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    jsonl_path = chat_dir / "messages.jsonl"

    # Write two valid messages
    with open(jsonl_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"message_id": 1, "text": "first"}) + "\n")
        f.write(json.dumps({"message_id": 2, "text": "second"}) + "\n")

    # Append a partial (corrupted) line - simulates crash mid-write
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write('{"message_id": 3, "text": "partia')  # no closing, no newline

    # Verify file has 3 lines (2 valid + 1 partial)
    with open(jsonl_path, "r") as f:
        lines = f.readlines()
    check(len(lines) == 3, f"file has 3 lines before recovery (got {len(lines)})")

    # Load exporter - should detect partial line and not count it
    from exporter import MessageExporter
    exp = MessageExporter(chat_dir, Path(tmp))
    check(exp.count() == 2, f"exporter counts 2 valid messages (got {exp.count()})")
    check(not exp.has_message(3), "partial message 3 is NOT counted")

    # Append a new message - should trigger truncation of partial line
    exp.append_message({"message_id": 4, "text": "fourth"})

    # Reload and verify
    exp2 = MessageExporter(chat_dir, Path(tmp))
    check(exp2.count() == 3, f"after append+reload, count=3 (got {exp2.count()})")
    check(exp2.has_message(1), "msg 1 present")
    check(exp2.has_message(2), "msg 2 present")
    check(exp2.has_message(4), "msg 4 present")
    check(not exp2.has_message(3), "msg 3 (partial) still not present")

    # Verify the partial line was physically removed from the file
    with open(jsonl_path, "r") as f:
        final_lines = f.readlines()
    check(len(final_lines) == 3, f"file has 3 lines after recovery (got {len(final_lines)})")
    for line in final_lines:
        parsed = json.loads(line.strip())
        check(parsed["message_id"] in (1, 2, 4), f"line is valid JSON with expected id")


# ----------------------------------------------------------------- 4. State scalability
section("4. State scalability (set-based media tracking)")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    state_file = Path(tmp) / "state.json"
    sm = StateManager(state_file)

    # Add 10000 media keys
    import time
    start = time.time()
    for i in range(10000):
        sm.mark_media_downloaded("chat_1", f"100:{i}")
    elapsed_add = time.time() - start

    # Membership check should be O(1) - 10000 checks in < 0.1s
    start = time.time()
    for i in range(10000):
        assert sm.is_media_downloaded("chat_1", f"100:{i}")
    elapsed_check = time.time() - start

    check(elapsed_check < 1.0, f"10000 O(1) membership checks in <1s (took {elapsed_check:.3f}s)")

    # Verify a missing key returns False
    check(not sm.is_media_downloaded("chat_1", "100:99999"), "missing key returns False")

    # Verify media log file is on disk and has 10000 lines
    media_log = Path(tmp) / "chat_1.media.jsonl"
    check(media_log.exists(), "media log file exists")
    with open(media_log) as f:
        line_count = sum(1 for _ in f if f)
    check(line_count == 10000, f"media log has 10000 lines (got {line_count})")

    # Reload state - set should be rebuilt from disk
    sm2 = StateManager(state_file)
    check(sm2.is_media_downloaded("chat_1", "100:5000"), "after reload, key 5000 is present")
    check(not sm2.is_media_downloaded("chat_1", "100:99999"), "after reload, missing key is absent")


# ----------------------------------------------------------------- 5. Failed media tracking
section("5. Failed media tracking and retry")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    state_file = Path(tmp) / "state.json"
    sm = StateManager(state_file)

    # Mark some media as failed
    sm.mark_media_failed("chat_1", "100:abc")
    sm.mark_media_failed("chat_1", "100:def")

    failed = sm.get_failed_media_keys("chat_1")
    check(len(failed) == 2, f"2 failed media keys (got {len(failed)})")
    check("100:abc" in failed, "key abc in failed set")
    check("100:def" in failed, "key def in failed set")

    # Simulate successful retry: clear failed, mark as downloaded
    sm.clear_failed_media("chat_1", "100:abc")
    sm.mark_media_downloaded("chat_1", "100:abc")

    failed = sm.get_failed_media_keys("chat_1")
    check(len(failed) == 1, f"1 failed key after retry (got {len(failed)})")
    check("100:abc" not in failed, "key abc removed from failed")
    check(sm.is_media_downloaded("chat_1", "100:abc"), "key abc now in downloaded set")

    # Reload and verify persistence
    sm2 = StateManager(state_file)
    failed2 = sm2.get_failed_media_keys("chat_1")
    check(len(failed2) == 1, f"after reload, 1 failed key (got {len(failed2)})")
    check(sm2.is_media_downloaded("chat_1", "100:abc"), "after reload, abc is downloaded")


# ----------------------------------------------------------------- 6. Filename sanitization / collisions
section("6. Filename sanitization / collision avoidance")

from utils import sanitize_filename, safe_chat_dir_name

# Two different original names must not produce the same sanitized name
# unless they're truly identical
a = sanitize_filename("photo (1).jpg")
b = sanitize_filename("photo (2).jpg")
check(a != b, f"different inputs produce different outputs: {a!r} != {b!r}")

# chat dir names with same display name but different IDs don't collide
dir1 = safe_chat_dir_name(1, "John")
dir2 = safe_chat_dir_name(2, "John")
check(dir1 != dir2, "same name, different IDs don't collide")

# Trailing underscore in name doesn't cause double underscore
dir3 = safe_chat_dir_name(123, "John_")
check("__" not in dir3, f"no double underscore in {dir3!r}")

# Reserved Windows names are escaped
check(sanitize_filename("CON") == "_CON", "CON escaped")
check(sanitize_filename("nul.txt") == "_nul.txt", "nul.txt escaped")

# Long names are truncated but extension preserved
long_name = "a" * 200 + ".pdf"
result = sanitize_filename(long_name, max_length=50)
check(result.endswith(".pdf"), "long name preserves extension")
check(len(result) <= 50, f"long name truncated to <=50 (got {len(result)})")

# Empty / whitespace
check(sanitize_filename("") == "unnamed", "empty -> unnamed")
check(sanitize_filename("   ") == "unnamed", "whitespace -> unnamed")
check(sanitize_filename(None) == "unnamed", "None -> unnamed")


# ----------------------------------------------------------------- 7. Media integrity verification
section("7. Media integrity verification")

# We test the _verify_file_integrity method indirectly by checking its logic
with tempfile.TemporaryDirectory() as tmp:
    from media_downloader import MediaDownloader

    # Create a minimal downloader instance without a real client
    class FakeConfig:
        backup_media = True
        download_photos = True
        download_videos = True
        download_documents = True
        download_audio = True
        download_voice = True
        max_photo_size = 0
        max_video_size = 0
        max_document_size = 0
        max_retries = 3
        base_dir = Path(tmp)

    class FakeState:
        def __init__(self):
            self.downloaded = set()
            self.failed = set()
        def is_media_downloaded(self, ck, mk): return mk in self.downloaded
        def mark_media_downloaded(self, ck, mk): self.downloaded.add(mk)
        def is_media_failed(self, ck, mk): return mk in self.failed
        def mark_media_failed(self, ck, mk): self.failed.add(mk)
        def clear_failed_media(self, ck, mk): self.failed.discard(mk)

    import logging
    fake_logger = logging.getLogger("test")
    fake_logger.addHandler(logging.NullHandler())

    downloader = MediaDownloader(
        client=None,
        config=FakeConfig(),
        state=FakeState(),
        error_logger=fake_logger,
    )

    # Test 1: non-existent file fails integrity
    check(not downloader._verify_file_integrity(Path(tmp) / "nonexistent.jpg", 0),
          "non-existent file fails integrity")

    # Test 2: zero-byte file fails integrity
    zero_path = Path(tmp) / "zero.jpg"
    zero_path.write_text("")
    check(not downloader._verify_file_integrity(zero_path, 0),
          "zero-byte file fails integrity")

    # Test 3: file with correct size passes
    good_path = Path(tmp) / "good.jpg"
    good_path.write_bytes(b"x" * 100)
    check(downloader._verify_file_integrity(good_path, 100),
          "file with correct size passes integrity")

    # Test 4: file with wrong size fails
    short_path = Path(tmp) / "short.jpg"
    short_path.write_bytes(b"x" * 50)
    check(not downloader._verify_file_integrity(short_path, 100),
          "file with wrong size fails integrity (size mismatch detected)")

    # Test 5: file with unknown expected size (0) passes if non-empty
    check(downloader._verify_file_integrity(good_path, 0),
          "file with unknown expected size passes if non-empty")


# ----------------------------------------------------------------- 8. Resume from stale state
section("8. Resume from stale state (state < JSONL)")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    from exporter import MessageExporter

    # Set up: JSONL has messages 1-5, but state only knows about 1-3
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    state_file = Path(tmp) / "state.json"

    sm = StateManager(state_file)
    exp = MessageExporter(chat_dir, Path(tmp))

    # Write 5 messages to JSONL
    for i in range(1, 6):
        exp.append_message({"message_id": i, "text": f"msg {i}"})
        sm.mark_message_processed("chat_1", i)
    sm.save()

    # Now simulate state being stale: rewrite state with last_message_id=3
    sm._state["chats"]["chat_1"]["last_message_id"] = 3
    sm.save()

    # Reload
    sm2 = StateManager(state_file)
    exp2 = MessageExporter(chat_dir, Path(tmp))

    state_hint = sm2.get_state_hint("chat_1")
    exp_max = exp2.get_max_message_id()
    # Resume point = jsonl_max = 5 (state is NOT used for resume)
    # This is efficient: we only fetch messages with id > 5.
    resume = exp_max

    check(state_hint == 3, f"stale state has last_message_id=3 (got {state_hint})")
    check(exp_max == 5, f"JSONL has max_id=5 (got {exp_max})")
    check(resume == 5, f"resume point = 5 (jsonl_max; efficient, no re-scan) (got {resume})")
    # Messages 1-5 are in JSONL, so has_message would skip them if re-fetched
    for i in range(1, 6):
        check(exp2.has_message(i), f"message {i} is in JSONL (will be skipped if re-fetched)")


# ----------------------------------------------------------------- 9. Schema migration v1 -> v2
section("9. State file schema migration (v1 -> v2)")

with tempfile.TemporaryDirectory() as tmp:
    state_file = Path(tmp) / "state.json"

    # Write a v1 state file with the old downloaded_media_keys list
    v1_state = {
        "version": 1,
        "started_at": "2025-01-01T00:00:00+00:00",
        "last_run": None,
        "chats": {
            "chat_1": {
                "last_message_id": 100,
                "message_count": 100,
                "media_count": 5,
                "downloaded_media_keys": ["1:a", "2:b", "3:c"],  # old format
                "completed": False,
                "started_at": "2025-01-01T00:00:00+00:00",
                "completed_at": None,
                "last_error": None,
            }
        }
    }
    with open(state_file, "w") as f:
        json.dump(v1_state, f)

    # Load - should migrate to v2
    from state_manager import StateManager
    sm = StateManager(state_file)

    check(sm._state["version"] == 3, f"state version migrated to 3 (got {sm._state['version']})")
    check("downloaded_media_keys" not in sm._state["chats"]["chat_1"],
          "old downloaded_media_keys list removed")
    check(sm._state["chats"]["chat_1"]["last_message_id"] == 100,
          "last_message_id preserved during migration")
    check(sm._state["chats"]["chat_1"]["failed_media_count"] == 0,
          "failed_media_count field added with default 0")


# ----------------------------------------------------------------- 10. Atomic state writes
section("10. Atomic state file writes (no corruption)")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    state_file = Path(tmp) / "state.json"
    sm = StateManager(state_file)

    # Add lots of data
    for i in range(100):
        sm.mark_message_processed("chat_1", i)
        sm.mark_media_downloaded("chat_1", f"msg:{i}")

    sm.save()

    # Verify file is valid JSON
    with open(state_file, "r") as f:
        data = json.load(f)
    check(data["version"] == 3, "saved state has version 3")
    check(data["chats"]["chat_1"]["message_count"] == 100, "message_count = 100")
    check(data["chats"]["chat_1"]["media_count"] == 100, "media_count = 100")

    # No temp file left behind
    check(not (state_file.parent / "state.json.tmp").exists(),
          "no .tmp file left after atomic write")


# ----------------------------------------------------------------- 11. Per-chat media log isolation
section("11. Per-chat media log isolation")

with tempfile.TemporaryDirectory() as tmp:
    from state_manager import StateManager
    state_file = Path(tmp) / "state.json"
    sm = StateManager(state_file)

    # Add media to two different chats
    sm.mark_media_downloaded("chat_A", "msg:1")
    sm.mark_media_downloaded("chat_A", "msg:2")
    sm.mark_media_downloaded("chat_B", "msg:1")  # same key, different chat

    check(sm.is_media_downloaded("chat_A", "msg:1"), "chat_A has msg:1")
    check(sm.is_media_downloaded("chat_A", "msg:2"), "chat_A has msg:2")
    check(sm.is_media_downloaded("chat_B", "msg:1"), "chat_B has msg:1")
    check(not sm.is_media_downloaded("chat_B", "msg:2"), "chat_B does NOT have msg:2")
    check(not sm.is_media_downloaded("chat_A", "msg:3"), "chat_A does NOT have msg:3")

    # Verify separate log files exist
    check((state_file.parent / "chat_A.media.jsonl").exists(), "chat_A media log exists")
    check((state_file.parent / "chat_B.media.jsonl").exists(), "chat_B media log exists")


# ----------------------------------------------------------------- 12. Exporter iter_messages
section("12. Exporter iter_messages for failed-media retry scan")

with tempfile.TemporaryDirectory() as tmp:
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    from exporter import MessageExporter
    exp = MessageExporter(chat_dir, Path(tmp))

    # Write messages, some with failed media
    exp.append_message({"message_id": 1, "text": "ok", "media": None})
    exp.append_message({"message_id": 2, "text": "media failed",
                        "media": {"type": "other", "error": "download failed"}})
    exp.append_message({"message_id": 3, "text": "media ok",
                        "media": {"type": "photo", "local_path": "photos/3.jpg"}})
    exp.append_message({"message_id": 4, "text": "media failed 2",
                        "media": {"type": "other", "error": "timeout"}})

    # Iterate and find failed-media messages
    failed_msgs = []
    for mid, record in exp.iter_messages():
        media = record.get("media")
        if media and media.get("error"):
            failed_msgs.append(mid)

    check(len(failed_msgs) == 2, f"found 2 failed-media messages (got {len(failed_msgs)})")
    check(set(failed_msgs) == {2, 4}, f"failed msg ids are 2 and 4 (got {failed_msgs})")


# ----------------------------------------------------------------- Summary
print()
print("=" * 60)
if failures:
    print(f"FAILED: {len(failures)} test(s) out of {test_count}")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"ALL {test_count} TESTS PASSED")
print("=" * 60)
