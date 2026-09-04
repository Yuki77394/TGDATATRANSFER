"""Smoke tests for the telegram_backup_tool modules.

Runs without real Telegram credentials - just verifies the logic of:
  - Config loading
  - StateManager (resume + atomic save)
  - Utils (filename sanitization, chat dir naming, human_size)
  - Exporter (idempotent append + JSON rebuild)
"""
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

failures = []


def check(condition, label):
    if condition:
        print(f"  PASS: {label}")
    else:
        print(f"  FAIL: {label}")
        failures.append(label)


# ----------------------------------------------------------------- Config
print("Test: Config loading")
with tempfile.TemporaryDirectory() as tmp:
    tmp_path = Path(tmp)
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"API_ID=12345\n"
        f"API_HASH=fakehash\n"
        f"PHONE=+15551234567\n"
        f"BACKUP_SAVED_MESSAGES=true\n"
        f"BACKUP_PRIVATE_CHATS=true\n"
        f"BACKUP_GROUPS=false\n"
        f"BACKUP_CHANNELS=false\n"
        f"BACKUP_MEDIA=true\n"
        f"MAX_RETRIES=3\n"
        f"CHECKPOINT_EVERY=10\n"
        f"BACKUP_DIR={tmp}/backup\n"
    )

    from config import Config
    cfg = Config(env_path=env_file)
    cfg.validate()
    cfg.ensure_directories()
    check(cfg.api_id == 12345, "api_id parsed as int")
    check(cfg.api_hash == "fakehash", "api_hash parsed")
    check(cfg.phone == "+15551234567", "phone parsed")
    check(cfg.max_retries == 3, "max_retries parsed")
    check(cfg.base_dir.exists(), "base_dir created")
    check(cfg.saved_messages_dir.exists(), "saved_messages_dir created")
    check(not cfg.backup_groups, "backup_groups defaults false")


# ----------------------------------------------------------------- StateManager
print("\nTest: StateManager resume")
with tempfile.TemporaryDirectory() as tmp:
    state_file = Path(tmp) / "state.json"
    from state_manager import StateManager
    sm = StateManager(state_file)
    sm.mark_message_processed("private_123", 100)
    sm.mark_message_processed("private_123", 200)
    sm.mark_media_downloaded("private_123", "200:media_abc")
    sm.mark_chat_completed("private_123")
    sm.save()

    # Reload from disk and verify
    sm2 = StateManager(state_file)
    check(sm2.get_state_hint("private_123") == 200, "resume min_id = 200")
    check(sm2.is_media_downloaded("private_123", "200:media_abc"), "media marked downloaded")
    check(sm2.is_chat_completed("private_123"), "chat marked completed")
    check(not sm2.is_media_downloaded("private_123", "200:media_xyz"), "unknown media not downloaded")


# ----------------------------------------------------------------- Utils
print("\nTest: Utils (filename sanitization)")
from utils import sanitize_filename, safe_chat_dir_name, human_size, truncate_text

# Forbidden characters replaced with underscore
result = sanitize_filename("hello/world:foo*bar?.txt")
check(result == "hello_world_foo_bar.txt", f"forbidden chars replaced (got: {result!r})")

# Windows reserved name gets leading underscore
result = sanitize_filename("CON")
check(result == "_CON", f"CON escaped (got: {result!r})")

result = sanitize_filename("PRN.txt")
check(result == "_PRN.txt", f"PRN with ext escaped (got: {result!r})")

# Empty string
check(sanitize_filename("") == "unnamed", "empty -> unnamed")

# None-like
check(sanitize_filename("   ") == "unnamed", "whitespace -> unnamed")

# Long name truncation preserves extension
long_name = "a" * 200 + ".txt"
result = sanitize_filename(long_name, max_length=50)
check(result.endswith(".txt"), "long name preserves extension")
check(len(result) <= 50, f"long name truncated to <=50 (got {len(result)})")

# Chat dir name includes chat ID
result = safe_chat_dir_name(12345, "Rahul Kumar")
check(result == "Rahul Kumar_12345", f"safe_chat_dir_name (got: {result!r})")

# Two chats with same name don't collide
a = safe_chat_dir_name(1, "John")
b = safe_chat_dir_name(2, "John")
check(a != b, "same-name chats with different IDs don't collide")

# human_size
check(human_size(0) == "0 B", "human_size(0)")
check(human_size(1023) == "1023 B", "human_size(1023)")
check(human_size(1024) == "1.0 KB", "human_size(1024)")
check(human_size(1024 * 1024) == "1.0 MB", "human_size(1MB)")
check(human_size(None) == "unknown", "human_size(None)")

# truncate_text
check(truncate_text("hello", 10) == "hello", "truncate short text unchanged")
check(truncate_text("hello world", 5) == "he...", "truncate long text")


# ----------------------------------------------------------------- Exporter
print("\nTest: Exporter (idempotent JSONL + JSON rebuild)")
with tempfile.TemporaryDirectory() as tmp:
    chat_dir = Path(tmp) / "chat"
    chat_dir.mkdir()
    from exporter import MessageExporter
    exp = MessageExporter(chat_dir, Path(tmp))
    exp.append_message({"message_id": 1, "text": "first"})
    exp.append_message({"message_id": 2, "text": "second"})
    # Duplicate should be skipped
    exp.append_message({"message_id": 1, "text": "DUPLICATE"})
    check(exp.count() == 2, "duplicate message_id skipped")
    check(exp.get_max_message_id() == 2, "max_id tracked correctly")
    check(exp.has_message(1), "has_message(1) True")
    check(not exp.has_message(99), "has_message(99) False")

    # Rebuild JSON
    count = exp.write_json()
    check(count == 2, "write_json returns count")

    # Verify JSON file structure
    import json
    with open(chat_dir / "messages.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    check("messages" in data, "messages.json has 'messages' key")
    check(data["count"] == 2, "messages.json count matches")
    check(data["messages"][0]["message_id"] == 1, "messages sorted by id ascending")

    # Reload exporter from disk - simulates resume
    exp2 = MessageExporter(chat_dir, Path(tmp))
    check(exp2.count() == 2, "reload: count matches")
    check(exp2.get_max_message_id() == 2, "reload: max_id matches")
    check(exp2.has_message(1), "reload: has_message works")


# ----------------------------------------------------------------- Summary
print()
if failures:
    print(f"FAILED: {len(failures)} test(s)")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print("ALL SMOKE TESTS PASSED")
