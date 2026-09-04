# Telegram Backup Tool

A **read-only**, **local** backup tool for your own Telegram account, built on
[Telethon](https://github.com/LonamiWebs/Telethon).

The tool backs up your **Saved Messages** and your **private (1-on-1) chats**,
including text, photos, videos, documents, voice messages, audio files,
stickers, and captions. It is designed to be **resumable** (interrupt it any
time and rerun it to continue) and **polite** (it respects Telegram's
`FloodWait` rate limits and never tries to bypass them).

**Version 2.0** — production-audited for crash safety, data integrity, and
large-account performance. See the [Crash Safety](#crash-safety--resume-design)
section for the full design.

---

## Safety & ethics

This tool:

- Only accesses **your own** Telegram account that you have legitimately
  logged in to.
- **Does not** bypass OTP, 2FA, account freezes, or rate limits.
- **Does not** access accounts that do not belong to you.
- **Does not** upload any backup data to external servers — everything stays
  on your local disk.
- **Does not** log API hashes, OTP codes, or 2FA passwords.
- **Does not** attempt to export secret chats (Telegram does not expose them
  via the API, and we do not try to circumvent that).

If your account is frozen or restricted by Telegram, the tool will simply
fail on the restricted operation and continue with what it can read. It will
**not** try to unfreeze the account.

---

## Crash safety & resume design

The backup is designed so that **no message can be silently lost**, regardless
of when the tool is interrupted (crash, power loss, network drop, FloodWait,
SIGTERM, Ctrl+C, etc.).

### Five invariants

**INVARIANT 1: `messages.jsonl` is the authoritative source of truth.**

The set of message_ids present in `messages.jsonl` for a chat defines what
has been successfully exported. `state.last_message_id` is ONLY a performance
hint and may be stale, corrupted, or ahead of the JSONL.

**INVARIANT 2: Resume point = `jsonl_max` (the highest message_id in JSONL).**

On restart, we compute:
```
state_hint  = state.last_message_id  # may be stale
jsonl_max   = max(message_id in messages.jsonl)
resume_from = jsonl_max              # safe AND efficient
```

We resume from `jsonl_max`, NOT `min(state, jsonl_max)` and NOT `max(state, jsonl_max)`. This is both SAFE and EFFICIENT:

- **SAFE:** Every message with id <= jsonl_max is in one of two states:
  (a) Present in messages.jsonl → already exported, `has_message()` will skip it if re-fetched.
  (b) Present in failed_messages.jsonl → will be retried by `_retry_failed_messages()` BEFORE this main loop runs.
  No message below jsonl_max can be silently skipped.
- **EFFICIENT:** `iter_messages(min_id=jsonl_max)` only fetches messages with id > jsonl_max — i.e., only genuinely NEW messages. We do NOT re-fetch the entire chat history on every run.

`state_hint` is NOT used for resume — only as a sanity check. If `state_hint > jsonl_max`, we log a warning but still resume from `jsonl_max`.

**INVARIANT 3: A failed message STOPS the chat.**

If processing message N fails for any non-FloodWait reason:
- The message ID is recorded in `failed_messages.jsonl`.
- State's `last_message_id` is NOT advanced past N.
- Processing of this chat **STOPS** (we do NOT continue to N+1).
- On the next run, `_retry_failed_messages()` retries N BEFORE the main loop.

This prevents the resume cursor from advancing past a failed message and
permanently skipping it.

Exception: **FloodWait is NOT a failure.** We sleep and retry the SAME
message repeatedly until it succeeds or a non-FloodWait error occurs.

**INVARIANT 4: Message is marked processed ONLY after JSONL append succeeds.**

Order of operations per message:
1. Download media (if any). On media failure, record `media.error` in the
   message record and add to `failed_media` set — but the message itself
   is still exported (media failure ≠ message failure).
2. Build the message record.
3. Append to `messages.jsonl` (atomic + `fsync`).
4. ONLY THEN call `state.mark_message_processed(id)`.

**INVARIANT 5: Failed messages and failed media are tracked separately.**

- `failed_messages.jsonl` — message IDs that failed to export.
  Retried by `_retry_failed_messages()` before the main loop.
- `failed_media.jsonl` — media keys that failed to download.
  Retried by `_retry_failed_media()` before the main loop.

A message with failed media is STILL written to JSONL (with `media.error`
set). The message is considered "exported" — only its media needs retry.

### Completed chats and incremental scanning

A "completed" chat does NOT mean "never process this chat again." It means:

> All messages known during the previous run were processed successfully.

On every future run, completed chats are still scanned for **new messages**
that arrived since the last backup. This is done efficiently:

- `resume_from = jsonl_max` (the highest message_id already in the JSONL)
- `iter_messages(min_id=jsonl_max)` only fetches messages with id > jsonl_max
- If no new messages exist, the loop exits immediately (no re-scanning)
- Already-exported messages are never re-downloaded or duplicated

This supports **incremental backups**: run the tool once to do the initial
backup, then run it again periodically to pick up only new messages.

### Failed-message retry semantics

When `_retry_failed_messages()` re-fetches a failed message from Telegram:

- **Successfully exported** → removed from `failed_messages.jsonl`.
- **Still fails (non-FloodWait exception)** → kept in `failed_messages.jsonl`, retry count incremented.
- **`get_messages` returns `None`** → this COULD mean the message was deleted, OR it could be a transient API issue. The retry count is incremented and the message is KEPT in the failed set. Only after **5 consecutive `None` returns** is the message considered "confirmed unavailable" (likely deleted) and removed from the failed set, with an explicit log entry in `errors.log`.

This ensures a temporary network/API failure does NOT cause a failed message to be silently forgotten.

### JSONL corruption recovery

The exporter handles three corruption scenarios:

1. **Partial trailing line** (crash mid-write): the line fails JSON parsing.
   It is excluded from the in-memory set. Before the next append, the partial
   line is **physically truncated** from the file.

2. **Malformed line in the middle** (rare — disk corruption): the bad line
   is logged with a WARNING, skipped, but iteration **CONTINUES**. All
   subsequent valid records are loaded into memory. The bad line is NOT
   removed automatically (to avoid destroying data). Call `compact_jsonl()`
   to rewrite the file dropping bad lines.

3. **Duplicate message_ids** (from a buggy old version): the LAST occurrence
   wins. `compact_jsonl()` removes duplicates.

### Media integrity verification

A file is only considered "successfully downloaded" if ALL of:

- The file exists on disk.
- The file has non-zero size.
- If Telegram reported an expected size, the file size matches exactly.

If a partial file is left on disk (e.g. download interrupted mid-write), it
is **deleted before retrying**. This prevents partial files from being
silently treated as complete.

---

## Folder structure

```
telegram_backup/
├── Saved_Messages/
│   ├── chat_info.json
│   ├── messages.json          ← full snapshot (rebuilt each run)
│   ├── messages.jsonl         ← append-only, primary source of truth
│   └── media/
│       ├── photos/
│       ├── videos/
│       ├── documents/
│       ├── audio/
│       └── voice/
├── Private_Chats/
│   └── <SanitizedName>_<chat_id>/
│       ├── chat_info.json
│       ├── messages.json
│       ├── messages.jsonl
│       └── media/
│           └── ...
├── backup_metadata.json       ← summary of the last run
├── backup_state.json          ← resume state (hint only; JSONL is source of truth)
├── saved_messages.media.jsonl       ← downloaded media keys (one per line)
├── saved_messages.failed_media.jsonl ← failed media keys (for retry)
├── saved_messages.failed_messages.jsonl ← failed message IDs (for retry)
├── private_<id>.media.jsonl         ← per-chat media key logs
├── private_<id>.failed_media.jsonl
├── private_<id>.failed_messages.jsonl
├── backup.log                 ← verbose log
└── errors.log                 ← errors only
```

If you enable `BACKUP_GROUPS=true` or `BACKUP_CHANNELS=true`, additional
`Groups/` and `Channels/` folders will be created.

### Filename sanitization

Chat folder names are sanitized to be safe on **Windows, Linux, and Android**:

- Forbidden characters (`\ / : * ? " < > |` and control chars) → `_`
- Windows reserved names (`CON`, `PRN`, `NUL`, `COM1..`, `LPT1..`) → prefixed
- Trailing dots and spaces are stripped (Windows quirk)
- Length is capped (with the file extension preserved)
- The numeric chat ID is appended to the folder name to prevent collisions
  between chats that happen to have the same display name

---

## Installation

### 1. Get your Telegram API credentials

Go to <https://my.telegram.org> → **API development tools**, log in with your
phone number, and create an application. You will receive:

- `api_id` (a number)
- `api_hash` (a long string)

> These are **app** credentials, not account credentials. They identify your
> script to Telegram. Still, treat them as private — anyone with them can
> impersonate your script.

### 2. Install Python dependencies

Requires **Python 3.9+** (tested on 3.11 and 3.12).

```bash
cd telegram_backup_tool
python -m venv .venv
source .venv/bin/activate         # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`cryptg` is included to speed up Telethon's encryption — it is optional but
recommended.

### 3. Configure the tool

```bash
cp .env.example .env
# Edit .env and fill in your API_ID, API_HASH, and PHONE
```

`.env` contents:

```ini
API_ID=123456
API_HASH=your_api_hash_here
PHONE=+10000000000

BACKUP_SAVED_MESSAGES=true
BACKUP_PRIVATE_CHATS=true
BACKUP_GROUPS=false
BACKUP_CHANNELS=false

BACKUP_MEDIA=true
DOWNLOAD_PHOTOS=true
DOWNLOAD_VIDEOS=true
DOWNLOAD_DOCUMENTS=true
DOWNLOAD_AUDIO=true
DOWNLOAD_VOICE=true

MAX_RETRIES=3
CHECKPOINT_EVERY=50
BACKUP_DIR=telegram_backup
```

> **Never commit `.env` to version control.** A `.gitignore` is recommended.

---

## Running the backup

```bash
python main.py
```

### First run

On the first run, the tool will:

1. Connect to Telegram.
2. Send an OTP code to your Telegram app (and/or SMS).
3. Prompt you for the code.
4. If you have 2FA enabled, prompt for your cloud password (via `getpass`,
   so it is not echoed to the terminal).
5. Save a Telethon session file (`telegram_backup/telegram_backup_session.session`)
   so subsequent runs don't need to re-authenticate.

### Subsequent runs

If the session file is still valid, the tool will skip the OTP/2FA prompts
and resume the backup from where it left off.

### Resuming after interruption

The tool checkpoints its progress to `backup_state.json` every 50 messages
(configurable via `CHECKPOINT_EVERY`) and at the end of each chat. If you
interrupt it (Ctrl+C, kill, power loss, network drop, FloodWait timeout),
just rerun:

```bash
python main.py
```

It will:

- Skip messages that are already in `messages.jsonl` (idempotent)
- Skip media that has already been downloaded (tracked by media ID + checked
  on disk)
- Resume iterating from the highest message ID it has seen per chat

### Sample terminal output

```
[1/245] Processing: Saved Messages
  Resuming from message id > 5420 (5420 already exported)
  Messages scanned: 100 | Media downloaded: 12 | Current msg_id: 5520
    Latest text: ...
  Messages scanned: 200 | Media downloaded: 25 | Current msg_id: 5620
    Latest text: ...
  Done. Scanned 421 new message(s); chat total now 5841.

[2/245] Processing: Rahul
  Messages scanned: 100 | Media downloaded: 4 | Current msg_id: ...
  ...

============================================================
  BACKUP COMPLETE
============================================================
  Chats processed:    245
  Messages exported:  83210
  Photos downloaded:  4102
  Videos downloaded:  318
  Files downloaded:   941
  Voice downloaded:   221
  Audio downloaded:   67
  Failed items:       3
============================================================
```

---

## Configuration reference

| Variable                 | Default     | Description                                                              |
|--------------------------|-------------|--------------------------------------------------------------------------|
| `API_ID`                 | —           | Telegram app ID from my.telegram.org (required).                         |
| `API_HASH`               | —           | Telegram app hash (required, never logged).                              |
| `PHONE`                  | —           | Your phone number in international format (required).                    |
| `SESSION_NAME`           | `telegram_backup_session` | Name of the Telethon session file (local only).             |
| `SESSION_STRING`         | —           | Telethon StringSession (for Heroku/cloud). Preferred over file session.  |
| `TG_OTP_CODE`            | —           | OTP code for non-interactive first-run auth (Heroku fallback).           |
| `TG_2FA_PASSWORD`        | —           | 2FA password for non-interactive auth (less secure; prefer SESSION_STRING). |
| `BACKUP_SAVED_MESSAGES`  | `true`      | Back up your Saved Messages.                                             |
| `BACKUP_PRIVATE_CHATS`   | `true`      | Back up 1-on-1 private chats.                                            |
| `BACKUP_GROUPS`          | `false`     | Also back up small group chats (opt-in).                                 |
| `BACKUP_CHANNELS`        | `false`     | Also back up channels and supergroups (opt-in).                          |
| `INCLUDE_BOTS`           | `true`      | Include bot conversations in private chat backup.                       |
| `BACKUP_MEDIA`           | `true`      | Download media files (master switch).                                    |
| `DOWNLOAD_PHOTOS`        | `true`      | Download photo messages.                                                 |
| `DOWNLOAD_VIDEOS`        | `true`      | Download video messages (including GIFs).                                |
| `DOWNLOAD_DOCUMENTS`     | `true`      | Download documents and files.                                            |
| `DOWNLOAD_AUDIO`         | `true`      | Download audio files.                                                    |
| `DOWNLOAD_VOICE`         | `true`      | Download voice messages and round video notes.                           |
| `MAX_PHOTO_SIZE_MB`      | `0`         | Skip photos larger than this (0 = no limit).                             |
| `MAX_VIDEO_SIZE_MB`      | `0`         | Skip videos larger than this (0 = no limit).                             |
| `MAX_DOCUMENT_SIZE_MB`   | `0`         | Skip documents larger than this (0 = no limit).                          |
| `MAX_RETRIES`            | `3`         | Number of retries for failed media downloads / network errors.           |
| `CHECKPOINT_EVERY`       | `50`        | Save state to disk every N messages.                                     |
| `BACKUP_DIR`             | `telegram_backup` | Output directory (relative to the project root or absolute).       |

---

## Heroku deployment

The tool can run as a **Heroku Worker** process. The `Procfile` defines:

```
worker: python main.py
```

### ⚠️ Ephemeral filesystem limitation

**Heroku's dyno filesystem is EPHEMERAL.** All files — including backup
output, state, logs, and the Telethon session file — are **lost on every
dyno restart or redeploy** (at least once every 24 hours).

This means:
- **Backups are NOT persistent on Heroku by default.** The tool will run,
  download messages/media, and write them to the dyno filesystem — but they
  will disappear on restart.
- **Resume state is also lost**, so every restart starts from scratch
  (re-downloading everything).
- **The file-based Telethon session is lost**, requiring re-authentication
  every restart (which is impossible without an interactive terminal).

To work around this:
1. Use `SESSION_STRING` (see below) — this keeps the auth in an env var
   that survives restarts.
2. For persistent backups, you must integrate an external storage service
   (e.g., S3, Google Drive, Dropbox). This is NOT currently implemented;
   the tool writes only to the local filesystem.
3. For local persistent backups, run the tool on your own machine instead
   of Heroku.

### Prerequisites

1. A Heroku account (free or paid).
2. Your Telegram API credentials from https://my.telegram.org.
3. The `SESSION_STRING` (generated locally — see below).

### Step 1: Generate a session string locally

Because Heroku has no interactive terminal, you must generate a Telethon
session string on your local machine first:

```bash
cd telegram_backup_tool
cp .env.example .env
# Edit .env: fill in API_ID, API_HASH, PHONE
pip install -r requirements.txt
python generate_session.py
```

The script will prompt for your OTP code (and 2FA password if enabled),
then print a session string. **Copy it** — it will be used as a Heroku
Config Var.

> **Security:** The session string grants full access to your Telegram
> account. Treat it like a password. Never commit it to git.

### Step 2: Create a Heroku app

```bash
heroku create my-telegram-backup
```

Or via the Heroku Dashboard: https://dashboard.heroku.com/new-app

### Step 3: Set Config Vars

```bash
heroku config:set API_ID=your_api_id
heroku config:set API_HASH=your_api_hash
heroku config:set PHONE=+10000000000
heroku config:set SESSION_STRING='the-session-string-you-generated'
```

Optional Config Vars (see Configuration reference above for all options):

```bash
heroku config:set BACKUP_DIR=/tmp/telegram_backup
heroku config:set BACKUP_MEDIA=true
heroku config:set DOWNLOAD_VIDEOS=true
```

### Step 4: Deploy the code

**Option A — Connect GitHub repo (recommended):**

1. Push the project to a GitHub repository.
2. In the Heroku Dashboard: Settings → Deploy → Deployment method → GitHub.
3. Connect the repository and enable Automatic Deploys (optional).
4. Deploy the `main` branch.

**Option B — Heroku Git:**

```bash
heroku git:remote -a my-telegram-backup
git push heroku main
```

### Step 5: Start the Worker dyno

```bash
# Scale up the worker (starts it)
heroku ps:scale worker=1

# Check dyno status
heroku ps
```

### Step 6: View logs

```bash
heroku logs --tail
```

You should see startup messages like:

```
[INFO] Starting Telegram backup tool.
[INFO] Configuration: {'is_heroku': True, 'session_string_set': True, ...}
[WARNING] Running on Heroku: the dyno filesystem is EPHEMERAL...
[INFO] Authenticated as user id=... (Your Name)
[INFO] Discovered N chats to back up.
```

### Step 7: Stop / restart the Worker

```bash
# Stop the worker (sends SIGTERM, tool saves state and exits gracefully)
heroku ps:scale worker=0

# Restart (scales down then up)
heroku ps:restart worker
```

Heroku sends `SIGTERM` ~30 seconds before forcing `SIGKILL`. The tool
handles `SIGTERM` by saving state and disconnecting cleanly.

### First-time Telegram authentication

**You cannot do interactive OTP/2FA on Heroku** — there's no terminal.

Two options:

1. **Preferred — `SESSION_STRING`:** Generate locally with
   `python generate_session.py` and set as a Config Var. This contains
   the full auth state and survives restarts.

2. **Fallback — env-var auth:** Set `TG_OTP_CODE` (and `TG_2FA_PASSWORD`
   if needed) as Config Vars. The tool will use them for non-interactive
   first-time auth. **This is less secure** because the OTP code must be
   set before the Telegram login code expires, and the 2FA password
   would be stored in plaintext Config Vars. After the first successful
   run, the session file is created — but it's lost on restart, so
   you'd need to set the OTP again. **Prefer `SESSION_STRING`.**

---

## Message format

Each message is stored as one JSON object per line in `messages.jsonl` and
as an array in `messages.json`. Example record:

```json
{
  "message_id": 123,
  "date": "2026-09-04T12:30:00+00:00",
  "sender_id": 123456789,
  "sender_username": "example_user",
  "sender_name": "Example User",
  "text": "Example message",
  "reply_to_message_id": null,
  "reply_to_top_id": null,
  "is_reply": false,
  "forwarded_from": null,
  "action": null,
  "media": {
    "type": "photo",
    "local_path": "Saved_Messages/media/photos/IMG_20260904_msg123.jpg",
    "file_size": 248320
  },
  "edited_date": null,
  "post": false,
  "via_bot_id": null,
  "ttl_seconds": null
}
```

For messages with no media, the `"media"` field is `null`. For service
messages (e.g. "user joined the group"), `"action"` is set and `"text"` is
typically empty.

---

## Error handling

- **`FloodWaitError`**: the tool prints the requested wait time, sleeps, and
  resumes automatically. It does **not** try to bypass the rate limit.
- **Network errors**: retried with exponential backoff up to `MAX_RETRIES`.
- **Per-message failures**: logged to `errors.log` (including chat ID and
  message ID) and skipped — the run continues.
- **Per-chat failures**: logged; the orchestrator moves on to the next chat.
- **Authentication failures**: abort the run (you cannot continue without
  a valid session).

`errors.log` lines look like:

```
2026-09-04 12:30:01 [ERROR] Media download failed for chat_key=private_123456 message_id=98765: FileLoadError: ...
```

---

## Architecture

```
telegram_backup_tool/
├── main.py                ← entry point: load config, auth, run backup
├── auth.py                ← Telethon client + OTP/2FA prompts (no secrets logged)
├── backup.py              ← orchestrator: chat discovery, message iteration, FloodWait handling
├── exporter.py            ← JSONL append + JSON rebuild (idempotent on resume)
├── media_downloader.py    ← categorize + download media with dedup & retries
├── state_manager.py       ← atomic state file for resume support
├── utils.py               ← filename sanitization, logging, safe JSON writes
├── config.py              ← .env loader + Config object
├── requirements.txt
├── .env.example
└── README.md
```

**Module responsibilities** (single-responsibility per module):

- `config.py` — knows nothing about Telegram; just loads `.env` into a typed
  `Config` object.
- `auth.py` — only handles authentication; knows nothing about the backup
  loop.
- `state_manager.py` — only persists resume state; knows nothing about
  Telegram or media.
- `media_downloader.py` — only downloads media for a single message; knows
  nothing about chat iteration.
- `exporter.py` — only writes message records to disk; knows nothing about
  how messages were fetched.
- `backup.py` — the orchestrator that wires the above together.
- `main.py` — process entry point, signal handling, top-level error handling.

---

## Troubleshooting

**"Configuration error: Missing required configuration: API_ID, API_HASH, PHONE"**
You didn't fill in your `.env` file. Copy `.env.example` to `.env` and edit it.

**"This phone number is banned from Telegram."**
Telegram has banned this number. The tool will not try to bypass this — you
cannot use it with a banned number.

**The session was lost / I want to log in fresh.**
Delete `telegram_backup/telegram_backup_session.session` and rerun `main.py`.

**A specific chat keeps failing.**
Check `errors.log` for details. You can mark the chat as not-yet-completed by
deleting its entry from `backup_state.json` (or just delete the whole state
file to start fresh — but you'll lose resume state for all chats).

**Disk space.**
Media downloads can be large. Use `MAX_VIDEO_SIZE_MB` and similar to skip
big files. The current run's totals are printed at the end.

**Large accounts.**
For accounts with hundreds of thousands of messages, the first run may take
hours. Just let it run — checkpoint frequency is 50 messages by default, so
at most 50 messages of work is lost on a crash. Subsequent runs are fast
because they skip already-processed messages.

---

## License & disclaimer

This tool is provided as-is, for personal backup of your **own** Telegram
account. You are responsible for complying with Telegram's Terms of Service
and any applicable local laws. Do not use this tool to access data that does
not belong to you.
