from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from difflib import get_close_matches
from threading import Thread
from typing import Any, Optional

import gspread
from flask import Flask
from google.oauth2.service_account import Credentials
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB").strip()
LOG_SHEET_NAME = os.environ.get("LOG_SHEET_NAME", "Logs").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID is missing or invalid")

# ================= PERFORMANCE TUNING =================

DELIVERY_WORKERS = max(1, int(os.environ.get("DELIVERY_WORKERS", "4")))
SEND_CONCURRENCY = max(1, int(os.environ.get("SEND_CONCURRENCY", "6")))
BOOK_CACHE_TTL = int(os.environ.get("BOOK_CACHE_TTL", "300"))
CALLBACK_CACHE_TTL = int(os.environ.get("CALLBACK_CACHE_TTL", "600"))
LOG_FLUSH_INTERVAL = int(os.environ.get("LOG_FLUSH_INTERVAL", "8"))
USER_FLUSH_INTERVAL = int(os.environ.get("USER_FLUSH_INTERVAL", "8"))
BOOK_REFRESH_INTERVAL = int(os.environ.get("BOOK_REFRESH_INTERVAL", "300"))
MAX_INLINE_SUGGESTIONS = 10
MAX_DIRECT_SEND = 5
MAX_BATCH_LOGS = 100
MAX_BATCH_USERS = 50

# ================= APP STATE =================

LOG_QUEUE: list[list[str]] = []
NEW_USER_QUEUE: set[int] = set()

_cached_book_sheet = None
_cached_user_sheet = None
_cached_log_sheet = None
_sheets_lock = asyncio.Lock()
_book_cache_lock = asyncio.Lock()
_callback_cache_lock = asyncio.Lock()

_callback_cache: dict[str, dict[str, Any]] = {}
_admin_reply_map: dict[int, int] = {}
_chat_send_locks: dict[int, asyncio.Lock] = {}

DELIVERY_QUEUE: Optional[asyncio.Queue] = None
DELIVERY_STARTED = False
SEND_GATE = asyncio.Semaphore(SEND_CONCURRENCY)

BOOK_CACHE = {
    "ts": 0.0,
    "rows": [],
    "indexed": [],
    "titles": [],
    "lookup": {},
}

USER_CACHE = {
    "ts": 0.0,
    "ids": set(),
}

# ================= FLASK (Render keep-alive) =================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    return "Library Bot is Running!"


def run_web():
    port = int(os.environ.get("PORT", 8080))
    import logging as flask_logging

    log = flask_logging.getLogger("werkzeug")
    log.setLevel(flask_logging.ERROR)
    web_app.run(host="0.0.0.0", port=port, use_reloader=False)


# ================= LOGGING =================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ================= SHEETS CONNECTION =================


def _build_credentials() -> Credentials:
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_file("creds.json", scopes=scope)


async def get_sheets():
    global _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    async with _sheets_lock:
        if _cached_book_sheet is not None and _cached_user_sheet is not None and _cached_log_sheet is not None:
            return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

        try:
            client = await asyncio.to_thread(gspread.authorize, _build_credentials())
            spreadsheet = await asyncio.to_thread(client.open, SHEET_NAME)

            def get_or_create(title: str, cols: int = 10):
                try:
                    return spreadsheet.worksheet(title)
                except Exception:
                    return spreadsheet.add_worksheet(title=title, rows=5000, cols=cols)

            if _cached_book_sheet is None:
                _cached_book_sheet = get_or_create("Sheet1", cols=6)
            if _cached_user_sheet is None:
                _cached_user_sheet = get_or_create("Users", cols=2)
            if _cached_log_sheet is None:
                _cached_log_sheet = get_or_create(LOG_SHEET_NAME, cols=10)
                try:
                    values = await asyncio.to_thread(_cached_log_sheet.get_all_values)
                    if not values:
                        await asyncio.to_thread(
                            _cached_log_sheet.append_row,
                            [
                                "timestamp",
                                "event",
                                "user_id",
                                "username",
                                "chat_id",
                                "query",
                                "normalized_query",
                                "status",
                                "matched_title",
                                "note",
                            ],
                        )
                except Exception:
                    pass

            return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

        except Exception as e:
            logging.error(f"Google Sheet Auth Error: {e}")
            return None, None, None


# ================= BACKGROUND FLUSHERS =================


async def flush_logs_once():
    _, _, log_sheet = await get_sheets()
    if not log_sheet or not LOG_QUEUE:
        return

    batch_logs = LOG_QUEUE[:MAX_BATCH_LOGS]
    del LOG_QUEUE[:MAX_BATCH_LOGS]

    try:
        await asyncio.to_thread(log_sheet.append_rows, batch_logs, value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"Batch Log error: {e}")
        LOG_QUEUE[:0] = batch_logs


async def flush_users_once():
    _, user_sheet, _ = await get_sheets()
    if not user_sheet or not NEW_USER_QUEUE:
        return

    users_to_add = list(NEW_USER_QUEUE)[:MAX_BATCH_USERS]
    rows_to_add = [[str(uid)] for uid in users_to_add]
    for uid in users_to_add:
        NEW_USER_QUEUE.discard(uid)

    try:
        await asyncio.to_thread(user_sheet.append_rows, rows_to_add, value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"Batch User error: {e}")
        NEW_USER_QUEUE.update(users_to_add)


async def background_sync_task():
    while True:
        await asyncio.sleep(min(LOG_FLUSH_INTERVAL, USER_FLUSH_INTERVAL))
        await flush_logs_once()
        await flush_users_once()


async def refresh_book_cache_periodically():
    while True:
        await asyncio.sleep(BOOK_REFRESH_INTERVAL)
        await get_book_cache(force=True)


async def cleanup_callback_cache_periodically():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        async with _callback_cache_lock:
            expired = [k for k, v in _callback_cache.items() if now - float(v.get("ts", 0.0)) > CALLBACK_CACHE_TTL]
            for key in expired:
                _callback_cache.pop(key, None)


# ================= CACHE HELPERS =================


def invalidate_book_cache():
    BOOK_CACHE["ts"] = 0.0


async def get_book_cache(force: bool = False):
    now = time.time()
    if (not force) and BOOK_CACHE["rows"] and (now - BOOK_CACHE["ts"] < BOOK_CACHE_TTL):
        return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

    async with _book_cache_lock:
        now = time.time()
        if (not force) and BOOK_CACHE["rows"] and (now - BOOK_CACHE["ts"] < BOOK_CACHE_TTL):
            return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

        book_sheet, _, _ = await get_sheets()
        if not book_sheet:
            return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

        try:
            values = await asyncio.to_thread(book_sheet.get_all_values)
            rows = values[1:] if len(values) > 1 else []
            rows = [r for r in rows if r and len(r) >= 2 and str(r[0]).strip() and str(r[1]).strip()]

            indexed, titles, lookup = build_search_index(rows)

            BOOK_CACHE["rows"] = rows
            BOOK_CACHE["indexed"] = indexed
            BOOK_CACHE["titles"] = titles
            BOOK_CACHE["lookup"] = lookup
            BOOK_CACHE["ts"] = now
            return rows, indexed, titles, lookup
        except Exception as e:
            logging.error(f"Book cache load error: {e}")
            return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]


async def load_users_initial():
    _, user_sheet, _ = await get_sheets()
    if not user_sheet:
        return
    try:
        values = await asyncio.to_thread(user_sheet.col_values, 1)
        ids = {int(str(x).strip()) for x in values[1:] if str(x).strip().isdigit()}
        USER_CACHE["ids"] = ids
        USER_CACHE["ts"] = time.time()
    except Exception as e:
        logging.error(f"User load error: {e}")


# ================= TEXT UTIL =================


def normalize(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    return re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)


def tokenize(text):
    return re.findall(r"[\w\u0980-\u09FF]+", str(text).lower(), flags=re.UNICODE)


def clean_title_like_text(text):
    text = str(text).strip()
    text = re.sub(r"^[\-\*\d\.\)\s]+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def is_url_like(text):
    return bool(re.search(r"(https?://|www\.|t\.me/|telegram\.me/)", str(text), flags=re.I))


def remove_urls(text):
    return re.sub(r"(https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+)", "", str(text), flags=re.I)


# ================= AUTO ALIAS GENERATION =================

BENGALI_LATIN_MAP = {
    "অ": "a", "আ": "a", "ই": "i", "ঈ": "i", "উ": "u", "ঊ": "u", "ঋ": "ri", "এ": "e", "ঐ": "oi", "ও": "o", "ঔ": "ou",
    "ক": "k", "খ": "kh", "গ": "g", "ঘ": "gh", "ঙ": "ng", "চ": "ch", "ছ": "chh", "জ": "j", "ঝ": "jh", "ঞ": "ny",
    "ট": "t", "ঠ": "th", "ড": "d", "ঢ": "dh", "ণ": "n", "ত": "t", "থ": "th", "দ": "d", "ধ": "dh", "ন": "n",
    "প": "p", "ফ": "f", "ব": "b", "ভ": "bh", "ম": "m", "য": "y", "র": "r", "ল": "l", "শ": "sh", "ষ": "sh",
    "স": "s", "হ": "h", "ড়": "r", "ঢ়": "rh", "য়": "y", "ং": "ng", "ঃ": "h", "ঁ": "n", "ৎ": "t",
    "া": "a", "ি": "i", "ী": "i", "ু": "u", "ূ": "u", "ে": "e", "ৈ": "oi", "ো": "o", "ৌ": "ou", "্": "",
}


def transliterate_bengali_to_latin(text):
    if not text:
        return ""
    out = [BENGALI_LATIN_MAP.get(ch, ch) for ch in str(text)]
    result = "".join(out)
    result = re.sub(r"[^a-zA-Z0-9\s]+", " ", result)
    return re.sub(r"\s+", " ", result).strip().lower()


def generate_auto_aliases(title):
    aliases = set()
    if not title:
        return aliases

    raw = str(title).strip()
    raw_lower = raw.lower()

    variants = {raw_lower, raw, normalize(raw), normalize(raw.replace(" ", ""))}
    volume_removed = re.sub(r"(খণ্ড|খন্ড|volume|vol\.?|v\.?)\s*\d*", "", raw_lower, flags=re.I)
    variants.add(normalize(volume_removed))

    tokens = [normalize(t) for t in tokenize(raw) if normalize(t) and len(normalize(t)) >= 2]
    variants.update(tokens)

    if len(tokens) >= 2:
        variants.add("".join(tokens))
        for i in range(len(tokens) - 1):
            pair = tokens[i] + tokens[i + 1]
            if len(pair) >= 3:
                variants.add(pair)

    latin = transliterate_bengali_to_latin(raw)
    if latin:
        variants.update({latin, normalize(latin), normalize(latin.replace(" ", ""))})
        latin_tokens = [normalize(t) for t in tokenize(latin) if len(normalize(t)) >= 2]
        variants.update(latin_tokens)

    return {v for v in variants if v and len(v) >= 2}


def parse_manual_aliases(value):
    aliases = set()
    if not value:
        return aliases
    for part in re.split(r"[,;\n|/]+", str(value)):
        part = part.strip()
        if part:
            aliases.add(normalize(part))
            aliases.update(generate_auto_aliases(part))
    return aliases


def build_search_index(rows):
    lookup = {}
    titles = []
    indexed = []

    for row in rows:
        if not row or len(row) < 2:
            continue

        title, file_id = str(row[0]).strip(), str(row[1]).strip()
        if not title or not file_id:
            continue

        title_norm = normalize(title)
        titles.append(title)
        indexed.append((row, title_norm))

        aliases = {title_norm}
        aliases.update(generate_auto_aliases(title))
        if len(row) >= 3:
            aliases.update(parse_manual_aliases(row[2]))

        for alias in aliases:
            if alias:
                lookup.setdefault(alias, []).append(row)

    return indexed, titles, lookup


# ================= LOGS & USERS =================


async def append_log(event, user_id, username, chat_id, query, normalized_query, status, matched_title="", note=""):
    row = [
        time.strftime("%Y-%m-%d %H:%M:%S"),
        event,
        str(user_id or ""),
        str(username or ""),
        str(chat_id or ""),
        str(query or ""),
        str(normalized_query or ""),
        str(status or ""),
        str(matched_title or ""),
        str(note or ""),
    ]
    LOG_QUEUE.append(row)


async def ensure_user_saved(user_id):
    if user_id not in USER_CACHE["ids"]:
        USER_CACHE["ids"].add(user_id)
        NEW_USER_QUEUE.add(user_id)


# ================= KEYBOARD =================


def make_inline_keyboard(titles):
    token = str(time.time_ns())[-10:]
    titles = titles[:MAX_INLINE_SUGGESTIONS]
    _callback_cache[token] = {"titles": titles, "ts": time.time()}
    keyboard = [[InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")] for i, title in enumerate(titles)]
    return InlineKeyboardMarkup(keyboard)


# ================= DELIVERY SYSTEM =================


@dataclass
class DeliveryJob:
    chat_id: int
    user_id: int
    username: str
    rows: list[list[str]]
    query_text: str
    normalized_query: str
    note: str = ""
    progress: int = 0
    total: int = 0
    created_at: float = field(default_factory=time.time)

    def __post_init__(self):
        self.total = len(self.rows)


async def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in _chat_send_locks:
        _chat_send_locks[chat_id] = asyncio.Lock()
    return _chat_send_locks[chat_id]


async def send_one_document(bot, chat_id: int, file_id: str, caption: str):
    async with SEND_GATE:
        await bot.send_document(chat_id=chat_id, document=file_id, caption=caption)


async def delivery_worker(application: Application, worker_no: int):
    assert DELIVERY_QUEUE is not None

    while True:
        job: DeliveryJob = await DELIVERY_QUEUE.get()
        try:
            if job.progress >= job.total:
                continue

            chat_lock = await get_chat_lock(job.chat_id)
            async with chat_lock:
                round_limit = 1 if job.total > 10 else 2
                sent_this_round = 0

                while job.progress < job.total and sent_this_round < round_limit:
                    row = job.rows[job.progress]
                    job.progress += 1
                    sent_this_round += 1

                    if len(row) < 2:
                        continue

                    book_name, file_id = str(row[0]).strip(), str(row[1]).strip()
                    if not book_name or not file_id:
                        continue

                    try:
                        await send_one_document(application.bot, job.chat_id, file_id, f"📘 {book_name}")
                    except Exception as e:
                        logging.error(f"Worker {worker_no} send error for {book_name}: {e}")

                    await asyncio.sleep(0.03)

            if job.progress < job.total:
                await DELIVERY_QUEUE.put(job)

        except Exception as e:
            logging.error(f"Delivery worker {worker_no} error: {e}")
        finally:
            DELIVERY_QUEUE.task_done()


async def start_delivery_system(application: Application):
    global DELIVERY_QUEUE, DELIVERY_STARTED

    if DELIVERY_STARTED:
        return

    DELIVERY_QUEUE = asyncio.Queue(maxsize=5000)
    for i in range(DELIVERY_WORKERS):
        application.create_task(delivery_worker(application, i + 1))

    DELIVERY_STARTED = True
    logging.info(f"Delivery system started with {DELIVERY_WORKERS} workers")


async def queue_book_delivery(context, chat_id: int, rows, user_id: int, username: str, query_text: str, normalized_query: str, note: str = ""):
    total = len(rows)
    ack = "📚 বই পাওয়া গেছে, পাঠানো শুরু করছি..." if total == 1 else f"📚 {total}টি বই পাওয়া গেছে, পাঠানো শুরু করছি..."
    await context.bot.send_message(chat_id=chat_id, text=ack)

    preview_title = str(rows[0][0]).strip() if rows and rows[0] else ""
    await append_log("search", user_id, username, chat_id, query_text, normalized_query, "QUEUED", preview_title, note)

    if DELIVERY_QUEUE is None:
        context.application.create_task(send_books_by_rows(context.bot, chat_id, rows))
        return

    job = DeliveryJob(
        chat_id=chat_id,
        user_id=user_id,
        username=username,
        rows=rows,
        query_text=query_text,
        normalized_query=normalized_query,
        note=note,
    )

    try:
        DELIVERY_QUEUE.put_nowait(job)
    except asyncio.QueueFull:
        await context.bot.send_message(chat_id=chat_id, text="⚠️ সার্ভারে চাপ বেশি। একটু পর আবার চেষ্টা করুন।")


async def send_books_by_rows(bot, chat_id: int, rows, batch_size: int = 3, batch_pause: float = 0.25):
    sent, seen = 0, set()
    chat_lock = await get_chat_lock(chat_id)

    async with chat_lock:
        for i, row in enumerate(rows, start=1):
            book_name, file_id = str(row[0]).strip(), str(row[1]).strip()
            if not book_name or not file_id or (book_name, file_id) in seen:
                continue
            seen.add((book_name, file_id))

            try:
                await send_one_document(bot, chat_id, file_id, f"📘 {book_name}")
                sent += 1
            except Exception as e:
                logging.error(f"Send err for {book_name}: {e}")

            if i % batch_size == 0:
                await asyncio.sleep(batch_pause)
            else:
                await asyncio.sleep(0.03)
    return sent


# ================= SEARCH LOGIC =================


async def process_book_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_text_raw: str, user_id: int, username: str = ""):
    await ensure_user_saved(user_id)

    try:
        rows, indexed_data, book_names, lookup = await get_book_cache()
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="❌ ডাটাবেসে কোনো বই নেই বা লোড হচ্ছে।")
            return

        user_norm = normalize(user_text_raw)
        if not user_norm:
            await context.bot.send_message(chat_id=chat_id, text="❌ বইয়ের নাম লিখুন")
            return

        matched = []
        match_type = ""

        if user_norm in lookup:
            matched = lookup[user_norm]
            match_type = "exact"
        else:
            matched = [row for row, title_norm in indexed_data if user_norm in title_norm or title_norm in user_norm]
            match_type = "partial"

        if matched:
            if len(matched) <= MAX_DIRECT_SEND:
                await queue_book_delivery(context, chat_id, matched, user_id, username, user_text_raw, user_norm, match_type)
                return

            suggestions = []
            for row in matched:
                title = str(row[0]).strip()
                if title and title not in suggestions:
                    suggestions.append(title)
                if len(suggestions) >= MAX_INLINE_SUGGESTIONS:
                    break

            await context.bot.send_message(
                chat_id=chat_id,
         
