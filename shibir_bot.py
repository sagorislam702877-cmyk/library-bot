from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Thread
from difflib import get_close_matches
from typing import Any

import gspread
from flask import Flask
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError, Forbidden
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB").strip()
BOOK_SHEET_NAME = os.environ.get("BOOK_SHEET_NAME", "Sheet1").strip()
USER_SHEET_NAME = os.environ.get("USER_SHEET_NAME", "Users").strip()
LOG_SHEET_NAME = os.environ.get("LOG_SHEET_NAME", "Logs").strip()
PORT = int(os.environ.get("PORT", "8080"))

# কতগুলো worker একসাথে ডেলিভারি পাঠাবে
DELIVERY_WORKERS = int(os.environ.get("DELIVERY_WORKERS", "8"))
# একসাথে এক ইউজারকে কতো ফাইল ব্যাচে পাঠাবে
SEND_BATCH_SIZE = int(os.environ.get("SEND_BATCH_SIZE", "2"))
# ব্যাচের মাঝে গ্যাপ
SEND_BATCH_PAUSE = float(os.environ.get("SEND_BATCH_PAUSE", "0.20"))
# প্রতিটি ডকুমেন্ট পাঠানোর মাঝে গ্যাপ
SEND_ITEM_PAUSE = float(os.environ.get("SEND_ITEM_PAUSE", "0.03"))
# cache TTL
BOOK_CACHE_TTL = int(os.environ.get("BOOK_CACHE_TTL", "300"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID is missing or invalid")

# ================= FLASK (Render Keep-Alive) =================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    return "Library Bot is Ultra Optimized and Running!"


def run_web():
    import logging as flask_logging

    log = flask_logging.getLogger("werkzeug")
    log.setLevel(flask_logging.ERROR)
    web_app.run(host="0.0.0.0", port=PORT, use_reloader=False)


# ================= LOGGING =================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ================= GLOBAL STATE =================

_cached_book_sheet = None
_cached_user_sheet = None
_cached_log_sheet = None
_gspread_client = None
_callback_cache: dict[str, list[str]] = {}
_admin_reply_map: dict[int, int] = {}

LOG_QUEUE: list[list[str]] = []
NEW_USER_QUEUE: set[int] = set()

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

# এক ইউজারের মেসেজ পাঠানো যেন তার নিজের চ্যাটে serial থাকে,
# কিন্তু অন্য ইউজারকে যেন ব্লক না করে
CHAT_SEND_LOCKS: dict[int, asyncio.Lock] = {}
ACTIVE_DELIVERIES: dict[int, str] = {}


@dataclass
class DeliveryTask:
    chat_id: int
    user_id: int
    username: str
    query_text: str
    normalized_query: str
    rows: list[list[str]]
    note: str = ""


# per-chat queue + fair scheduler
DELIVERY_QUEUE_BY_CHAT: dict[int, deque[DeliveryTask]] = defaultdict(deque)
READY_CHAT_IDS: asyncio.Queue[int] | None = None
READY_CHAT_SET: set[int] = set()
SCHEDULER_LOCK: asyncio.Lock | None = None


# ================= SHEETS CONNECTION =================

def get_sheets():
    global _cached_book_sheet, _cached_user_sheet, _cached_log_sheet, _gspread_client

    if _cached_book_sheet and _cached_user_sheet and _cached_log_sheet:
        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        _gspread_client = gspread.authorize(creds)

        spreadsheet = _gspread_client.open(SHEET_NAME)
        _cached_book_sheet = spreadsheet.worksheet(BOOK_SHEET_NAME)
        _cached_user_sheet = spreadsheet.worksheet(USER_SHEET_NAME)

        try:
            _cached_log_sheet = spreadsheet.worksheet(LOG_SHEET_NAME)
        except Exception:
            _cached_log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows=5000, cols=10)
            _cached_log_sheet.append_row(
                [
                    "timestamp", "event", "user_id", "username", "chat_id",
                    "query", "normalized_query", "status", "matched_title", "note",
                ]
            )

        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet
    except Exception as e:
        logger.exception("Google Sheet Auth Error: %s", e)
        return None, None, None


# ================= TEXT UTIL =================

def normalize(text: Any) -> str:
    if not text:
        return ""
    text = str(text).lower().strip()
    return re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)


def tokenize(text: Any) -> list[str]:
    return re.findall(r"[\w\u0980-\u09FF]+", str(text).lower(), flags=re.UNICODE)


def clean_title_like_text(text: Any) -> str:
    text = str(text).strip()
    text = re.sub(r"^[\-\*\d\.\)\s]+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def is_url_like(text: Any) -> bool:
    return bool(re.search(r"(https?://|www\.|t\.me/|telegram\.me/)", str(text), flags=re.I))


def remove_urls(text: Any) -> str:
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


def transliterate_bengali_to_latin(text: Any) -> str:
    if not text:
        return ""
    out = [BENGALI_LATIN_MAP.get(ch, ch) for ch in str(text)]
    result = "".join(out)
    result = re.sub(r"[^a-zA-Z0-9\s]+", " ", result)
    return re.sub(r"\s+", " ", result).strip().lower()


def generate_auto_aliases(title: Any) -> set[str]:
    aliases: set[str] = set()
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


def parse_manual_aliases(value: Any) -> set[str]:
    aliases: set[str] = set()
    if not value:
        return aliases
    for part in re.split(r"[,;\n|/]+", str(value)):
        part = part.strip()
        if part:
            aliases.add(normalize(part))
            aliases.update(generate_auto_aliases(part))
    return aliases


def build_search_index(rows: list[list[str]]):
    lookup: dict[str, list[list[str]]] = {}
    titles: list[str] = []
    indexed: list[tuple[list[str], str]] = []

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


# ================= CACHE =================

def invalidate_book_cache():
    BOOK_CACHE["ts"] = 0.0


async def get_book_cache(force: bool = False):
    now = time.time()
    if (not force) and BOOK_CACHE["rows"] and (now - BOOK_CACHE["ts"] < BOOK_CACHE_TTL):
        return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

    book_sheet, _, _ = get_sheets()
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
        logger.exception("Book cache load error: %s", e)
        return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]


async def load_users_initial():
    _, user_sheet, _ = get_sheets()
    if not user_sheet:
        return
    try:
        values = await asyncio.to_thread(user_sheet.col_values, 1)
        ids = {int(str(x).strip()) for x in values[1:] if str(x).strip().isdigit()}
        USER_CACHE["ids"] = ids
        USER_CACHE["ts"] = time.time()
        logger.info("Loaded %s users into cache", len(ids))
    except Exception as e:
        logger.exception("User load error: %s", e)


# ================= ASYNC LOGS & USERS =================

async def append_log(event, user_id, username, chat_id, query, normalized_query, status, matched_title="", note=""):
    row = [
        time.strftime("%Y-%m-%d %H:%M:%S"),
        str(event or ""),
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


async def ensure_user_saved(user_id: int):
    if user_id not in USER_CACHE["ids"]:
        USER_CACHE["ids"].add(user_id)
        NEW_USER_QUEUE.add(user_id)


async def background_sync_task():
    while True:
        await asyncio.sleep(10)
        _, user_sheet, log_sheet = get_sheets()
        if not user_sheet or not log_sheet:
            continue

        if LOG_QUEUE:
            batch_logs = LOG_QUEUE[:150]
            del LOG_QUEUE[:150]
            try:
                await asyncio.to_thread(log_sheet.append_rows, batch_logs, value_input_option="USER_ENTERED")
            except Exception as e:
                logger.exception("Batch Log error: %s", e)
                LOG_QUEUE[:0] = batch_logs

        if NEW_USER_QUEUE:
            users_to_add = list(NEW_USER_QUEUE)[:100]
            rows_to_add = [[str(uid)] for uid in users_to_add]
            for uid in users_to_add:
                NEW_USER_QUEUE.discard(uid)
            try:
                await asyncio.to_thread(user_sheet.append_rows, rows_to_add, value_input_option="RAW")
            except Exception as e:
                logger.exception("Batch User error: %s", e)
                NEW_USER_QUEUE.update(users_to_add)


# ================= KEYBOARD =================

def make_inline_keyboard(titles: list[str]):
    token = str(time.time_ns())[-10:]
    titles = titles[:10]
    _callback_cache[token] = titles
    keyboard = [[InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")] for i, title in enumerate(titles)]
    return InlineKeyboardMarkup(keyboard)


# ================= DELIVERY ENGINE =================

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in CHAT_SEND_LOCKS:
        CHAT_SEND_LOCKS[chat_id] = asyncio.Lock()
    return CHAT_SEND_LOCKS[chat_id]


async def schedule_delivery(task: DeliveryTask):
    global READY_CHAT_IDS, SCHEDULER_LOCK
    if READY_CHAT_IDS is None or SCHEDULER_LOCK is None:
        raise RuntimeError("Delivery scheduler is not ready")

    async with SCHEDULER_LOCK:
        DELIVERY_QUEUE_BY_CHAT[task.chat_id].append(task)
        if task.chat_id not in READY_CHAT_SET:
            READY_CHAT_SET.add(task.chat_id)
            await READY_CHAT_IDS.put(task.chat_id)


async def send_document_with_retry(bot, chat_id: int, file_id: str, caption: str, retries: int = 4) -> bool:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            await bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
            return True
        except RetryAfter as e:
            last_error = e
            await asyncio.sleep(float(getattr(e, "retry_after", 2)) + 0.5)
        except (TimedOut, NetworkError) as e:
            last_error = e
            await asyncio.sleep(min(2.5 * attempt, 8))
        except Forbidden as e:
            last_error = e
            logger.warning("Forbidden while sending to %s: %s", chat_id, e)
            return False
        except BadRequest as e:
            last_error = e
            logger.warning("BadRequest while sending to %s: %s", chat_id, e)
            return False
        except Exception as e:
            last_error = e
            logger.exception("Unexpected send error to %s: %s", chat_id, e)
            await asyncio.sleep(min(2 * attempt, 6))
    logger.error("Failed to send document to %s after retries: %s", chat_id, last_error)
    return False


async def send_books_by_rows(bot, chat_id: int, rows: list[list[str]], batch_size: int = SEND_BATCH_SIZE, batch_pause: float = SEND_BATCH_PAUSE):
    sent = 0
    seen: set[tuple[str, str]] = set()

    async with get_chat_lock(chat_id):
        for i, row in enumerate(rows, start=1):
            if not row or len(row) < 2:
                continue

            book_name = str(row[0]).strip()
            file_id = str(row[1]).strip()
            if not book_name or not file_id:
                continue

            key = (book_name, file_id)
            if key in seen:
                continue
            seen.add(key)

            ok = await send_document_with_retry(bot, chat_id, file_id, f"📘 {book_name}")
            if ok:
                sent += 1

            if i % batch_size == 0:
                await asyncio.sleep(batch_pause)
            else:
                await asyncio.sleep(SEND_ITEM_PAUSE)

    return sent


async def delivery_worker(app: Application, worker_no: int):
    global READY_CHAT_IDS, SCHEDULER_LOCK
    if READY_CHAT_IDS is None or SCHEDULER_LOCK is None:
        raise RuntimeError("Delivery scheduler not initialized")

    logger.info("Delivery worker %s started", worker_no)

    while True:
        chat_id = await READY_CHAT_IDS.get()
        try:
            async with SCHEDULER_LOCK:
                q = DELIVERY_QUEUE_BY_CHAT.get(chat_id)
                task = q.popleft() if q else None
                if q and len(q) > 0:
                    await READY_CHAT_IDS.put(chat_id)
                else:
                    READY_CHAT_SET.discard(chat_id)
                    if q is not None and len(q) == 0:
                        DELIVERY_QUEUE_BY_CHAT.pop(chat_id, None)

            if task is None:
                continue

            ACTIVE_DELIVERIES[chat_id] = task.query_text
            preview_title = str(task.rows[0][0]).strip() if task.rows and task.rows[0] else ""

            sent = await send_books_by_rows(app.bot, chat_id, task.rows)
            status = "DELIVERED" if sent else "FAILED"
            note = task.note or f"requested={len(task.rows)}, sent={sent}"
            await append_log("search", task.user_id, task.username, task.chat_id, task.query_text, task.normalized_query, status, preview_title, note)
        except Exception as e:
            logger.exception("Worker %s delivery error: %s", worker_no, e)
        finally:
            ACTIVE_DELIVERIES.pop(chat_id, None)
            READY_CHAT_IDS.task_done()


async def queue_book_delivery(context: ContextTypes.DEFAULT_TYPE, chat_id: int, rows, user_id: int, username: str, query_text: str, normalized_query: str, note: str = ""):
    total = len(rows)
    ack = "📚 বই পাওয়া গেছে, পাঠানো শুরু করছি..." if total == 1 else f"📚 {total}টি বই পাওয়া গেছে, পাঠানো শুরু করছি..."
    await context.bot.send_message(chat_id=chat_id, text=ack)

    task = DeliveryTask(
        chat_id=chat_id,
        user_id=user_id,
        username=username,
        query_text=query_text,
        normalized_query=normalized_query,
        rows=list(rows),
        note=note,
    )
    await schedule_delivery(task)

    preview_title = str(rows[0][0]).strip() if rows and rows[0] else ""
    await append_log("search", user_id, username, chat_id, query_text, normalized_query, "QUEUED", preview_title, note)


# ================= SEARCH LOGIC =================

async def process_book_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_text_raw: str, user_id: int, username: str = ""):
    await ensure_user_saved(user_id)

    try:
        rows, indexed_data, _book_names, lookup = await get_book_cache()
        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="❌ ডাটাবেসে কোনো বই নেই বা লোড হচ্ছে।")
            return

        user_norm = normalize(user_text_raw)
        if not user_norm:
            await context.bot.send_message(chat_id=chat_id, text="❌ বইয়ের নাম লিখুন")
            return

        matched: list[list[str]] = []
        match_type = ""

        # 1) exact alias match
        if user_norm in lookup:
            matched = lookup[user_norm]
            match_type = "exact"
        else:
            # 2) partial match
            matched = [row for row, title_norm in indexed_data if user_norm in title_norm or title_norm in user_norm]
            match_type = "partial"

        if matched:
            unique_rows = []
            seen = set()
            for row in matched:
                key = (str(row[0]).strip(), str(row[1]).strip())
                if key not in seen:
                    seen.add(key)
                    unique_rows.append(row)
            matched = unique_rows

            if len(matched) <= 5:
                await queue_book_delivery(context, chat_id, matched, user_id, username, user_text_raw, user_norm, match_type)
                return

            suggestions = []
            for row in matched:
                title = str(row[0]).strip()
                if title and title not in suggestions:
                    suggestions.append(title)
                if len(suggestions) >= 10:
                    break

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"📚 '{user_text_raw}' সম্পর্কিত প্রায় *{len(matched)}* টি বই পাওয়া গেছে।\n"
                    "একসাথে এত বই পাঠালে বট ধীর হয়ে যেতে পারে।\n\n"
                    "দয়া করে নিচের তালিকা থেকে নির্দিষ্ট বইটি বেছে নিন:"
                ),
                reply_markup=make_inline_keyboard(suggestions),
                parse_mode="Markdown",
            )
            await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "TOO_MANY_MATCHES", suggestions[0] if suggestions else "")
            return

        # 3) fuzzy suggestions
        close_keys = get_close_matches(user_norm, list(lookup.keys()), n=5, cutoff=0.72)
        if close_keys:
            suggestions = []
            for key in close_keys:
                for row in lookup.get(key, []):
                    title = str(row[0]).strip()
                    if title and title not in suggestions:
                        suggestions.append(title)
                    if len(suggestions) >= 10:
                        break
                if len(suggestions) >= 10:
                    break

            if suggestions:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🤖 আমি কয়েকটা সম্ভাব্য বই খুঁজে পেয়েছি:",
                    reply_markup=make_inline_keyboard(suggestions),
                )
                await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "SUGGESTION", suggestions[0])
                return

        await context.bot.send_message(chat_id=chat_id, text="❌ বইটি খুঁজে পাওয়া যাচ্ছে না")
        await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "MISS")

    except Exception as e:
        logger.exception("Search error: %s", e)
        await context.bot.send_message(chat_id=chat_id, text="⚠️ সার্ভারে সমস্যা হয়েছে। একটু পর চেষ্টা করুন।")


# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await ensure_user_saved(user.id)
    await update.message.reply_text(
        "আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখুন।\n"
        "এডমিনের সাথে কথা বলতে /admin + আপনার টেক্সট লিখুন"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "বট ব্যবহারের নিয়মাবলী:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. এডমিন: এডমিনের সাথে যোগাযোগ করতে চাইলে /admin লিখে আপনার কথা লিখুন।\n"
        "   যেমন: /admin ভাই আমার অমুক বই প্রয়োজন"
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    body = parts[1].strip() if len(parts) > 1 else ""

    if not body:
        await update.message.reply_text("ব্যবহার: /admin আপনার মেসেজ")
        return

    user = update.effective_user
    chat_id = update.effective_chat.id
    username = f"@{user.username}" if user and user.username else "No username"

    msg = (
        f"📩 নতুন ইউজার মেসেজ\n\n"
        f"নাম: {user.full_name if user else 'Unknown'}\n"
        f"ইউজারনেম: {username}\n"
        f"ইউজার আইডি: {user.id if user else ''}\n"
        f"চ্যাট আইডি: {chat_id}\n\n"
        f"মেসেজ:\n{body}"
    )
    sent = await context.bot.send_message(chat_id=ADMIN_ID, text=msg)
    _admin_reply_map[sent.message_id] = chat_id
    await update.message.reply_text("✅ মেসেজ এডমিনের কাছে পাঠানো হয়েছে")
    await append_log("admin_message", user.id if user else 0, username, chat_id, body, normalize(body), "SENT")


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    broadcast_text = parts[1].strip() if len(parts) > 1 else ""

    if not broadcast_text and update.message.reply_to_message:
        broadcast_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""

    if not broadcast_text:
        await update.message.reply_text("ব্যবহার: /broadcast আপনার মেসেজ")
        return

    await update.message.reply_text("📢 ব্রডকাস্ট শুরু হয়েছে। ব্যাকগ্রাউন্ডে মেসেজ যাচ্ছে...")

    async def run_broadcast():
        success, failed = 0, 0
        for uid in list(USER_CACHE["ids"]):
            try:
                await context.bot.send_message(chat_id=uid, text=broadcast_text)
                success += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"✅ Broadcast শেষ\nসফল: {success}\nব্যর্থ: {failed}",
        )

    context.application.create_task(run_broadcast())


async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, _, _ = get_sheets()
    if not book_sheet:
        await update.message.reply_text("⚠️ শিট সংযোগ পাওয়া যায়নি")
        return

    target = update.message.reply_to_message
    if not target or not target.document:
        await update.message.reply_text("ব্যবহার: ডকুমেন্ট ফাইলে রিপ্লাই দিয়ে /upload বা /upload বইয়ের নাম দিন।")
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    book_name = parts[1].strip() if len(parts) > 1 else (target.document.file_name or "অজানা বই")
    aliases = target.caption or ""

    try:
        await asyncio.to_thread(book_sheet.append_row, [book_name, target.document.file_id, aliases])
        invalidate_book_cache()
        await update.message.reply_text(f"✅ বই আপলোড হয়েছে: {book_name}")
    except Exception as e:
        logger.exception("Upload error: %s", e)
        await update.message.reply_text("⚠️ আপলোডে এরর হয়েছে")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    users_count = len(USER_CACHE["ids"])
    books_count = len(BOOK_CACHE["rows"])
    pending_delivery_chats = len(DELIVERY_QUEUE_BY_CHAT)
    active_delivery_chats = len(ACTIVE_DELIVERIES)
    await update.message.reply_text(
        "📊 লাইভ স্ট্যাটাস:\n"
        f"ইউজার: {users_count}\n"
        f"মোট বই: {books_count}\n"
        f"পেন্ডিং লগ: {len(LOG_QUEUE)}\n"
        f"অ্যাক্টিভ ডেলিভারি: {active_delivery_chats}\n"
        f"কিউতে থাকা চ্যাট: {pending_delivery_chats}"
    )


async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await get_book_cache(force=True)
    await update.message.reply_text("✅ বইয়ের cache refresh হয়েছে")


# ================= HANDLERS =================

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user = update.effective_user
    text = update.message.text.strip()
    if not user or not text:
        return

    # admin reply routing
    if user.id == ADMIN_ID and update.message.reply_to_message:
        target_chat_id = _admin_reply_map.get(update.message.reply_to_message.message_id)
        if target_chat_id:
            try:
                await context.bot.send_message(chat_id=target_chat_id, text=f"👨‍💼 এডমিন: {text}")
                await update.message.reply_text("✅ রিপ্লাই পাঠানো হয়েছে")
            except Exception:
                await update.message.reply_text("⚠️ পাঠানো যায়নি")
            return

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass

    await process_book_search(
        context,
        update.effective_chat.id,
        text,
        user.id,
        f"@{user.username}" if user.username else "",
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("pick|"):
        try:
            _, token, idx = data.split("|", 2)
            idx_i = int(idx)
        except Exception:
            await query.message.reply_text("⚠️ ভুল ডাটা এসেছে। আবার সার্চ করুন।")
            return

        titles = _callback_cache.get(token, [])
        if not titles or idx_i >= len(titles):
            await query.message.reply_text("⚠️ সেশন শেষ, আবার সার্চ করুন।")
            return

        selected_title = titles[idx_i]
        try:
            await query.message.edit_text(f"🔎 খোঁজা হচ্ছে: {selected_title}")
        except Exception:
            pass

        await process_book_search(
            context,
            query.message.chat.id,
            selected_title,
            query.from_user.id,
            f"@{query.from_user.username}" if query.from_user.username else "",
        )


# ================= STARTUP =================

async def startup_task(app: Application):
    global READY_CHAT_IDS, SCHEDULER_LOCK

    READY_CHAT_IDS = asyncio.Queue()
    SCHEDULER_LOCK = asyncio.Lock()

    await load_users_initial()
    await get_book_cache()

    app.create_task(background_sync_task())
    for worker_no in range(1, DELIVERY_WORKERS + 1):
        app.create_task(delivery_worker(app, worker_no))

    logger.info("Startup complete. Workers=%s", DELIVERY_WORKERS)


def build_application() -> Application:
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(512)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("upload", upload_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("refresh", refresh_command))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    app.post_init = startup_task
    return app


def main():
    Thread(target=run_web, daemon=True).start()
    app = build_application()
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
