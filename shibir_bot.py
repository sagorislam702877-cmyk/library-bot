from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from threading import Thread
from difflib import get_close_matches

import gspread
from flask import Flask
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB").strip()
LOG_SHEET_NAME = os.environ.get("LOG_SHEET_NAME", "Logs").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID is missing or invalid")

# ================= CACHE =================

_cached_book_sheet = None
_cached_user_sheet = None
_cached_log_sheet = None
_callback_cache = {}
_admin_reply_map = {}
_chat_send_locks = {}

BOOK_CACHE = {
    "ts": 0.0,
    "rows": [],
    "indexed": [],
    "titles": [],
    "lookup": {},
}

USER_CACHE = {
    "ts": 0.0,
    "ids": [],
}

BOOK_CACHE_TTL = 300
USER_CACHE_TTL = 300

# ================= FLASK =================

web_app = Flask(__name__)

@web_app.route("/")
def home():
    return "Library Bot is Optimized!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port, use_reloader=False)

# ================= LOGGING =================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ================= SHEETS =================

def get_sheets():
    global _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    if _cached_book_sheet and _cached_user_sheet:
        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open(SHEET_NAME)
        _cached_book_sheet = spreadsheet.worksheet("Sheet1")
        _cached_user_sheet = spreadsheet.worksheet("Users")

        try:
            _cached_log_sheet = spreadsheet.worksheet(LOG_SHEET_NAME)
        except Exception:
            _cached_log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows=5000, cols=10)
            _cached_log_sheet.append_row(
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
                ]
            )

        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    except Exception as e:
        logging.error(f"Sheet Error: {e}")
        return None, None, None

# ================= CACHE HELPERS =================

def invalidate_book_cache():
    BOOK_CACHE["ts"] = 0.0
    BOOK_CACHE["rows"] = []
    BOOK_CACHE["indexed"] = []
    BOOK_CACHE["titles"] = []
    BOOK_CACHE["lookup"] = {}

def invalidate_user_cache():
    USER_CACHE["ts"] = 0.0
    USER_CACHE["ids"] = []

# ================= TEXT UTIL =================

def normalize(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)
    return text

def tokenize(text):
    return re.findall(r"[\w\u0980-\u09FF]+", str(text).lower(), flags=re.UNICODE)

def looks_like_latin(text):
    text = str(text)
    return bool(re.search(r"[A-Za-z]", text)) and not bool(re.search(r"[\u0980-\u09FF]", text))

def clean_title_like_text(text):
    text = str(text).strip()
    text = re.sub(r"^[\-\*\d\.\)\s]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def clean_candidate_line(line):
    line = line.strip()
    line = re.sub(r"^[\-\*\d\.\)\s]+", "", line)
    return line.strip()

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
    out = []
    for ch in str(text):
        out.append(BENGALI_LATIN_MAP.get(ch, ch))
    result = "".join(out)
    result = re.sub(r"[^a-zA-Z0-9\s]+", " ", result)
    result = re.sub(r"\s+", " ", result).strip().lower()
    return result

def generate_auto_aliases(title):
    aliases = set()
    if not title:
        return aliases

    raw = str(title).strip()
    raw_lower = raw.lower().strip()

    variants = {
        raw_lower,
        raw,
        normalize(raw),
        normalize(raw.replace(" ", "")),
    }

    volume_removed = re.sub(r"(খণ্ড|খন্ড|volume|vol\.?|v\.?)\s*\d*", "", raw_lower, flags=re.I)
    variants.add(normalize(volume_removed))
    variants.add(normalize(volume_removed.replace(" ", "")))

    tokens = [normalize(t) for t in tokenize(raw) if normalize(t)]
    tokens = [t for t in tokens if len(t) >= 2]

    for t in tokens:
        variants.add(t)

    if len(tokens) >= 2:
        variants.add("".join(tokens))
        variants.add(" ".join(tokens))
        for i in range(len(tokens) - 1):
            pair = tokens[i] + tokens[i + 1]
            if len(pair) >= 3:
                variants.add(pair)

    latin = transliterate_bengali_to_latin(raw)
    if latin:
        variants.add(latin)
        variants.add(normalize(latin))
        variants.add(normalize(latin.replace(" ", "")))

        latin_tokens = [normalize(t) for t in tokenize(latin) if normalize(t)]
        for t in latin_tokens:
            if len(t) >= 2:
                variants.add(t)

    aliases = {v for v in variants if v and len(v) >= 2}
    return aliases

def parse_manual_aliases(value):
    aliases = set()
    if not value:
        return aliases

    parts = re.split(r"[,;\n|/]+", str(value))
    for part in parts:
        part = part.strip()
        if not part:
            continue
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

        title = str(row[0]).strip()
        file_id = str(row[1]).strip()

        if not title or not file_id:
            continue

        title_norm = normalize(title)
        titles.append(title)
        indexed.append((row, title_norm))

        aliases = set()
        aliases.add(title_norm)
        aliases.update(generate_auto_aliases(title))

        if len(row) >= 3:
            aliases.update(parse_manual_aliases(row[2]))

        for alias in aliases:
            if not alias:
                continue
            lookup.setdefault(alias, []).append(row)

    return indexed, titles, lookup

async def get_book_cache(force=False):
    book_sheet, _, _ = get_sheets()
    if not book_sheet:
        return [], [], [], {}

    now = time.time()
    if (not force) and BOOK_CACHE["rows"] and (now - BOOK_CACHE["ts"] < BOOK_CACHE_TTL):
        return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

    try:
        values = await asyncio.to_thread(book_sheet.get_all_values)
        rows = values[1:] if len(values) > 1 else []

        rows = [
            row for row in rows
            if row and len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip()
        ]

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

async def get_user_ids_cached(force=False):
    _, user_sheet, _ = get_sheets()
    if not user_sheet:
        return []

    now = time.time()
    if (not force) and USER_CACHE["ids"] and (now - USER_CACHE["ts"] < USER_CACHE_TTL):
        return USER_CACHE["ids"]

    try:
        values = await asyncio.to_thread(user_sheet.col_values, 1)
        ids = []
        for x in values[1:] if len(values) > 1 else []:
            x = str(x).strip()
            if x.isdigit():
                ids.append(int(x))

        ids = list(dict.fromkeys(ids))
        USER_CACHE["ids"] = ids
        USER_CACHE["ts"] = now
        return ids

    except Exception as e:
        logging.error(f"User cache load error: {e}")
        return USER_CACHE["ids"]

# ================= LOGS =================

async def append_log(event, user_id, username, chat_id, query, normalized_query, status, matched_title="", note=""):
    _, _, log_sheet = get_sheets()
    if not log_sheet:
        return

    try:
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
        await asyncio.to_thread(log_sheet.append_row, row, value_input_option="USER_ENTERED")
    except Exception as e:
        logging.error(f"Log write error: {e}")

# ================= BOOK NAME EXTRACTION =================

def derive_book_name_from_caption_or_filename(caption, file_name):
    candidates = []

    if caption:
        for raw_line in str(caption).splitlines():
            line = clean_title_like_text(raw_line)
            if not line:
                continue

            low = line.lower()

            if low.startswith("/upload"):
                line = line.split(maxsplit=1)[1].strip() if len(line.split(maxsplit=1)) > 1 else ""
                line = clean_title_like_text(line)

            line = remove_urls(line)
            line = clean_title_like_text(line)

            if not line:
                continue

            if low.startswith("forwarded from"):
                continue

            if is_url_like(line):
                continue

            candidates.append(line)

    if candidates:
        return candidates[0]

    if file_name:
        base = os.path.splitext(str(file_name))[0]
        base = base.replace("_", " ").replace("-", " ")
        base = re.sub(r"\s+", " ", base).strip()
        base = clean_title_like_text(base)
        if base and not is_url_like(base):
            return base

    return "অজানা বই"

# ================= KEYBOARD =================

def make_inline_keyboard(titles):
    token = str(time.time_ns())[-10:]
    titles = titles[:10]
    _callback_cache[token] = titles

    keyboard = [
        [InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")]
        for i, title in enumerate(titles)
    ]
    return InlineKeyboardMarkup(keyboard)

# ================= USER / ADMIN HELPERS =================

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    lock = _chat_send_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _chat_send_locks[chat_id] = lock
    return lock

def extract_target_chat_id_from_admin_message(message_text: str):
    if not message_text:
        return None
    m = re.search(r"চ্যাট আইডি:\s*(-?\d+)", message_text)
    if m:
        return int(m.group(1))
    return None

async def ensure_user_saved(user_id, user_sheet):
    if not user_sheet:
        return
    try:
        cached_ids = await get_user_ids_cached()
        if user_id not in cached_ids:
            await asyncio.to_thread(user_sheet.append_row, [str(user_id)])
            if user_id not in USER_CACHE["ids"]:
                USER_CACHE["ids"].append(user_id)
            USER_CACHE["ts"] = time.time()
    except Exception as e:
        logging.error(f"User save error: {e}")

async def send_books_by_rows(bot, chat_id: int, rows, batch_size: int = 3, batch_pause: float = 0.3):
    sent = 0
    seen = set()

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

            try:
                await bot.send_document(
                    chat_id=chat_id,
                    document=file_id,
                    caption=f"📘 {book_name}",
                )
                sent += 1
            except BadRequest as e:
                logging.error(f"Send document failed for {book_name}: {e}")
            except Exception as e:
                logging.error(f"Send document error for {book_name}: {e}")

            if i % batch_size == 0:
                await asyncio.sleep(batch_pause)
            else:
                await asyncio.sleep(0)

    return sent

async def queue_book_delivery(context, chat_id: int, rows, user_id: int, username: str, query_text: str, normalized_query: str, note: str = ""):
    if not rows:
        await context.bot.send_message(chat_id=chat_id, text="❌ বই পাওয়া যায়নি")
        return

    total = len(rows)
    if total == 1:
        ack = "📚 বই পাওয়া গেছে, পাঠানো শুরু করছি..."
    else:
        ack = f"📚 {total}টি বই পাওয়া গেছে, পাঠানো শুরু করছি..."

    await context.bot.send_message(chat_id=chat_id, text=ack)

    context.application.create_task(
        send_books_by_rows(
            bot=context.bot,
            chat_id=chat_id,
            rows=rows,
            batch_size=3,
            batch_pause=0.3,
        )
    )

    preview_title = str(rows[0][0]).strip() if rows and rows[0] else ""
    await append_log("search", user_id, username, chat_id, query_text, normalized_query, "QUEUED", preview_title, note)

async def upsert_book(book_sheet, book_name, file_id, aliases_text=""):
    try:
        values = await asyncio.to_thread(book_sheet.get_all_values)
        rows = values[1:] if len(values) > 1 else []
        target_norm = normalize(book_name)

        for idx, row in enumerate(rows, start=2):
            if not row:
                continue

            existing_name = str(row[0]).strip() if len(row) > 0 else ""
            if existing_name and normalize(existing_name) == target_norm:
                existing_aliases = str(row[2]).strip() if len(row) >= 3 else ""
                final_aliases = aliases_text.strip() if aliases_text.strip() else existing_aliases
                if final_aliases:
                    await asyncio.to_thread(
                        book_sheet.update,
                        f"A{idx}:C{idx}",
                        [[book_name, file_id, final_aliases]],
                    )
                else:
                    await asyncio.to_thread(
                        book_sheet.update,
                        f"A{idx}:B{idx}",
                        [[book_name, file_id]],
                    )
                invalidate_book_cache()
                return "updated"

        if aliases_text.strip():
            await asyncio.to_thread(book_sheet.append_row, [book_name, file_id, aliases_text.strip()])
        else:
            await asyncio.to_thread(book_sheet.append_row, [book_name, file_id])

        invalidate_book_cache()
        return "added"

    except Exception as e:
        logging.error(f"Upsert book error: {e}")
        return "error"

# ================= SEARCH =================

async def process_book_search(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_text_raw: str,
    user_id: int,
    username: str = "",
):
    book_sheet, user_sheet, _ = get_sheets()

    if not book_sheet:
        await context.bot.send_message(chat_id=chat_id, text="❌ Database error")
        return

    await ensure_user_saved(user_id, user_sheet)

    try:
        rows, indexed_data, book_names, lookup = await get_book_cache()

        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="❌ বই পাওয়া যায়নি")
            return

        user_norm = normalize(user_text_raw)

        if not user_norm:
            await context.bot.send_message(chat_id=chat_id, text="❌ বইয়ের নাম লিখুন")
            await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "EMPTY")
            return

        # 1) exact alias/title match
        if user_norm in lookup:
            matched_rows = lookup[user_norm]
            await queue_book_delivery(
                context=context,
                chat_id=chat_id,
                rows=matched_rows,
                user_id=user_id,
                username=username,
                query_text=user_text_raw,
                normalized_query=user_norm,
                note="alias/exact",
            )
            return

        # 2) partial contains match
        matched = []
        for row, title_norm in indexed_data:
            if user_norm in title_norm or title_norm in user_norm:
                matched.append(row)

        if matched:
            await queue_book_delivery(
                context=context,
                chat_id=chat_id,
                rows=matched,
                user_id=user_id,
                username=username,
                query_text=user_text_raw,
                normalized_query=user_norm,
                note="partial",
            )
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
                await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "SUGGESTION", suggestions[0], "fuzzy")
                return

        await context.bot.send_message(chat_id=chat_id, text="❌ বইটি খুঁজে পাওয়া যাচ্ছে না")
        await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "MISS")

    except Exception as e:
        logging.error(f"Search error: {e}")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Error occurred")
        await append_log("search", user_id, username, chat_id, user_text_raw, normalize(user_text_raw), "ERROR", note=str(e))

# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখুন।\n"
        "এডমিনের সাথে কথা বলতে /admin + আপনার টেক্সটি লিখুন"
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
        await update.message.reply_text(
            "ব্যবহার: /admin আপনার মেসেজ\n"
            "উদাহরণ: /admin ভাই আমার অমুক বই প্রয়োজন"
        )
        return

    user = update.effective_user
    chat_id = update.effective_chat.id
    user_name = user.full_name if user else "Unknown"
    user_username = f"@{user.username}" if user and user.username else "No username"

    msg = (
        "📩 নতুন ইউজার মেসেজ\n\n"
        f"নাম: {user_name}\n"
        f"ইউজারনেম: {user_username}\n"
        f"ইউজার আইডি: {user.id}\n"
        f"চ্যাট আইডি: {chat_id}\n\n"
        f"মেসেজ:\n{body}\n\n"
        "এই মেসেজের রিপ্লাই দিলে বট ইউজারকে পাঠাবে।"
    )

    sent = await context.bot.send_message(chat_id=ADMIN_ID, text=msg)
    _admin_reply_map[sent.message_id] = chat_id
    await update.message.reply_text("✅ মেসেজ এডমিনের কাছে পাঠানো হয়েছে")
    await append_log("admin_message", user.id if user else None, user_username, chat_id, body, normalize(body), "SENT_ADMIN")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    _, user_sheet, _ = get_sheets()
    if not user_sheet:
        await update.message.reply_text("❌ User sheet error")
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    broadcast_text = parts[1].strip() if len(parts) > 1 else ""

    if not broadcast_text and update.message.reply_to_message:
        broadcast_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""

    if not broadcast_text:
        await update.message.reply_text("ব্যবহার: /broadcast আপনার মেসেজ")
        return

    user_ids = await get_user_ids_cached(force=True)

    success = 0
    failed = 0

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=broadcast_text)
            success += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1

    await update.message.reply_text(
        f"✅ Broadcast শেষ হয়েছে\nসফল: {success}\nব্যর্থ: {failed}"
    )
    await append_log("broadcast", ADMIN_ID, "admin", update.effective_chat.id, broadcast_text, normalize(broadcast_text), "DONE", note=f"success={success},failed={failed}")

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, _, _ = get_sheets()
    if not book_sheet:
        await update.message.reply_text("❌ Sheet error")
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    book_name = parts[1].strip() if len(parts) > 1 else ""

    target_message = update.message.reply_to_message

    if not target_message or not target_message.document:
        await update.message.reply_text(
            "ব্যবহার: /upload বইয়ের নাম\n"
            "এই কমান্ডটা এমন একটি মেসেজের রিপ্লাই হিসেবে দিন যেখানে ডকুমেন্ট আছে।"
        )
        return

    if not book_name:
        book_name = target_message.document.file_name or "অজানা বই"

    file_id = target_message.document.file_id
    aliases_text = target_message.caption or ""
    result = await upsert_book(book_sheet, book_name, file_id, aliases_text=aliases_text)

    if result == "updated":
        await update.message.reply_text(f"✅ বইটি আপডেট করা হয়েছে: {book_name}")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "UPDATED")
    elif result == "added":
        await update.message.reply_text(f"✅ বইটি আপলোড করা হয়েছে: {book_name}")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "ADDED")
    else:
        await update.message.reply_text("⚠️ আপলোড করা যায়নি")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "ERROR")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet, log_sheet = get_sheets()
    if not book_sheet or not user_sheet:
        await update.message.reply_text("❌ Sheet error")
        return

    try:
        total_users = len(await get_user_ids_cached(force=True))
        total_books = len((await get_book_cache(force=True))[0])
        total_logs = 0

        try:
            if log_sheet:
                total_logs = len(await asyncio.to_thread(log_sheet.get_all_values)) - 1
                if total_logs < 0:
                    total_logs = 0
        except Exception:
            total_logs = 0

        await update.message.reply_text(
            f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি\nমোট লগ: {max(0, total_logs)} টি"
        )
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await update.message.reply_text("⚠️ Stats error")

async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    _, _, log_sheet = get_sheets()
    if not log_sheet:
        await update.message.reply_text("❌ Log sheet error")
        return

    try:
        values = await asyncio.to_thread(log_sheet.get_all_values)
        rows = values[1:] if len(values) > 1 else []
        last_rows = rows[-10:]

        if not last_rows:
            await update.message.reply_text("কোনো লগ নেই।")
            return

        lines = ["শেষ ১০টি লগ:\n"]
        for row in last_rows:
            ts = row[0] if len(row) > 0 else ""
            event = row[1] if len(row) > 1 else ""
            q = row[5] if len(row) > 5 else ""
            status = row[7] if len(row) > 7 else ""
            matched = row[8] if len(row) > 8 else ""
            lines.append(f"• {ts} | {event} | {status} | {q} | {matched}")

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logging.error(f"Logs command error: {e}")
        await update.message.reply_text("⚠️ Logs read error")

# ================= TEXT HANDLER =================

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    username = f"@{user.username}" if user and user.username else ""

    if user_id == ADMIN_ID:
        if update.message.reply_to_message:
            reply_to = update.message.reply_to_message.message_id
            target_chat_id = _admin_reply_map.get(reply_to)

            if not target_chat_id:
                fallback_text = update.message.reply_to_message.text or ""
                target_chat_id = extract_target_chat_id_from_admin_message(fallback_text)

            if target_chat_id:
                try:
                    await context.bot.send_message(chat_id=target_chat_id, text=f"👨‍💼 এডমিন: {text}")
                    await update.message.reply_text("✅ ইউজারকে রিপ্লাই পাঠানো হয়েছে")
                    await append_log("admin_reply", ADMIN_ID, username, target_chat_id, text, normalize(text), "SENT")
                    return
                except Exception as e:
                    logging.error(f"Admin reply error: {e}")
                    await update.message.reply_text("⚠️ রিপ্লাই পাঠানো যায়নি")
                    await append_log("admin_reply", ADMIN_ID, username, target_chat_id, text, normalize(text), "ERROR", note=str(e))
                    return
            else:
                await update.message.reply_text("⚠️ এই মেসেজটার ইউজার লিংক পাওয়া যায়নি")
                return

        return

    await process_book_search(
        context=context,
        chat_id=update.effective_chat.id,
        user_text_raw=text,
        user_id=user_id,
        username=username,
    )

# ================= DOCUMENT HANDLER (AUTO UPLOAD) =================

async def handle_admin_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.document:
        return

    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, _, _ = get_sheets()
    if not book_sheet:
        await update.message.reply_text("❌ Sheet error")
        return

    caption = update.message.caption or ""
    file_name = update.message.document.file_name or ""
    book_name = derive_book_name_from_caption_or_filename(caption, file_name)
    file_id = update.message.document.file_id

    result = await upsert_book(book_sheet, book_name, file_id, aliases_text=caption)

    if result == "updated":
        await update.message.reply_text(f"✅ বইটি আপডেট করা হয়েছে: {book_name}")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "UPDATED")
    elif result == "added":
        await update.message.reply_text(f"✅ বইটি আপলোড করা হয়েছে: {book_name}")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "ADDED")
    else:
        await update.message.reply_text("⚠️ আপলোড করা যায়নি")
        await append_log("upload", ADMIN_ID, "admin", update.effective_chat.id, book_name, normalize(book_name), "ERROR")

# ================= CALLBACK =================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        data = query.data or ""

        if data.startswith("pick|"):
            _, token, idx = data.split("|", 2)
            titles = _callback_cache.get(token, [])

            if not titles:
                await query.message.reply_text("⚠️ Suggestion expired")
                return

            try:
                index = int(idx)
            except ValueError:
                await query.message.reply_text("⚠️ Invalid selection")
                return

            if index < 0 or index >= len(titles):
                await query.message.reply_text("⚠️ Invalid selection")
                return

            selected_title = titles[index]

            try:
                await query.message.edit_text(f"🔎 খোঁজা হচ্ছে: {selected_title}")
            except Exception:
                pass

            await append_log(
                "suggestion_click",
                query.from_user.id,
                f"@{query.from_user.username}" if query.from_user and query.from_user.username else "",
                query.message.chat.id,
                selected_title,
                normalize(selected_title),
                "CLICKED",
            )

            await process_book_search(
                context=context,
                chat_id=query.message.chat.id,
                user_text_raw=selected_title,
                user_id=query.from_user.id,
                username=f"@{query.from_user.username}" if query.from_user and query.from_user.username else "",
            )
            return

    except Exception as e:
        logging.error(f"Callback error: {e}")
        try:
            await query.message.reply_text("⚠️ Callback error")
        except Exception:
            pass

# ================= MAIN =================

def main():
    Thread(target=run_web, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).concurrent_updates(16).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("Admin", admin_command))

    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("Broadcast", broadcast_command))

    app.add_handler(CommandHandler("upload", upload_command))
    app.add_handler(CommandHandler("Upload", upload_command))

    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("Stats", stats))

    app.add_handler(CommandHandler("logs", logs_command))
    app.add_handler(CommandHandler("Logs", logs_command))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_admin_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

    
