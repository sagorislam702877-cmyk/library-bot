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

# ================= QUEUES & CACHE =================

# ব্যাচ প্রসেসিং এর জন্য Queue (গুগল শিট API লিমিট বাঁচানোর জন্য)
LOG_QUEUE = []
NEW_USER_QUEUE = set()

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
    "ids": set(), # Set ব্যবহার করা হয়েছে দ্রুত খোঁজার জন্য
}

BOOK_CACHE_TTL = 300 # 5 minutes

# ================= FLASK (For Render Keep-Alive) =================

web_app = Flask(__name__)

@web_app.route("/")
def home():
    return "Library Bot is Highly Optimized and Running!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    # logging disable করা হলো যাতে ফ্লাস্কের ফালতু লগ কনসোল না ভরে
    import logging as flask_logging
    log = flask_logging.getLogger('werkzeug')
    log.setLevel(flask_logging.ERROR)
    web_app.run(host="0.0.0.0", port=port, use_reloader=False)

# ================= LOGGING =================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# ================= SHEETS CONNECTION =================

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
                    "timestamp", "event", "user_id", "username", "chat_id",
                    "query", "normalized_query", "status", "matched_title", "note",
                ]
            )

        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet

    except Exception as e:
        logging.error(f"Google Sheet Auth Error: {e}")
        return None, None, None

# ================= BACKGROUND SYNC TASK =================
# এই টাস্কটি প্রতি ১০ সেকেন্ড পরপর জমাকৃত লগ এবং নতুন ইউজারদের ডাটা শিটে পাঠাবে
# এতে বট কখনো ইউজারকে অপেক্ষা করাবে না এবং গুগল API লিমিট খাবে না।

async def background_sync_task():
    while True:
        await asyncio.sleep(10) # ১০ সেকেন্ড পর পর চেক করবে
        
        _, user_sheet, log_sheet = get_sheets()
        if not log_sheet or not user_sheet:
            continue

        # লগ ব্যাচ প্রসেসিং
        if LOG_QUEUE:
            batch_logs = LOG_QUEUE[:100] # একসাথে সর্বোচ্চ ১০০টা নিবে
            del LOG_QUEUE[:100]
            try:
                await asyncio.to_thread(log_sheet.append_rows, batch_logs, value_input_option="USER_ENTERED")
            except Exception as e:
                logging.error(f"Batch Log error: {e}")
                LOG_QUEUE.extend(batch_logs) # ফেইল হলে আবার queue তে ফেরত দিবে

        # নতুন ইউজার ব্যাচ প্রসেসিং
        if NEW_USER_QUEUE:
            users_to_add = list(NEW_USER_QUEUE)[:50]
            rows_to_add = [[str(uid)] for uid in users_to_add]
            for uid in users_to_add:
                NEW_USER_QUEUE.remove(uid)
            try:
                await asyncio.to_thread(user_sheet.append_rows, rows_to_add)
            except Exception as e:
                logging.error(f"Batch User error: {e}")
                NEW_USER_QUEUE.update(users_to_add)

# ================= CACHE HELPERS =================

def invalidate_book_cache():
    BOOK_CACHE["ts"] = 0.0

# ================= TEXT UTIL (অপরিবর্তিত) =================

def normalize(text):
    if not text: return ""
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
    if not text: return ""
    out = [BENGALI_LATIN_MAP.get(ch, ch) for ch in str(text)]
    result = "".join(out)
    result = re.sub(r"[^a-zA-Z0-9\s]+", " ", result)
    return re.sub(r"\s+", " ", result).strip().lower()

def generate_auto_aliases(title):
    aliases = set()
    if not title: return aliases
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
            if len(pair) >= 3: variants.add(pair)

    latin = transliterate_bengali_to_latin(raw)
    if latin:
        variants.update({latin, normalize(latin), normalize(latin.replace(" ", ""))})
        latin_tokens = [normalize(t) for t in tokenize(latin) if len(normalize(t)) >= 2]
        variants.update(latin_tokens)

    return {v for v in variants if v and len(v) >= 2}

def parse_manual_aliases(value):
    aliases = set()
    if not value: return aliases
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
        if not row or len(row) < 2: continue
        title, file_id = str(row[0]).strip(), str(row[1]).strip()
        if not title or not file_id: continue

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

async def get_book_cache(force=False):
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
        logging.error(f"Book cache load error: {e}")
        return BOOK_CACHE["rows"], BOOK_CACHE["indexed"], BOOK_CACHE["titles"], BOOK_CACHE["lookup"]

async def load_users_initial():
    _, user_sheet, _ = get_sheets()
    if not user_sheet: return
    try:
        values = await asyncio.to_thread(user_sheet.col_values, 1)
        ids = {int(str(x).strip()) for x in values[1:] if str(x).strip().isdigit()}
        USER_CACHE["ids"] = ids
        USER_CACHE["ts"] = time.time()
    except Exception as e:
        logging.error(f"User load error: {e}")

# ================= ASYNC LOGS & USERS (FAST) =================

async def append_log(event, user_id, username, chat_id, query, normalized_query, status, matched_title="", note=""):
    # সরাসরি শিটে না লিখে মেমোরিতে রাখা হচ্ছে। ব্যাকগ্রাউন্ড টাস্ক এটা সেভ করবে।
    row = [
        time.strftime("%Y-%m-%d %H:%M:%S"),
        event, str(user_id or ""), str(username or ""), str(chat_id or ""),
        str(query or ""), str(normalized_query or ""), str(status or ""),
        str(matched_title or ""), str(note or "")
    ]
    LOG_QUEUE.append(row)

async def ensure_user_saved(user_id):
    # ইন-মেমোরি চেক, সুপার ফাস্ট!
    if user_id not in USER_CACHE["ids"]:
        USER_CACHE["ids"].add(user_id)
        NEW_USER_QUEUE.add(user_id)

# ================= BOOK EXTRACTION =================

def derive_book_name_from_caption_or_filename(caption, file_name):
    if caption:
        for raw_line in str(caption).splitlines():
            line = clean_title_like_text(raw_line)
            if not line or is_url_like(line) or line.lower().startswith("forwarded from"): continue
            if line.lower().startswith("/upload"):
                line = line.split(maxsplit=1)[1].strip() if len(line.split(maxsplit=1)) > 1 else ""
                line = clean_title_like_text(line)
            line = remove_urls(line)
            if clean_title_like_text(line): return clean_title_like_text(line)

    if file_name:
        base = clean_title_like_text(re.sub(r"\s+", " ", os.path.splitext(str(file_name))[0].replace("_", " ").replace("-", " ")))
        if base and not is_url_like(base): return base
    return "অজানা বই"

# ================= KEYBOARD =================

def make_inline_keyboard(titles):
    token = str(time.time_ns())[-10:]
    titles = titles[:10]
    _callback_cache[token] = titles

    keyboard = [[InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")] for i, title in enumerate(titles)]
    return InlineKeyboardMarkup(keyboard)

# ================= DELIVERY =================

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in _chat_send_locks:
        _chat_send_locks[chat_id] = asyncio.Lock()
    return _chat_send_locks[chat_id]

async def send_books_by_rows(bot, chat_id: int, rows, batch_size: int = 3, batch_pause: float = 0.3):
    sent, seen = 0, set()
    async with get_chat_lock(chat_id):
        for i, row in enumerate(rows, start=1):
            book_name, file_id = str(row[0]).strip(), str(row[1]).strip()
            if not book_name or not file_id or (book_name, file_id) in seen: continue
            seen.add((book_name, file_id))

            try:
                await bot.send_document(chat_id=chat_id, document=file_id, caption=f"📘 {book_name}")
                sent += 1
            except Exception as e:
                logging.error(f"Send err for {book_name}: {e}")

            if i % batch_size == 0:
                await asyncio.sleep(batch_pause)
            else:
                await asyncio.sleep(0.05) # সামান্য গ্যাপ, টেলিগ্রাম রেট লিমিট এড়াতে
    return sent

async def queue_book_delivery(context, chat_id: int, rows, user_id: int, username: str, query_text: str, normalized_query: str, note: str = ""):
    total = len(rows)
    ack = "📚 বই পাওয়া গেছে, পাঠানো শুরু করছি..." if total == 1 else f"📚 {total}টি বই পাওয়া গেছে, পাঠানো শুরু করছি..."
    await context.bot.send_message(chat_id=chat_id, text=ack)

    # ব্যাকগ্রাউন্ডে বই পাঠানো হবে, ইউজার আটকে থাকবে না
    context.application.create_task(send_books_by_rows(context.bot, chat_id, rows))
    
    preview_title = str(rows[0][0]).strip() if rows and rows[0] else ""
    await append_log("search", user_id, username, chat_id, query_text, normalized_query, "QUEUED", preview_title, note)

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

        # 1 & 2) Exact and Partial Match
        matched = []
        match_type = ""

        if user_norm in lookup:
            matched = lookup[user_norm]
            match_type = "exact"
        else:
            matched = [row for row, title_norm in indexed_data if user_norm in title_norm or title_norm in user_norm]
            match_type = "partial"

        if matched:
            if len(matched) <= 5: # সর্বোচ্চ ৫টি বই সরাসরি পাঠাবে
                await queue_book_delivery(context, chat_id, matched, user_id, username, user_text_raw, user_norm, match_type)
                return
            else:
                # ৫টির বেশি হলে বাটন দেখাবে (টেলিগ্রাম Rate limit ও বট হ্যাং হওয়া এড়াতে)
                suggestions = []
                for row in matched:
                    title = str(row[0]).strip()
                    if title and title not in suggestions: suggestions.append(title)
                    if len(suggestions) >= 10: break # বাটনে সর্বোচ্চ ১০টা অপশন দেখাবে

                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📚 '{user_text_raw}' সম্পর্কিত প্রায় *{len(matched)}* টি বই পাওয়া গেছে। একসাথে এত বই পাঠালে বট স্লো হয়ে যায়।\n\nদয়া করে নিচের তালিকা থেকে নির্দিষ্ট বইটি বেছে নিন:",
                    reply_markup=make_inline_keyboard(suggestions),
                    parse_mode="Markdown"
                )
                await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "TOO_MANY_MATCHES", suggestions[0] if suggestions else "")
                return

        # 3) Fuzzy match
        close_keys = get_close_matches(user_norm, list(lookup.keys()), n=5, cutoff=0.72)
        if close_keys:
            suggestions = []
            for key in close_keys:
                for row in lookup.get(key, []):
                    title = str(row[0]).strip()
                    if title and title not in suggestions: suggestions.append(title)
                    if len(suggestions) >= 10: break
                if len(suggestions) >= 10: break

            if suggestions:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🤖 আমি কয়েকটা সম্ভাব্য বই খুঁজে পেয়েছি:",
                    reply_markup=make_inline_keyboard(suggestions)
                )
                await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "SUGGESTION", suggestions[0])
                return

        await context.bot.send_message(chat_id=chat_id, text="❌ বইটি খুঁজে পাওয়া যাচ্ছে না")
        await append_log("search", user_id, username, chat_id, user_text_raw, user_norm, "MISS")

    except Exception as e:
        logging.error(f"Search error: {e}")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ সার্ভারে সমস্যা হয়েছে। একটু পর চেষ্টা করুন।")

# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user_saved(update.effective_user.id)
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
        await update.message.reply_text("ব্যবহার: /admin আপনার মেসেজ")
        return

    user = update.effective_user
    chat_id = update.effective_chat.id
    username = f"@{user.username}" if user.username else "No username"

    msg = f"📩 নতুন ইউজার মেসেজ\n\nনাম: {user.full_name}\nইউজারনেম: {username}\nইউজার আইডি: {user.id}\nচ্যাট আইডি: {chat_id}\n\nমেসেজ:\n{body}"
    sent = await context.bot.send_message(chat_id=ADMIN_ID, text=msg)
    _admin_reply_map[sent.message_id] = chat_id
    await update.message.reply_text("✅ মেসেজ এডমিনের কাছে পাঠানো হয়েছে")
    await append_log("admin_message", user.id, username, chat_id, body, normalize(body), "SENT")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
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
                await asyncio.sleep(0.05) # রেট লিমিট এড়াতে গ্যাপ
            except Exception:
                failed += 1
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"✅ Broadcast শেষ\nসফল: {success}\nব্যর্থ: {failed}")

    context.application.create_task(run_broadcast())

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    book_sheet, _, _ = get_sheets()
    
    target = update.message.reply_to_message
    if not target or not target.document:
        await update.message.reply_text("ব্যবহার: ডক ফাইলে রিপ্লাই দিয়ে /upload বা /upload বইয়ের নাম দিন।")
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    book_name = parts[1].strip() if len(parts) > 1 else (target.document.file_name or "অজানা বই")
    
    try:
        await asyncio.to_thread(book_sheet.append_row, [book_name, target.document.file_id, target.caption or ""])
        invalidate_book_cache()
        await update.message.reply_text(f"✅ বই আপলোড হয়েছে: {book_name}")
    except Exception as e:
        await update.message.reply_text("⚠️ এরর হয়েছে।")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    users_count = len(USER_CACHE["ids"])
    books_count = len(BOOK_CACHE["rows"])
    await update.message.reply_text(f"📊 লাইভ স্ট্যাটাস:\nইউজার: {users_count}\nমোট বই: {books_count}\nপেন্ডিং লগ (Queue): {len(LOG_QUEUE)}")

# ================= HANDLERS =================

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    user = update.effective_user
    text = update.message.text.strip()

    if user.id == ADMIN_ID and update.message.reply_to_message:
        target_chat_id = _admin_reply_map.get(update.message.reply_to_message.message_id)
        if target_chat_id:
            try:
                await context.bot.send_message(chat_id=target_chat_id, text=f"👨‍💼 এডমিন: {text}")
                await update.message.reply_text("✅ রিপ্লাই পাঠানো হয়েছে")
            except Exception:
                await update.message.reply_text("⚠️ পাঠানো যায়নি")
        return

    await process_book_search(context, update.effective_chat.id, text, user.id, f"@{user.username}" if user.username else "")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("pick|"):
        _, token, idx = data.split("|", 2)
        titles = _callback_cache.get(token, [])
        if not titles or int(idx) >= len(titles):
            await query.message.reply_text("⚠️ সেশন শেষ, আবার সার্চ করুন।")
            return
            
        selected_title = titles[int(idx)]
        await query.message.edit_text(f"🔎 খোঁজা হচ্ছে: {selected_title}")
        await process_book_search(context, query.message.chat.id, selected_title, query.from_user.id, f"@{query.from_user.username}" if query.from_user.username else "")

# ================= MAIN RUNNER =================
async def startup_task(app: Application):
    # স্টার্টআপের সময় একবার ইউজার এবং বই ক্যাশ করে নিবে
    await load_users_initial()
    await get_book_cache()
    # ব্যাকগ্রাউন্ড ব্যাচ সেভ টাস্ক চালু করা হলো
    asyncio.create_task(background_sync_task())

def main():
    # Render Keep-Alive Server
    Thread(target=run_web, daemon=True).start()

    # concurrent_updates 100 করে দেওয়া হয়েছে যাতে একসাথে ১০০ জন মেসেজ দিলেও হ্যান্ডেল হয়
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(100).build()

    # Handlers
    app.add_handler(CommandHandler(["start", "help", "admin", "broadcast", "upload", "stats"], 
                                   lambda u, c: globals()[u.message.text.split()[0][1:].lower() + ("_command" if u.message.text.split()[0][1:].lower() in ["help", "admin", "broadcast", "upload"] else "")](u, c)))
    
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # Startup Task Hook
    app.job_queue.run_once(lambda context: asyncio.create_task(startup_task(app)), 1)

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
