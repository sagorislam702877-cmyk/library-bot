import logging
import os
import re
import time
import uuid
from difflib import SequenceMatcher, get_close_matches
from threading import Thread

import gspread
import google.generativeai as genai
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
    ContextTypes
)

# ================= CONFIG =================

TOKEN = os.environ.get("BOT_TOKEN", "8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "8596482199"))
SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "AIzaSyBJnqVnln-PtyPxpOYptJxy0Pisb8nxmHM")

if GEMINI_API_KEY and GEMINI_API_KEY != "AIzaSyBJnqVnln-PtyPxpOYptJxy0Pisb8nxmHM":
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel("gemini-1.5-flash")
else:
    ai_model = None

# ================= CACHE =================

_cached_book_sheet = None
_cached_user_sheet = None

# callback cache: token -> [book titles]
_callback_cache = {}

# admin reply map: admin_message_id -> user_chat_id
_admin_reply_map = {}

# ================= FLASK =================

web_app = Flask(__name__)

@web_app.route("/")
def home():
    return "Library Bot is Optimized!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port)

# ================= LOGGING =================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# ================= SHEETS =================

def get_sheets():
    global _cached_book_sheet, _cached_user_sheet

    if _cached_book_sheet and _cached_user_sheet:
        return _cached_book_sheet, _cached_user_sheet

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open(SHEET_NAME)

        _cached_book_sheet = spreadsheet.worksheet("Sheet1")
        _cached_user_sheet = spreadsheet.worksheet("Users")

        return _cached_book_sheet, _cached_user_sheet

    except Exception as e:
        logging.error(f"Sheet Error: {e}")
        return None, None

# ================= UTIL =================

def normalize(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)
    return text

def contains_volume(text):
    keywords = ["খণ্ড", "vol", "volume"]
    text = text.lower()
    return any(k in text for k in keywords)

def tokenize(text):
    return re.findall(r"[\w\u0980-\u09FF]+", str(text).lower(), flags=re.UNICODE)

def rank_candidates(query, titles, limit=20):
    qn = normalize(query)
    q_tokens = set(tokenize(query))

    scored = []
    for title in titles:
        tn = normalize(title)
        t_tokens = set(tokenize(title))

        ratio = SequenceMatcher(None, qn, tn).ratio()
        token_overlap = 0.0
        if q_tokens and t_tokens:
            token_overlap = len(q_tokens & t_tokens) / max(len(q_tokens), len(t_tokens))

        score = (ratio * 0.8) + (token_overlap * 0.2)
        scored.append((score, title))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [title for _, title in scored[:limit]]

def clean_candidate_line(line):
    line = line.strip()
    line = re.sub(r"^[\-\*\d\.\)\s]+", "", line)
    return line.strip()

def make_inline_keyboard(titles):
    token = uuid.uuid4().hex[:10]
    _callback_cache[token] = titles[:10]

    keyboard = [
        [InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")]
        for i, title in enumerate(titles[:10])
    ]
    return InlineKeyboardMarkup(keyboard)

async def ensure_user_saved(update, user_sheet):
    if not user_sheet:
        return
    try:
        uid = str(update.effective_user.id)
        users = user_sheet.col_values(1)
        if uid not in users:
            user_sheet.append_row([uid])
    except Exception as e:
        logging.error(f"User save error: {e}")

async def send_books_by_rows(context: ContextTypes.DEFAULT_TYPE, chat_id: int, rows):
    sent = 0
    seen = set()

    for row in rows:
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
            await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=f"📘 {book_name}"
            )
            sent += 1
        except BadRequest as e:
            logging.error(f"Send document failed for {book_name}: {e}")
        except Exception as e:
            logging.error(f"Send document error for {book_name}: {e}")

    return sent

async def upsert_book(book_sheet, book_name, file_id):
    try:
        values = book_sheet.get_all_values()
        rows = values[1:] if len(values) > 1 else []

        target_norm = normalize(book_name)

        for idx, row in enumerate(rows, start=2):
            if not row:
                continue
            existing_name = str(row[0]).strip() if len(row) > 0 else ""
            if existing_name and normalize(existing_name) == target_norm:
                book_sheet.update(f"A{idx}:B{idx}", [[book_name, file_id]])
                return "updated"

        book_sheet.append_row([book_name, file_id])
        return "added"

    except Exception as e:
        logging.error(f"Upsert book error: {e}")
        return "error"

async def get_gemini_suggestions(user_text_raw, candidate_titles):
    if not ai_model:
        return candidate_titles[:5]

    candidate_block = "\n".join([f"{i+1}. {title}" for i, title in enumerate(candidate_titles[:20])])

    prompt = f"""
ইউজারের লেখা: {user_text_raw}

নিচের বইয়ের তালিকা থেকে সবচেয়ে সম্ভাব্য 5টি বই বেছে দাও:
{candidate_block}

নিয়ম:
- শুধু ওই তালিকার ভেতরের বইয়ের নাম দেবে
- এক লাইনে একটি করে
- সর্বোচ্চ 5টি
- কোনো ব্যাখ্যা নয়
"""

    try:
        response = ai_model.generate_content(prompt)
        if not response or not response.text:
            return candidate_titles[:5]

        lines = [clean_candidate_line(x) for x in response.text.splitlines()]
        lines = [x for x in lines if x]

        normalized_map = {normalize(title): title for title in candidate_titles}
        final = []

        for line in lines:
            ln = normalize(line)
            if ln in normalized_map:
                final.append(normalized_map[ln])
                continue

            close = get_close_matches(ln, list(normalized_map.keys()), n=1, cutoff=0.8)
            if close:
                final.append(normalized_map[close[0]])

        # unique preserve order
        unique_final = []
        seen = set()
        for item in final:
            if item not in seen:
                seen.add(item)
                unique_final.append(item)

        if unique_final:
            return unique_final[:5]

        return candidate_titles[:5]

    except Exception as e:
        logging.error(f"Gemini suggestion error: {e}")
        return candidate_titles[:5]

async def process_book_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_text_raw: str, user_id: int, allow_gemini=True):
    book_sheet, user_sheet = get_sheets()

    if not book_sheet:
        await context.bot.send_message(chat_id=chat_id, text="❌ Database error")
        return

    await ensure_user_saved(
        type("obj", (), {
            "effective_user": type("u", (), {"id": user_id})(),
        })(),
        user_sheet
    )

    try:
        all_data = book_sheet.get_all_values()
        all_data = all_data[1:] if len(all_data) > 1 else []
        all_data = [row for row in all_data if row and len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip()]

        if not all_data:
            await context.bot.send_message(chat_id=chat_id, text="❌ বই পাওয়া যায়নি")
            return

        user_norm = normalize(user_text_raw)
        book_names = [row[0] for row in all_data]

        # ================= DIRECT PARTIAL MATCH =================
        matched = []
        for row in all_data:
            title_norm = normalize(row[0])
            if user_norm and (user_norm in title_norm or title_norm in user_norm):
                matched.append(row)

        if matched:
            await send_books_by_rows(context, chat_id, matched)
            return

        # ================= GEMINI ONLY AFTER NO MATCH =================
        if allow_gemini:
            candidate_titles = rank_candidates(user_text_raw, book_names, limit=20)
            suggestions = await get_gemini_suggestions(user_text_raw, candidate_titles)

            if suggestions:
                keyboard = make_inline_keyboard(suggestions)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🤖 আমি কয়েকটা সম্ভাব্য বই খুঁজে পেয়েছি:",
                    reply_markup=keyboard
                )
                return

        await context.bot.send_message(chat_id=chat_id, text="❌ বইটি খুঁজে পাওয়া যাচ্ছে না")

    except Exception as e:
        logging.error(f"Search error: {e}")
        await context.bot.send_message(chat_id=chat_id, text="⚠️ Error occurred")

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
        "   যেমন: /admin ভাই আমার অমুক বই প্রয়োজন\n"
        "৩. স্ট্যাটাস: /stats\n"
        "৪. ব্রডকাস্ট: /broadcast আপনার মেসেজ\n"
        "৫. আপলোড: /upload + বইয়ের নাম (ডকুমেন্ট রিপ্লাই করে)\n"
    )

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    body = parts[1].strip() if len(parts) > 1 else ""

    if not body:
        await update.message.reply_text(
            "ব্যবহার: /admin আপনার মেসেজ\n"
            "উদাহরণ: /admin ভাই আমার অমুক বই প্রয়োজন"
        )
        return

    book_sheet, _ = get_sheets()

    user = update.effective_user
    user_name = user.full_name if user else "Unknown"
    user_username = f"@{user.username}" if user and user.username else "No username"
    chat_id = update.effective_chat.id

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

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet = get_sheets()
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

    users = user_sheet.col_values(1)[1:] if len(user_sheet.col_values(1)) > 1 else []
    user_ids = []

    for uid in users:
        uid = str(uid).strip()
        if uid.isdigit():
            user_ids.append(int(uid))

    user_ids = list(dict.fromkeys(user_ids))

    success = 0
    failed = 0

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=broadcast_text)
            success += 1
        except Exception:
            failed += 1

    await update.message.reply_text(
        f"✅ Broadcast শেষ হয়েছে\nসফল: {success}\nব্যর্থ: {failed}"
    )

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, _ = get_sheets()
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

    try:
        result = await upsert_book(book_sheet, book_name, file_id)
        if result == "updated":
            await update.message.reply_text(f"✅ বইটি আপডেট করা হয়েছে: {book_name}")
        elif result == "added":
            await update.message.reply_text(f"✅ বইটি আপলোড করা হয়েছে: {book_name}")
        else:
            await update.message.reply_text("⚠️ আপলোড করা যায়নি")
    except Exception as e:
        logging.error(f"Upload error: {e}")
        await update.message.reply_text("⚠️ Upload error")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet = get_sheets()
    if not book_sheet or not user_sheet:
        await update.message.reply_text("❌ Sheet error")
        return

    try:
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1

        await update.message.reply_text(
            f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি"
        )
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await update.message.reply_text("⚠️ Stats error")

# ================= TEXT HANDLER =================

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_id = update.effective_user.id
    text = update.message.text.strip()

    # Admin reply to user
    if user_id == ADMIN_ID:
        if update.message.reply_to_message:
            reply_to = update.message.reply_to_message.message_id
            target_chat_id = _admin_reply_map.get(reply_to)

            if target_chat_id:
                try:
                    await context.bot.send_message(
                        chat_id=target_chat_id,
                        text=f"👨‍💼 এডমিন: {text}"
                    )
                    await update.message.reply_text("✅ ইউজারকে রিপ্লাই পাঠানো হয়েছে")
                    return
                except Exception as e:
                    logging.error(f"Admin reply error: {e}")
                    await update.message.reply_text("⚠️ রিপ্লাই পাঠানো যায়নি")
                    return

        # Admin এর সাধারণ text search না ধরে নীরব থাকা
        return

    # Normal user search
    await process_book_search(
        context=context,
        chat_id=update.effective_chat.id,
        user_text_raw=text,
        user_id=user_id,
        allow_gemini=True
    )

# ================= DOCUMENT HANDLER (ADMIN UPLOAD) =================

async def handle_admin_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.document:
        return

    if update.effective_user.id != ADMIN_ID:
        return

    caption = (update.message.caption or "").strip()
    if not caption.lower().startswith("/upload"):
        return

    book_sheet, _ = get_sheets()
    if not book_sheet:
        await update.message.reply_text("❌ Sheet error")
        return

    parts = caption.split(maxsplit=1)
    book_name = parts[1].strip() if len(parts) > 1 else update.message.document.file_name or "অজানা বই"
    file_id = update.message.document.file_id

    try:
        result = await upsert_book(book_sheet, book_name, file_id)
        if result == "updated":
            await update.message.reply_text(f"✅ বইটি আপডেট করা হয়েছে: {book_name}")
        elif result == "added":
            await update.message.reply_text(f"✅ বইটি আপলোড করা হয়েছে: {book_name}")
        else:
            await update.message.reply_text("⚠️ আপলোড করা যায়নি")
    except Exception as e:
        logging.error(f"Admin document upload error: {e}")
        await update.message.reply_text("⚠️ Upload error")

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

            await process_book_search(
                context=context,
                chat_id=query.message.chat_id,
                user_text_raw=selected_title,
                user_id=query.from_user.id,
                allow_gemini=False
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

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CommandHandler(["admin", "Admin"], admin_command))
    app.add_handler(CommandHandler(["broadcast", "Broadcast"], broadcast_command))
    app.add_handler(CommandHandler(["upload", "Upload"], upload_command))
    app.add_handler(CommandHandler(["stats", "Stats"], stats))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_admin_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
