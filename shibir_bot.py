import logging
import gspread
import os
from difflib import get_close_matches
import google.generativeai as genai
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)
from flask import Flask
from threading import Thread

# ================= CONFIG =================

TOKEN = '8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y'
ADMIN_ID = 8596482199
SHEET_NAME = "MyBotDB"
GEMINI_API_KEY = "AIzaSyBJnqVnln-PtyPxpOYptJxy0Pisb8nxmHM"

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

# ================= CACHE (NEW) =================

_cached_book_sheet = None
_cached_user_sheet = None

# ================= FLASK =================

web_app = Flask(__name__)

@web_app.route('/')
def home():
    return "Library Bot is Optimized!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

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

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ================= UTIL =================

def normalize(text):
    if not text:
        return ""
    return text.lower().replace(" ", "").strip()

def contains_volume(text):
    keywords = ["খণ্ড", "vol", "volume"]
    text = text.lower()
    return any(k in text for k in keywords)

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
        "২. এডমিন: /admin লিখে আপনার কথা লিখুন।"
    )

# ================= SEARCH =================

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text_raw = update.message.text.strip()
    user_text = normalize(user_text_raw)

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action="typing"
    )

    book_sheet, user_sheet = get_sheets()

    if not book_sheet:
        await update.message.reply_text("❌ Database error")
        return

    try:
        # Save user
        if user_sheet:
            uid = str(update.effective_user.id)
            users = user_sheet.col_values(1)

            if uid not in users:
                user_sheet.append_row([uid])

        all_data = book_sheet.get_all_values()[1:]

        # clean rows
        all_data = [row for row in all_data if row and row[0]]

        book_names = [row[0] for row in all_data]

        found_books = []

        # ================= CASE: volume =================
        if contains_volume(user_text_raw):

            for row in all_data:
                if user_text in normalize(row[0]):
                    found_books.append(row)

            if found_books:
                for book in found_books:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=book[1],
                        caption=f"📘 {book[0]}"
                    )
                return

        # ================= NORMAL SEARCH =================
        matched = []

        for row in all_data:
            if user_text in normalize(row[0]):
                matched.append(row)

        if matched:
            if len(matched) > 1:
                keyboard = [
                    [InlineKeyboardButton(b[0], callback_data=f"book|{b[0]}")]
                    for b in matched[:10]  # limit buttons
                ]

                await update.message.reply_text(
                    "📚 আপনার বই নির্বাচন করুন:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            for book in matched:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=book[1],
                    caption=f"📘 {book[0]}"
                )
            return

        # ================= FUZZY =================
        matches = get_close_matches(
            user_text,
            [normalize(b) for b in book_names],
            n=5,
            cutoff=0.6  # slightly stricter for accuracy
        )

        if matches:
            for row in all_data:
                if normalize(row[0]) in matches:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=row[1],
                        caption=f"📘 {row[0]}"
                    )
            return

        # ================= GEMINI =================
        prompt = f"""
User wrote: {user_text_raw}

Convert to correct Bengali book name.

Rules:
- Only book name
- No explanation
"""

        response = ai_model.generate_content(prompt)

        if not response or not response.text:
            await update.message.reply_text("❌ বুঝতে পারিনি")
            return

        ai_res = response.text.strip().replace("*", "").split("\n")[0]

        ai_matches = get_close_matches(
            normalize(ai_res),
            [normalize(b) for b in book_names],
            n=5,
            cutoff=0.6
        )

        if ai_matches:
            for row in all_data:
                if normalize(row[0]) in ai_matches:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=row[1],
                        caption=f"📘 {row[0]}"
                    )
            return

        await update.message.reply_text(
            f"❌ বই পাওয়া যায়নি\n\n👉 আপনি কি এটা বুঝাতে চেয়েছেন?\n{ai_res}"
        )

    except Exception as e:
        logging.error(f"Search error: {e}")
        await update.message.reply_text("⚠️ Error occurred")

# ================= CALLBACK =================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    book_name = query.data.split("|")[1]

    book_sheet, _ = get_sheets()
    all_data = book_sheet.get_all_values()[1:]

    for row in all_data:
        if row[0] == book_name:
            await query.message.reply_document(
                document=row[1],
                caption=f"📘 {row[0]}"
            )
            return

# ================= ADMIN =================

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet = get_sheets()

    total_users = len(user_sheet.col_values(1)) - 1
    total_books = len(book_sheet.col_values(1)) - 1

    await update.message.reply_text(
        f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি"
    )

# (remaining admin, broadcast, upload same as before — unchanged for stability)

# ================= MAIN =================

def main():
    Thread(target=run_web).start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))

    app.add_handler(CallbackQueryHandler(handle_callback))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
