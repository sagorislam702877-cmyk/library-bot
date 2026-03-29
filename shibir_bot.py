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

# ================= LOGGING =================

logging.basicConfig(level=logging.INFO)

# ================= FLASK =================

web_app = Flask(__name__)

@web_app.route('/')
def home():
    return "Library Bot is Running"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

# ================= SHEETS =================

def get_sheets():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open(SHEET_NAME)
        return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")

    except Exception as e:
        logging.error(f"Sheet Error: {e}")
        return None, None

# ================= GEMINI =================

def suggest_books(user_text, book_list):
    prompt = f"""
User input: {user_text}

From the following book list, suggest the closest matching book names.

Book list:
{book_list}

Rules:
- Return only 3 to 5 book names
- One per line
- No explanation
"""

    try:
        response = ai_model.generate_content(prompt)

        if not response or not response.text:
            return []

        suggestions = response.text.strip().split("\n")
        suggestions = [s.strip() for s in suggestions if s.strip()]

        return suggestions[:5]

    except Exception as e:
        logging.error(f"Gemini error: {e}")
        return []

# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_text = (
        "আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখুন।\n"
        "এডমিনের সাথে কথা বলতে /admin + আপনার টেক্সটি লিখুন"
    )
    await update.message.reply_text(start_text)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "বট ব্যবহারের নিয়মাবলী:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. এডমিন: নতুন বই বা সমস্যার জন্য /admin লিখে আপনার কথাটি লিখুন।\n"
        "যেমন : /admin আমার অমুক বইটি প্রয়োজন"
    )
    await update.message.reply_text(help_text)

# ================= SEARCH =================

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip().lower()

    # typing indicator
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action="typing"
    )

    book_sheet, user_sheet = get_sheets()

    if not book_sheet:
        await update.message.reply_text("❌ Database error")
        return

    try:
        # Save user (avoid duplicate)
        if user_sheet:
            user_id = str(update.effective_user.id)
            users = user_sheet.col_values(1)

            if user_id not in users:
                user_sheet.append_row([user_id])

        all_data = book_sheet.get_all_values()[1:]
        book_names = [row[0] for row in all_data]

        # Direct match
        for row in all_data:
            if user_text in row[0].lower():
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=row[1],
                    caption=f"আপনার বই: {row[0]}"
                )
                return

        # Fuzzy match
        matches = get_close_matches(user_text, book_names, n=5, cutoff=0.5)

        if matches:
            keyboard = [
                [InlineKeyboardButton(name, callback_data=f"book|{name}")]
                for name in matches
            ]

            await update.message.reply_text(
                "এইগুলোর মধ্যে কি আপনার বইটি আছে?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # Gemini suggestions
        suggestions = suggest_books(user_text, book_names)

        if suggestions:
            keyboard = [
                [InlineKeyboardButton(name, callback_data=f"book|{name}")]
                for name in suggestions
            ]

            await update.message.reply_text(
                "এইগুলোর মধ্যে কি আপনার বইটি আছে?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        await update.message.reply_text("❌ বুঝতে পারিনি")

    except Exception as e:
        logging.error(f"Search error: {e}")
        await update.message.reply_text("⚠️ সাময়িক সমস্যা, আবার চেষ্টা করুন")

# ================= BUTTON HANDLER =================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("book|"):
        book_name = data.split("|")[1]

        book_sheet, _ = get_sheets()

        if not book_sheet:
            await query.message.reply_text("❌ Database error")
            return

        all_data = book_sheet.get_all_values()[1:]

        for row in all_data:
            if row[0].lower() == book_name.lower():
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=row[1],
                    caption=f"📘 {row[0]}"
                )
                return

        await query.message.reply_text("❌ এই বইটি পাওয়া যায়নি")

# ================= ADMIN =================

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet = get_sheets()

    if not book_sheet or not user_sheet:
        await update.message.reply_text("❌ Error loading data")
        return

    total_users = len(user_sheet.col_values(1)) - 1
    total_books = len(book_sheet.col_values(1)) - 1

    await update.message.reply_text(
        f"স্ট্যাটাস:\nমোট ইউজার: {total_users}\nমোট বই: {total_books}"
    )

# ================= MAIN =================

def main():
    Thread(target=run_web).start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    app.add_handler(CallbackQueryHandler(button_handler))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
