import logging
import gspread
import os
from difflib import get_close_matches
import google.generativeai as genai
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. কনফিগারেশন
TOKEN = '8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y'
ADMIN_ID = 8596482199
SHEET_NAME = "MyBotDB"
GEMINI_API_KEY = "AIzaSyAuT06iRlvTPkDtzkyaV4u7eW_rMLqXSsc"

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

web_app = Flask('')

@web_app.route('/')
def home():
    return "Library Bot is Optimized!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

# ২. শিট কানেকশন
def get_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")
    except Exception as e:
        logging.error(f"Sheet Error: {e}")
        return None, None

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৩. ইউজার কমান্ডস
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

# ৪. সুপার সার্চ ইঞ্জিন
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip().lower()
    book_sheet, _ = get_sheets()

    if not book_sheet:
        await update.message.reply_text("❌ Database error")
        return

    try:
        all_data = book_sheet.get_all_values()[1:]
        book_names = [row[0] for row in all_data]

        # ধাপ ১: Direct match
        found_books = []
        for row in all_data:
            if user_text in row[0].lower():
                found_books.append(row)

        if found_books:
            for book in found_books:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=book[1],
                    caption=f"আপনার বই: {book[0]}"
                )
            return

        # ধাপ ২: Fuzzy match
        matches = get_close_matches(user_text, book_names, n=5, cutoff=0.5)

        if matches:
            for row in all_data:
                if row[0] in matches:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=row[1],
                        caption=f"আপনার বই: {row[0]}"
                    )
            return

        # ধাপ ৩: AI correction
        prompt = f"""
User wrote: {user_text}

Convert to correct Bengali book name.

Rules:
- Only book name
- No explanation
- No extra text
"""

        response = ai_model.generate_content(prompt)

        if not response.text:
            await update.message.reply_text("❌ বুঝতে পারিনি")
            return

        ai_res = response.text.strip().replace("*", "").replace("\n", "")

        # ধাপ ৪: AI + Fuzzy
        matches = get_close_matches(ai_res, book_names, n=5, cutoff=0.5)

        if matches:
            for row in all_data:
                if row[0] in matches:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=row[1],
                        caption=f"আপনার বই: {row[0]}"
                    )
            return

        await update.message.reply_text(f"❌ বই পাওয়া যায়নি\n\n👉 আপনি কি এটা বুঝাতে চেয়েছেন?\n{ai_res}")

    except Exception as e:
        logging.error(f"Search error: {e}")
        await update.message.reply_text("⚠️ Error occurred")

# ৫. এডমিন ফিচারস
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    book_sheet, user_sheet = get_sheets()

    if book_sheet and user_sheet:
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1

        await update.message.reply_text(
            f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি"
        )

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)

    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার কথাটি লিখুন।")
        return

    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"মেসেজ!\nID: {update.effective_user.id}\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে: /reply {update.effective_user.id} বার্তা"
    )

    await update.message.reply_text("মেসেজটি এডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2:
        return

    target_id = context.args[0]
    reply_msg = " ".join(context.args[1:])

    await context.bot.send_message(
        chat_id=target_id,
        text=f"এডমিন রিপ্লাই:\n\n{reply_msg}"
    )

    await update.message.reply_text("পাঠানো হয়েছে।")

async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    doc = update.message.document
    name = (update.message.caption or doc.file_name).replace(".pdf", "").replace("_", " ").strip()

    book_sheet, _ = get_sheets()

    if book_sheet:
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"সেভ হয়েছে: {name}")

# MAIN
def main():
    Thread(target=run_web).start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))

    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))

    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
