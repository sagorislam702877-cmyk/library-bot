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

# ================= CONFIG =================

TOKEN = '8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y'
ADMIN_ID = 8596482199
SHEET_NAME = "MyBotDB"
GEMINI_API_KEY = "AIzaSyBJnqVnln-PtyPxpOYptJxy0Pisb8nxmHM"

genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

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

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ================= GEMINI =================

async def correct_book_name(user_text):
    prompt = f"""
User wrote a book name with spelling mistakes.

Task:
Correct it to the most accurate Bengali book name.

Rules:
- Only return the corrected book name
- No explanation
- No extra words
- No punctuation

Input:
{user_text}
"""

    try:
        response = ai_model.generate_content(prompt)

        if not response or not response.text:
            return None

        corrected = response.text.strip().replace("*", "").replace("\n", "").lower()
        return corrected if corrected else None

    except Exception as e:
        logging.error(f"Gemini error: {e}")
        return None

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
        "২. এডমিন: নতুন বই বা সমস্যার জন্য /admin লিখে আপনার কথাটি লিখুন।\n"
    )

# ================= SEARCH =================

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip().lower()

    book_sheet, user_sheet = get_sheets()

    if not book_sheet:
        await update.message.reply_text("❌ Database error")
        return

    try:
        # Save user
        if user_sheet:
            user_ids = user_sheet.col_values(1)
            if str(update.effective_user.id) not in user_ids:
                user_sheet.append_row([str(update.effective_user.id)])

        all_data = book_sheet.get_all_values()[1:]
        book_names = [row[0] for row in all_data]

        # Direct match
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

        # Fuzzy match
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

        # Gemini correction
        ai_res = await correct_book_name(user_text)

        if not ai_res:
            await update.message.reply_text("❌ বুঝতে পারিনি")
            return

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

        await update.message.reply_text(
            f"❌ বই পাওয়া যায়নি\n\n👉 আপনি কি এটা বুঝাতে চেয়েছেন?\n{ai_res}"
        )

    except Exception as e:
        logging.error(f"Search error: {e}")
        await update.message.reply_text("⚠️ Error occurred")

# ================= ADMIN =================

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

    target_id = int(context.args[0])
    reply_msg = " ".join(context.args[1:])

    await context.bot.send_message(
        chat_id=target_id,
        text=f"এডমিন রিপ্লাই:\n\n{reply_msg}"
    )

    await update.message.reply_text("পাঠানো হয়েছে।")

# ================= BROADCAST =================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    message = " ".join(context.args)

    if not message:
        await update.message.reply_text("Usage: /broadcast আপনার মেসেজ")
        return

    _, user_sheet = get_sheets()

    if not user_sheet:
        await update.message.reply_text("❌ User DB error")
        return

    users = user_sheet.col_values(1)[1:]

    success = 0

    for user_id in users:
        try:
            await context.bot.send_message(
                chat_id=int(user_id),
                text=f"📢 Broadcast:\n\n{message}"
            )
            success += 1
        except:
            pass

    await update.message.reply_text(f"✅ মোট {success} জন ইউজারকে মেসেজ পাঠানো হয়েছে")

# ================= UPLOAD =================

async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    doc = update.message.document
    name = (update.message.caption or doc.file_name).replace(".pdf", "").replace("_", " ").strip()

    book_sheet, _ = get_sheets()

    if book_sheet:
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"সেভ হয়েছে: {name}")

# ================= MAIN =================

def main():
    Thread(target=run_web).start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("broadcast", broadcast))

    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
