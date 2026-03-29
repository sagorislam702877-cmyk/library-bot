import logging
import gspread
import os
import asyncio
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
ai_model = genai.GenerativeModel('gemini-1.5-flash-latest')

web_app = Flask(__name__)
@web_app.route('/')
def home(): return "Library Bot is Online and Search Logic Fixed!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

# ================= SHEETS =================

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

# ================= USER COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখে মেসেজ দিন।\n"
        "এডমিনের সাথে কথা বলতে /admin + আপনার টেক্সটি লিখুন"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 বট ব্যবহারের নিয়মাবলী:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখুন। আংশিক মিল থাকলেও বট বই খুঁজে দেবে।\n"
        "২. এডমিন: এডমিনের সাথে যোগাযোগ করতে /admin লিখে আপনার কথাটি লিখুন।\n\n"
        "যেমন: `/admin ভাই, আমার অমুক বইটি প্রয়োজন।`"
    )

# ================= SEARCH LOGIC =================

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip().lower()
    book_sheet, user_sheet = get_sheets()
    if not book_sheet: return

    # ইউজার সেভ করা
    try:
        uid = str(update.effective_user.id)
        if uid not in user_sheet.col_values(1): user_sheet.append_row([uid])
    except: pass

    all_data = book_sheet.get_all_values()[1:]
    found_books = [row for row in all_data if user_text in row[0].lower()]

    if found_books:
        for book in found_books:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=book[1], caption=f"📘 {book[0]}")
    else:
        # কোনো মিল না পাওয়া গেলে AI সহায়তা
        prompt = (
            f"User searched for: '{user_text}'. It is not in the list. "
            "Suggest 3 most similar book names from general knowledge in Bengali. "
            "Format: Name1, Name2, Name3. No extra text."
        )
        try:
            response = ai_model.generate_content(prompt)
            suggestions = response.text.strip().split(',')
            keyboard = [[InlineKeyboardButton(name.strip(), callback_data=f"search|{name.strip()}")] for name in suggestions[:3]]
            await update.message.reply_text(
                f"❌ '{user_text}' নামে সরাসরি কোনো বই পাওয়া যায়নি। আপনি কি নিচের কোনোটি খুঁজছেন?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except:
            await update.message.reply_text("দুঃখিত, বইটি খুঁজে পাওয়া যাচ্ছে না। সঠিক বানান ব্যবহার করে চেষ্টা করুন।")

async def callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split('|')
    
    if data[0] == "search":
        search_term = data[1].lower()
        book_sheet, _ = get_sheets()
        all_data = book_sheet.get_all_values()[1:]
        found = [row for row in all_data if search_term in row[0].lower()]
        
        if found:
            for book in found:
                await query.message.reply_document(document=book[1], caption=f"📘 {book[0]}")
        else:
            await query.message.reply_text(f"দুঃখিত, '{data[1]}' বইটি আমাদের তালিকায় নেই।")

# ================= ADMIN SECTION =================

async def admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("ব্যবহার: `/admin আপনার মেসেজ`")
        return
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"📩 নতুন মেসেজ!\nID: `{update.effective_user.id}`\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে: `/reply {update.effective_user.id} আপনার বার্তা`"
    )
    await update.message.reply_text("মেসেজটি এডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    try:
        target_id, reply_msg = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"👨‍💼 এডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text("রিপ্লাই পাঠানো হয়েছে।")
    except: await update.message.reply_text("পাঠানো সম্ভব হয়নি।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg: return
    _, user_sheet = get_sheets()
    uids = user_sheet.col_values(1)[1:]
    count = 0
    for uid in uids:
        try:
            await context.bot.send_message(chat_id=uid, text=msg)
            count += 1
        except: pass
    await update.message.reply_text(f"মোট {count} জন ইউজারকে মেসেজ পাঠানো হয়েছে।")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    book_sheet, user_sheet = get_sheets()
    total_u = len(user_sheet.col_values(1)) - 1
    total_b = len(book_sheet.col_values(1)) - 1
    await update.message.reply_text(f"📊 স্ট্যাটাস:\nমোট ইউজার: {max(0, total_u)} জন\nমোট বই: {max(0, total_b)} টি")

async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    name = (update.message.caption if update.message.caption else doc.file_name).replace(".pdf", "").replace("_", " ").strip()
    book_sheet, _ = get_sheets()
    if book_sheet:
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"✅ সেভ হয়েছে: {name}")

# ================= MAIN =================

def main():
    Thread(target=run_web).start()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", admin_message))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("stats", stats))
    
    app.add_handler(CallbackQueryHandler(callback_query))
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
