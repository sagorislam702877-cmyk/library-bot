import logging
import gspread
import os
import asyncio
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
def home(): return "Library Bot is Optimized!"

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

# ৩. ইউজার কমান্ডস (আপনার দেওয়া টেক্সট অনুযায়ী)
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

# ৪. সুপার সার্চ ইঞ্জিন (একাধিক খণ্ড এবং AI ফিক্স)
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip().lower()
    book_sheet, _ = get_sheets()
    if not book_sheet: return

    try:
        # দ্রুত রেজাল্টের জন্য সব ডেটা একবারে আনা
        all_data = book_sheet.get_all_values()[1:] 
        found_books = []

        # ধাপ ১: হুবহু বা আংশিক মিল খোঁজা (একাধিক খণ্ড থাকলে সব নিবে)
        for row in all_data:
            sheet_book_name = row[0].strip().lower()
            if user_text in sheet_book_name:
                found_books.append(row)

        if found_books:
            for book in found_books:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id, 
                    document=book[1], 
                    caption=f"আপনার বই: {book[0]}"
                )
            return

        # ধাপ ২: যদি কিছু না পাওয়া যায়, তবেই AI সক্রিয় হবে
        book_list_str = ", ".join([row[0] for row in all_data[:100]]) # প্রথম ১০০ বই প্রম্পটে পাঠানো
        prompt = (
            f"User search: '{user_text}'. Available books: {book_list_str}.\n"
            "Instructions:\n"
            "1. No asterisks (*) or bold formatting.\n"
            "2. If it's a phonetic English match (e.g. 'Subhe sadik' for 'সুবহে সাদিক'), reply ONLY with the correct Bengali name.\n"
            "3. If no match found, politely ask for the correct name in Bengali."
        )
        
        response = ai_model.generate_content(prompt)
        ai_res = response.text.strip().replace("*", "")

        # AI এর সাজেস্ট করা নাম দিয়ে আবার সার্চ
        ai_found = False
        for row in all_data:
            if ai_res.lower() in row[0].lower():
                await context.bot.send_document(
                    chat_id=update.effective_chat.id, 
                    document=row[1], 
                    caption=f"আপনার বই: {row[0]}"
                )
                ai_found = True
        
        if not ai_found:
            await update.message.reply_text(ai_res)

    except Exception as e:
        logging.error(f"Search error: {e}")

# ৫. এডমিন ফিচারস
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    book_sheet, user_sheet = get_sheets()
    if book_sheet and user_sheet:
        try:
            total_users = len(user_sheet.col_values(1)) - 1
            total_books = len(book_sheet.col_values(1)) - 1
            await update.message.reply_text(f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি")
        except: pass

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার কথাটি লিখুন।")
        return
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"মেসেজ!\nID: `{update.effective_user.id}`\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে: `/reply {update.effective_user.id} বার্তা`",
        parse_mode='Markdown'
    )
    await update.message.reply_text("মেসেজটি এডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    try:
        target_id, reply_msg = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"এডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text("পাঠানো হয়েছে।")
    except: pass

async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    name = (update.message.caption if update.message.caption else doc.file_name).replace(".pdf", "").replace("_", " ").strip()
    book_sheet, _ = get_sheets()
    if book_sheet:
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"সেভ হয়েছে: {name}")

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

if __name__ == '__main__': main()
