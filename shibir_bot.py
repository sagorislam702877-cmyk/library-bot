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

# AI সেটআপ
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

# ২. ওয়েব সার্ভার
web_app = Flask('')
@web_app.route('/')
def home(): return "Library Bot is Online and Reply Fixed!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

# ৩. গুগল শিট কানেকশন
def get_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")
    except Exception as e:
        logging.error(f"Sheet Connection Error: {e}")
        return None, None

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. ইউজার কমান্ডস
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        _, user_sheet = get_sheets()
        if user_id not in user_sheet.col_values(1):
            user_sheet.append_row([user_id])
    except: pass
    
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

# ৫. এডমিন ফিচারস (Stats, Broadcast, Reply Fixed)
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1
        await update.message.reply_text(f"স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি")
    except: await update.message.reply_text("তথ্য পাওয়া যায়নি।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg: return
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)[1:]
    for u_id in users:
        try: await context.bot.send_message(chat_id=u_id, text=f"এডমিন নোটিশ:\n\n{msg}")
        except: continue
    await update.message.reply_text("ব্রডকাস্ট সম্পন্ন হয়েছে।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    user_id = update.effective_user.id
    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার কথাটি লিখুন।")
        return
    # এডমিনকে জানানো
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"নতুন মেসেজ!\nইউজার আইডি: `{user_id}`\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে লিখুন: `/reply {user_id} আপনার বার্তা`",
        parse_mode='Markdown'
    )
    await update.message.reply_text("মেসেজটি এডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if len(context.args) < 2:
        await update.message.reply_text("ব্যবহার: `/reply [ইউজার_আইডি] [বার্তা]`")
        return
    try:
        target_id = context.args[0]
        reply_msg = " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"এডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text(f"ইউজারকে ({target_id}) রিপ্লাই পাঠানো হয়েছে।")
    except Exception as e:
        await update.message.reply_text(f"ভুল হয়েছে: {e}")

# ৬. হাইব্রিড সার্চ (স্বাভাবিক ফিচার + AI ব্যাকআপ)
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    book_sheet, _ = get_sheets()
    if not book_sheet: return

    try:
        all_rows = book_sheet.get_all_values()
        books_data = all_rows[1:]
        
        # ধাপ ১: হুবহু এবং আংশিক মিল চেক (স্বাভাবিক ফিচার)
        found_row = None
        for row in books_data:
            if user_text.lower() == row[0].strip().lower() or user_text.lower() in row[0].lower():
                found_row = row
                break
        
        if found_row:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=found_row[1], caption=f"আপনার বই: {found_row[0]}")
            return

        # ধাপ ২: খুঁজে না পেলে AI সাহায্য করবে
        available_books = [row[0] for row in books_data]
        books_string = ", ".join(available_books)
        
        prompt = (
            f"ইউজার লিখেছে: '{user_text}'. লাইব্রেরির বই: {books_string}.\n"
            "নির্দেশনা: কোনো স্টার (*) ব্যবহার করবে না। যদি এটি তালিকার বই হয় তবে শুধু নাম দাও, নয়তো বাংলায় সাহায্য করো।"
        )
        
        response = ai_model.generate_content(prompt)
        ai_res = response.text.strip().replace("*", "")

        ai_found = False
        for row in books_data:
            if ai_res.lower() == row[0].lower() or row[0].lower() in ai_res.lower():
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"আপনার বই: {row[0]}")
                ai_found = True
                break
        
        if not ai_found:
            await update.message.reply_text(ai_res)
                
    except Exception as e:
        logging.error(f"Search Error: {e}")

# ৭. বই আপলোড
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
    
    # কমান্ড হ্যান্ডলার
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user)) # রিপ্লাই অপশনটি এখানে যুক্ত করা হয়েছে
    
    # মেসেজ হ্যান্ডলার
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
