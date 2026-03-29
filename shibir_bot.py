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
def home(): return "Library Bot is Online and Search Fixed!"

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

# ৩. কমান্ডস
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখুন।")

# ৪. উন্নত সার্চ ইঞ্জিন (English to Bengali & Spelling Fix)
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    book_sheet, _ = get_sheets()
    if not book_sheet: return

    try:
        all_data = book_sheet.get_all_values()[1:] # সব বইয়ের ডাটা
        book_names = [row[0] for row in all_data]
        
        # AI-কে দিয়ে সঠিক নাম উদ্ধার করা (ইংরেজি বা ভুল বানান ঠিক করতে)
        prompt = (
            f"User search: '{user_text}'. Available books: {', '.join(book_names[:150])}.\n"
            "নির্দেশনা:\n"
            "১. ইউজার যদি ইংরেজি (যেমন: Bukhari) বা ভুল বানানে লিখে, তবে উপরের তালিকা থেকে সঠিক বাংলা নামটি দাও।\n"
            "২. যদি একাধিক খণ্ড থাকে, তবে শুধু মূল নামটি দাও।\n"
            "৩. কোনো স্টার (*) বা বাড়তি কথা বলবে না। শুধু বইয়ের নাম দাও।"
        )
        
        ai_response = ai_model.generate_content(prompt)
        ai_res_name = ai_response.text.strip().replace("*", "")

        found_books = []
        # AI এর দেওয়া নাম অথবা ইউজারের নামের সাথে মিল খোঁজা
        search_key = ai_res_name.lower() if ai_res_name else user_text.lower()

        for row in all_data:
            sheet_name = row[0].lower()
            if search_key in sheet_name or user_text.lower() in sheet_name:
                found_books.append(row)

        if found_books:
            for book in found_books:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id, 
                    document=book[1], 
                    caption=f"আপনার বই: {book[0]}"
                )
        else:
            await update.message.reply_text(f"দুঃখিত, '{user_text}' নামে কোনো বই খুঁজে পাওয়া যায়নি। দয়া করে সঠিক বানান লিখুন।")

    except Exception as e:
        logging.error(f"Search Error: {e}")
        await update.message.reply_text("সার্ভারে সমস্যা হচ্ছে, কিছুক্ষণ পর চেষ্টা করুন।")

# ৫. এডমিন ফিচারস
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
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
        
