import logging
import gspread
import os
import time
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. আপনার তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 

# ডাইনামিক কাউন্টার
active_searches = 0

# ২. ওয়েব সার্ভার
web_app = Flask('')
@web_app.route('/')
def home():
    return "Library Bot is Active!"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web)
    t.start()

# ৩. গুগল শিট কানেক্ট
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("MyBotDB")
    return spreadsheet.sheet1, spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. অটো ইউজার সেভ
def save_user(user_id):
    try:
        _, user_sheet = get_sheets()
        existing_users = user_sheet.col_values(1)
        if str(user_id) not in existing_users:
            user_sheet.append_row([str(user_id)])
    except Exception as e:
        logging.error(f"User Save Error: {e}")

# ৫. স্টার্ট কমান্ড
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id)
    welcome_text = "✨ **আসসালামু আলাইকুম!** ✨\n\nবইয়ের নাম লিখে মেসেজ দিন। আমাদের বট অটোমেটিক আপনার ফাইলটি খুঁজে দেবে। 📚"
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

# ৬. ডাইনামিক হ্যান্ডলার (বট নিজেই লিমিট ঠিক করবে)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_searches
    
    try:
        if update.message.text:
            query = update.message.text.lower().strip()
            
            # --- ডাইনামিক লিমিট লজিক ---
            wait_time = 0
            if active_searches > 10:
                wait_time = 5  # ১০ জনের বেশি হলে ৫ সেকেন্ড অপেক্ষা
            elif active_searches > 5:
                wait_time = 2  # ৫ জনের বেশি হলে ২ সেকেন্ড অপেক্ষা
                
            if wait_time > 0:
                await update.message.reply_text(f"⏳ বর্তমানে **{active_searches} জন** ইউজার সার্চ করছেন। সার্ভারের ওপর চাপ কমাতে আপনাকে {wait_time} সেকেন্ড অপেক্ষা করতে হচ্ছে...")
                await asyncio.sleep(wait_time)
            
            active_searches += 1 # সার্চ শুরু
            
            book_sheet, _ = get_sheets()
            all_books = book_sheet.get_all_records()
            found_books = [row for row in all_books if query in str(row['Book Name']).lower()]

            if found_books:
                await update.message.reply_text(f"🔍 {len(found_books)}টি বই পাওয়া গেছে। পাঠানো হচ্ছে...")
                for book in found_books:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=book['File ID'], caption=f"📖 {book['Book Name']}")
                    await asyncio.sleep(1.2)
            else:
                await update.message.reply_text("❌ দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
            
            active_searches -= 1 # সার্চ শেষ

        elif update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                book_sheet, _ = get_sheets()
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "").replace("_", " ").strip()
                book_sheet.append_row([raw_name, doc.file_id])
                await update.message.reply_text(f"✅ যুক্ত হয়েছে: {raw_name}")

    except Exception as e:
        if active_searches > 0: active_searches -= 1
        logging.error(f"Error: {e}")

# ৭. স্ট্যাটাস চেক
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        try:
            book_sheet, user_sheet = get_sheets()
            u_count = len(user_sheet.col_values(1)) - 1
            b_count = len(book_sheet.col_values(1)) - 1
            await update.message.reply_text(f"📊 **পরিসংখ্যান:**\n👤 ইউজার: {u_count}\n📚 বই: {b_count}\n🔥 একটিভ সার্চ: {active_searches}")
        except:
            await update.message.reply_text("ডেটা পাওয়া যাচ্ছে না।")

def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
    
