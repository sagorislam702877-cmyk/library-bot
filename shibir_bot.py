import logging
import gspread
import os
import time
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. আপনার তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 
ADMIN_USERNAME = "SagorIslam29" # আপনার আপডেট করা ইউজারনেম

# ডাইনামিক কাউন্টার
active_searches = 0

# ২. ওয়েব সার্ভার (Render সচল রাখতে)
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

# ৩. গুগল শিট কানেক্ট করার ফাংশন
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("MyBotDB")
    return spreadsheet.sheet1, spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. অটো ইউজার সেভ করার ফাংশন
def save_user(user_id):
    try:
        _, user_sheet = get_sheets()
        existing_users = user_sheet.col_values(1)
        if str(user_id) not in existing_users:
            user_sheet.append_row([str(user_id)])
    except Exception as e:
        logging.error(f"User Save Error: {e}")

# ৫. স্টার্ট কমান্ড (যোগাযোগের বাটনসহ)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id)
    
    keyboard = [[InlineKeyboardButton("👨‍💻 অ্যাডমিনের সাথে যোগাযোগ", url=f"https://t.me/{ADMIN_USERNAME}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = (
        "✨ **আসসালামু আলাইকুম!** ✨\n\n"
        "আমাদের **অনলাইন লাইব্রেরি বটে** আপনাকে স্বাগত। 📚\n\n"
        "🔍 **বই খুঁজবেন যেভাবে:**\n"
        "বইয়ের নাম লিখে মেসেজ দিন। বট আপনাকে স্বয়ংক্রিয়ভাবে পিডিএফ পাঠিয়ে দেবে।"
    )
    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')

# ৬. অ্যাডমিন কন্টাক্ট কমান্ড (ইউজার আইডি ছাড়া)
async def admin_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👨‍💻 **অ্যাডমিন ইনফো:**\n\n"
        "বট ব্যবহারে কোনো সমস্যা হলে বা নতুন বই যুক্ত করতে চাইলে সরাসরি নিচে ক্লিক করে মেসেজ দিন:\n"
        f"👉 @{ADMIN_USERNAME}"
    )
    keyboard = [[InlineKeyboardButton("📩 সরাসরি মেসেজ দিন", url=f"https://t.me/{ADMIN_USERNAME}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# ৭. হেল্প কমান্ড (বট ব্যবহারের নিয়ম)
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 **বট ব্যবহারের নিয়মাবলী:**\n\n"
        "১. সরাসরি বইয়ের নাম (যেমন: 'সংবিধান') লিখে মেসেজ দিন।\n"
        "২. বানান সঠিক হলে বট আপনাকে অটোমেটিক পিডিএফ ফাইলটি পাঠিয়ে দেবে।\n"
        "৩. বড় নামের ক্ষেত্রে নামের একটি নির্দিষ্ট অংশ লিখে সার্চ দিন।\n\n"
        "👨‍💻 কোনো সমস্যা হলে `/admin` কমান্ড ব্যবহার করুন।"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')

# ৮. মেসেজ হ্যান্ডলার (সার্চ এবং বই আপলোড)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_searches
    try:
        # ইউজার সার্চ করলে
        if update.message.text:
            query = update.message.text.lower().strip()
            
            # ডাইনামিক ওয়েটিং লজিক (গুগল লিমিট বাঁচাতে)
            wait_time = 0
            if active_searches > 10: wait_time = 5
            elif active_searches > 5: wait_time = 2
                
            if wait_time > 0:
                await update.message.reply_text(f"⏳ বর্তমানে **{active_searches} জন** সার্চ করছেন। চাপ কমাতে আপনাকে {wait_time} সেকেন্ড অপেক্ষা করতে হচ্ছে...")
                await asyncio.sleep(wait_time)
            
            active_searches += 1
            book_sheet, _ = get_sheets()
            all_books = book_sheet.get_all_records()
            found_books = [row for row in all_books if query in str(row['Book Name']).lower()]

            if found_books:
                await update.message.reply_text(f"🔍 {len(found_books)}টি বই পাওয়া গেছে। পাঠানো হচ্ছে...")
                for book in found_books:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=book['File ID'], caption=f"📖 {book['Book Name']}")
                    await asyncio.sleep(1.2) # রেট লিমিট এড়াতে বিরতি
            else:
                await update.message.reply_text("❌ দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
            
            active_searches -= 1

        # অ্যাডমিন নতুন বই আপলোড করলে
        elif update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                book_sheet, _ = get_sheets()
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "").replace("_", " ").strip()
                book_sheet.append_row([raw_name, doc.file_id])
                await update.message.reply_text(f"✅ লাইব্রেরিতে যুক্ত হয়েছে: {raw_name}")

    except Exception as e:
        if active_searches > 0: active_searches -= 1
        logging.error(f"Error: {e}")

# ৯. পরিসংখ্যান দেখা (শুধুমাত্র অ্যাডমিন)
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        try:
            book_sheet, user_sheet = get_sheets()
            u_count = len(user_sheet.col_values(1)) - 1
            b_count = len(book_sheet.col_values(1)) - 1
            await update.message.reply_text(f"📊 **পরিসংখ্যান:**\n👤 মোট ইউজার: {u_count} জন\n📚 মোট বই: {b_count} টি\n🔥 এক্টিভ সার্চ: {active_searches}")
        except:
            await update.message.reply_text("ডেটা লোড করতে সমস্যা হচ্ছে।")

# ১০. মেইন ফাংশন
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    # কমান্ড হ্যান্ডলারসমূহ
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("admin", admin_info))
    app.add_handler(CommandHandler("help", help_command))
    
    # মেসেজ এবং ডকুমেন্ট হ্যান্ডলার
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    
    app.run_polling()

if __name__ == '__main__':
    main()
    
