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
ADMIN_USERNAME = "SagorIslam29"

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

# ৪. অটো ইউザー সেভ
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
    keyboard = [[InlineKeyboardButton("👨‍💻 অ্যাডমিনের সাথে যোগাযোগ", url=f"https://t.me/{ADMIN_USERNAME}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_text = "✨ **আসসালামু আলাইকুম!** ✨\n\nআমাদের **অনলাইন লাইব্রেরি বটে** আপনাকে স্বাগত। 📚\n\n🔍 **বই খুঁজবেন যেভাবে:**\nবইয়ের নাম লিখে মেসেজ দিন।"
    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')

# ৬. ব্রডকাস্ট ফিচার (সব ইউজারকে মেসেজ পাঠানো)
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ এই কমান্ডটি শুধু অ্যাডমিনের জন্য।")
        return

    message_to_send = " ".join(context.args)
    if not message_to_send:
        await update.message.reply_text("⚠️ ব্যবহার: `/broadcast আপনার মেসেজটি লিখুন`")
        return

    _, user_sheet = get_sheets()
    user_ids = user_sheet.col_values(1)[1:] # প্রথম সারির টাইটেল বাদ দিয়ে সব আইডি
    
    success_count = 0
    await update.message.reply_text(f"🚀 ব্রডকাস্ট শুরু হয়েছে... {len(user_ids)} জন ইউজারকে পাঠানো হচ্ছে।")

    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=f"📢 **নোটিশ:**\n\n{message_to_send}", parse_mode='Markdown')
            success_count += 1
            await asyncio.sleep(0.1) # টেলিগ্রাম স্প্যাম ফিল্টার এড়াতে বিরতি
        except Exception:
            pass # ইউজার বট ব্লক করলে এরর এড়িয়ে যাবে

    await update.message.reply_text(f"✅ ব্রডকাস্ট সম্পন্ন! {success_count} জন ইউজার মেসেজ পেয়েছেন।")

# ৭. অ্যাডমিন ও হেল্প কমান্ড
async def admin_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = f"👨‍💻 **অ্যাডমিন ইনফো:**\n\nবট ব্যবহারে সমস্যা হলে মেসেজ দিন:\n👉 @{ADMIN_USERNAME}"
    keyboard = [[InlineKeyboardButton("📩 সরাসরি মেসেজ দিন", url=f"https://t.me/{ADMIN_USERNAME}")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = "📖 **বট ব্যবহারের নিয়ম:**\n১. বইয়ের নাম লিখে মেসেজ দিন।\n২. বানান সঠিক হলে বট পিডিএফ দেবে।\n৩. সমস্যার জন্য `/admin` ব্যবহার করুন।"
    await update.message.reply_text(help_text, parse_mode='Markdown')

# ৮. মেসেজ হ্যান্ডলার
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_searches
    try:
        if update.message.text:
            query = update.message.text.lower().strip()
            active_searches += 1
            book_sheet, _ = get_sheets()
            all_books = book_sheet.get_all_records()
            found_books = [row for row in all_books if query in str(row['Book Name']).lower()]

            if found_books:
                await update.message.reply_text(f"🔍 {len(found_books)}টি বই পাওয়া গেছে।")
                for book in found_books:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=book['File ID'], caption=f"📖 {book['Book Name']}")
                    await asyncio.sleep(1)
            else:
                await update.message.reply_text("❌ দুঃখিত, কোনো বই পাওয়া যায়নি।")
            active_searches -= 1

        elif update.message.document and update.effective_user.id == ADMIN_ID:
            book_sheet, _ = get_sheets()
            raw_name = update.message.document.file_name.replace(".pdf", "").replace("_", " ").strip()
            book_sheet.append_row([raw_name, update.message.document.file_id])
            await update.message.reply_text(f"✅ যুক্ত হয়েছে: {raw_name}")
    except Exception as e:
        if active_searches > 0: active_searches -= 1
        logging.error(f"Error: {e}")

# ৯. পরিসংখ্যান
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        book_sheet, user_sheet = get_sheets()
        u_count = len(user_sheet.col_values(1)) - 1
        b_count = len(book_sheet.col_values(1)) - 1
        await update.message.reply_text(f"📊 **পরিসংখ্যান:**\n👤 ইউজার: {u_count}\n📚 বই: {b_count}\n🔥 লাইভ সার্চ: {active_searches}")

def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("admin", admin_info))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
    
