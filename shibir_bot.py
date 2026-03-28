import logging
import gspread
import os
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from flask import Flask
from threading import Thread
from fuzzywuzzy import process 

# --- কনফিগারেশন ---
TOKEN = '8762483955:AAFai0evS1PBKMK1X6dVVa-dCzIc3oZEMCo' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 

# --- ওয়েব সার্ভার ---
web_app = Flask('')
@web_app.route('/')
def home(): return "Library Bot is Online!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()

# --- গুগল শিট কানেক্ট ---
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ১. স্টার্ট ও হেল্প কমান্ড ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if str(user_id) not in all_users:
            user_sheet.append_row([str(user_id)])
    except: pass
    
    await update.message.reply_text(
        "আসসালামু আলাইকুম।\nঅনলাইন লাইব্রেরিতে আপনাকে স্বাগতম। আপনি যেকোনো বইয়ের নাম লিখে মেসেজ দিলে বট সেটি খুঁজে দেবে।\n\n"
        "বট ব্যবহারের নিয়ম জানতে /help কমান্ডটি ব্যবহার করুন।"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "বট ব্যবহারের সহজ নিয়মাবলী:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন। যেমন: সিলেবাস বা সংবিধান।\n"
        "২. সাজেশন: বানান সামান্য ভুল হলেও বট আপনাকে কাছাকাছি নামের তালিকা দেখাবে।\n"
        "৩. যোগাযোগ: কোনো প্রয়োজনে মেসেজের শুরুতে /admin লিখে আপনার কথাটি লিখুন।\n\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )

# --- ২. অ্যাডমিন ও যোগাযোগ লজিক ---
async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("মেসেজ পাঠাতে /admin এর পর আপনার কথাটি লিখুন।")
        return
    await context.bot.send_message(chat_id=ADMIN_ID, text=f"ইউজার মেসেজ (ID: {user_id}):\n{user_msg}")
    await update.message.reply_text("আপনার মেসেজটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    target_id, msg = context.args[0], " ".join(context.args[1:])
    try:
        await context.bot.send_message(chat_id=target_id, text=f"অ্যাডমিনের উত্তর:\n\n{msg}")
        await update.message.reply_text("উত্তর পাঠানো হয়েছে।")
    except: await update.message.reply_text("বার্তাটি পাঠানো যায়নি।")

# --- ৩. স্মার্ট সার্চ লজিক (সরাসরি পজিশন রিড) ---
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    book_sheet, _ = get_sheets()
    
    # সকল ডাটা লিস্ট আকারে নিয়ে আসা (কলামের নামের ওপর নির্ভর করবে না)
    all_data = book_sheet.get_all_values()
    if len(all_data) < 2: return
    
    books = all_data[1:] # হেডার বাদ দিয়ে
    
    # ১. হুবহু মিল চেক
    for row in books:
        if row[0].strip().lower() == user_query.lower():
            await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"বই: {row[0]}")
            return

    # ২. আংশিক মিল চেক (Keyword Match)
    matches = []
    for row in books:
        if user_query.lower() in row[0].lower():
            matches.append(row[0])
    
    # ৩. ফাজি সাজেশন (বানান ভুল হলে)
    if not matches:
        all_names = [row[0] for row in books]
        fuzzy_results = process.extract(user_query, all_names, limit=5)
        matches = [m[0] for m in fuzzy_results if m[1] > 45]

    if matches:
        buttons = [[InlineKeyboardButton(f"📖 {name}", callback_data=f"get_{name[:40]}")] for name in matches[:8]]
        await update.message.reply_text("আপনি কি নিচের কোনো বইটি খুঁজছেন?", reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি। সঠিক নাম লিখে চেষ্টা করুন।")

# --- ৪. বাটন ও ফাইল হ্যান্ডলার ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('get_'):
        target = query.data.replace("get_", "")
        book_sheet, _ = get_sheets()
        all_rows = book_sheet.get_all_values()[1:]
        # নামের শুরুতে মিল থাকলে বই পাঠিয়ে দেবে
        book = next((row for row in all_rows if row[0].startswith(target)), None)
        if book:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=book[1], caption=f"বই: {book[0]}")

# --- ৫. মেইন রানার ---
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    print("Bot is Starting...")
    # Conflict এরর সমাধানে drop_pending_updates=True ব্যবহার করা হয়েছে
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
