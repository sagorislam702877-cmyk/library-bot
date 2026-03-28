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
TOKEN = '8762483955:AAFai0evS1PBKMK1X6dVVa-dCzIc3oZEMCo' # আপনার নিউ টোকেন
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 

# --- ওয়েব সার্ভার (Render-কে সচল রাখতে) ---
web_app = Flask('')
@web_app.route('/')
def home(): return "Library Bot is Online and Healthy!"

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
    return spreadsheet.worksheet("sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ১. স্টার্ট কমান্ড ও ইউজার সেভ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if str(user_id) not in all_users:
            user_sheet.append_row([str(user_id)])
    except Exception as e:
        logging.error(f"Error saving user: {e}")

    welcome_text = (
        "📚 **আসসালামু আলাইকুম!**\n"
        "অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে সার্চ দিন।"
    )
    await update.message.reply_text(welcome_text)

# --- ২. স্মার্ট সার্চ লজিক ---
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    if not user_query: return

    try:
        book_sheet, _ = get_sheets()
        all_books = book_sheet.get_all_records()
        
        # Exact Match Check
        exact_match = next((b for b in all_books if str(b['Book Name']).lower() == user_query.lower()), None)
        
        if exact_match:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=exact_match['File ID'], caption=f"📖 {exact_match['Book Name']}")
            return

        # Fuzzy Suggestion
        book_names = [b['Book Name'] for b in all_books]
        matches = process.extract(user_query, book_names, limit=5)
        
        buttons = []
        for match_name, score in matches:
            if score > 55: 
                buttons.append([InlineKeyboardButton(f"📖 {match_name}", callback_data=f"get_{match_name}")])
        
        if buttons:
            await update.message.reply_text("🔍 হুবহু মিল পাওয়া যায়নি। আপনি কি এগুলো খুঁজছেন?", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await update.message.reply_text("❌ দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
    except Exception as e:
        logging.error(f"Search error: {e}")

# --- ৩. বাটন হ্যান্ডলার ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('get_'):
        target_book = query.data.replace("get_", "")
        book_sheet, _ = get_sheets()
        all_books = book_sheet.get_all_records()
        book_info = next((b for b in all_books if b['Book Name'] == target_book), None)
        
        if book_info:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=book_info['File ID'], caption=f"📖 {book_info['Book Name']}")

# --- ৪. মেইন রানার (স্বয়ংক্রিয় সমাধানসহ) ---
def main():
    keep_alive()
    # অ্যাপ বিল্ড করা
    app = Application.builder().token(TOKEN).build()
    
    # হ্যান্ডলার যুক্ত করা
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    print("Bot is starting with Auto-Conflict Fix...")
    
    # কনফ্লিক্ট এড়াতে drop_pending_updates=True ব্যবহার করা হয়েছে
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
    
