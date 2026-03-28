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
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" # আপনার শিট ফাইলের নাম

# --- ওয়েব সার্ভার (Render-কে সচল রাখতে) ---
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
    # আপনার দেওয়া নাম অনুযায়ী ট্যাবগুলো সিলেক্ট করা হয়েছে
    return spreadsheet.worksheet("sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ১. স্টার্ট কমান্ড ও ইউজার আইডি সেভ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    _, user_sheet = get_sheets()
    
    # ইউজার আগে থেকে সেভ করা না থাকলে সেভ করা হবে
    all_users = user_sheet.col_values(1)
    if str(user_id) not in all_users:
        user_sheet.append_row([str(user_id)])

    welcome_text = (
        "📚 **আসসালামু আলাইকুম!**\n"
        "অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে সার্চ দিন।"
    )
    await update.message.reply_text(welcome_text)

# --- ২. বাল্ক আপলোড (অ্যাডমিনের জন্য) ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    if doc.mime_type == 'application/pdf':
        file_info = {
            'name': doc.file_name.replace(".pdf", "").replace("_", " ").strip(),
            'id': doc.file_id
        }
        if 'pending_files' not in context.user_data:
            context.user_data['pending_files'] = []
        context.user_data['pending_files'].append(file_info)
        
        if len(context.user_data['pending_files']) == 1:
            keyboard = [[InlineKeyboardButton("✅ সব বই সেভ করুন", callback_data='save_all_bulk')]]
            await update.message.reply_text(
                "📥 ফাইল পাওয়া যাচ্ছে... ফরওয়ার্ড করা শেষ হলে নিচের বাটনে ক্লিক করুন:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

# --- ৩. স্মার্ট সার্চ (Direct File or Suggestion) ---
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    if not user_query: return

    book_sheet, _ = get_sheets()
    all_books = book_sheet.get_all_records()
    
    # ১. প্রথমে Exact Match চেক করা
    exact_match = next((b for b in all_books if str(b['Book Name']).lower() == user_query.lower()), None)
    
    if exact_match:
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=exact_match['File ID'],
            caption=f"📖 {exact_match['Book Name']}"
        )
        return

    # ২. হুবহু মিল না পাওয়া গেলে Fuzzy Search
    book_names = [b['Book Name'] for b in all_books]
    matches = process.extract(user_query, book_names, limit=5)
    
    buttons = []
    for match_name, score in matches:
        if score > 55: 
            buttons.append([InlineKeyboardButton(f"📖 {match_name}", callback_data=f"get_{match_name}")])
    
    if buttons:
        await update.message.reply_text(
            "🔍 হুবহু মিল পাওয়া যায়নি। আপনি কি নিচের বইগুলো খুঁজছেন?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    else:
        await update.message.reply_text("❌ দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")

# --- ৪. বাটন হ্যান্ডলার ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('get_'):
        target_book = query.data.replace("get_", "")
        book_sheet, _ = get_sheets()
        all_books = book_sheet.get_all_records()
        book_info = next((b for b in all_books if b['Book Name'] == target_book), None)
        
        if book_info:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=book_info['File ID'],
                caption=f"📖 {book_info['Book Name']}"
            )
            
    elif query.data == 'save_all_bulk':
        files = context.user_data.get('pending_files', [])
        if files:
            book_sheet, _ = get_sheets()
            for f in files:
                book_sheet.append_row([f['name'], f['id']])
                await asyncio.sleep(0.6)
            await query.edit_message_text(f"✅ সফলভাবে {len(files)}টি বই সেভ হয়েছে!")
            context.user_data['pending_files'] = []

# --- ৫. মেইন রানার ---
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Document.PDF, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    print("Bot is starting...")
    app.run_polling()

if __name__ == '__main__':
    main()
    
