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
def home(): return "Shibir Online Library is Active!"

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

# --- ১. স্টার্ট ও হেল্প কমান্ড (অফিসিয়াল ভাষা) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if str(user_id) not in all_users:
            user_sheet.append_row([str(user_id)])
    except: pass
    
    welcome_text = (
        "আসসালামু আলাইকুম।\n"
        "অনলাইন লাইব্রেরি সেবায় আপনাকে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে আমাদের মেসেজ প্রদান করুন।\n\n"
        "বিস্তারিত নির্দেশনার জন্য /help কমান্ডটি ব্যবহার করুন।"
    )
    await update.message.reply_text(welcome_text)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "নির্দেশনাবলী:\n\n"
        "১. বই অনুসন্ধান: সরাসরি বইয়ের সঠিক নাম লিখে মেসেজ দিন। যেমন: সিলেবাস বা সংবিধান।\n\n"
        "২. বানান সংশোধন: আপনার প্রদানকৃত বানানে ভুল থাকলেও বট আপনাকে সম্ভাব্য বইয়ের তালিকা প্রদান করবে।\n\n"
        "৩. কর্তৃপক্ষের সাথে যোগাযোগ: কোনো বইয়ের জন্য আবেদন বা অভিযোগ জানাতে মেসেজের শুরুতে /admin লিখে আপনার বার্তাটি প্রদান করুন।\n\n"
        "উদাহরণ: /admin আমার একটি বিশেষ বই প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# --- ২. অ্যাডমিন ড্যাশবোর্ড ও ব্রডকাস্ট ---
async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    book_sheet, user_sheet = get_sheets()
    total_users = len(user_sheet.col_values(1)) - 1
    total_books = len(book_sheet.col_values(1)) - 1
    
    admin_text = (
        "অ্যাডমিন ড্যাশবোর্ড\n\n"
        f"মোট ব্যবহারকারী: {total_users} জন\n"
        f"সংগৃহীত বই: {total_books} টি\n\n"
        "সকলকে বার্তা পাঠাতে: /broadcast আপনার বার্তা"
    )
    await update.message.reply_text(admin_text)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or not context.args: return
    msg = " ".join(context.args)
    _, user_sheet = get_sheets()
    user_ids = user_sheet.col_values(1)[1:]
    for uid in user_ids:
        try: await context.bot.send_message(chat_id=uid, text=f"জরুরি নোটিশ:\n\n{msg}")
        except: continue
    await update.message.reply_text("সফলভাবে সকলের কাছে বার্তা পৌঁছেছে।")

# --- ৩. যোগাযোগ ও উত্তর প্রদান (অফিসিয়াল লজিক) ---
async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("ভুল ফরম্যাট। উদাহরণ: /admin আপনার বার্তা")
        return
    admin_notif = f"নতুন বার্তা প্রাপ্তি:\nUser ID: {user_id}\nনাম: {update.effective_user.first_name}\n\nবার্তা: {user_msg}"
    await context.bot.send_message(chat_id=ADMIN_ID, text=admin_notif)
    await update.message.reply_text("আপনার বার্তাটি সফলভাবে কর্তৃপক্ষের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    target_id = context.args[0]
    reply_msg = " ".join(context.args[1:])
    try:
        await context.bot.send_message(chat_id=target_id, text=f"কর্তৃপক্ষের উত্তর:\n\n{reply_msg}")
        await update.message.reply_text("ব্যবহারকারীর কাছে উত্তর পাঠানো হয়েছে।")
    except: await update.message.reply_text("বার্তাটি পাঠানো সম্ভব হয়নি। আইডি যাচাই করুন।")

# --- ৪. স্মার্ট অনুসন্ধান ও ফাইল প্রেরণ ---
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    book_sheet, _ = get_sheets()
    all_books = book_sheet.get_all_records()
    
    # হুবহু মিল অনুসন্ধান
    exact_match = next((b for b in all_books if str(b['Book Name']).lower() == user_query.lower()), None)
    if exact_match:
        await context.bot.send_document(chat_id=update.effective_chat.id, document=exact_match['File ID'], caption=f"বই: {exact_match['Book Name']}")
        return

    # সম্ভাব্য সাজেশন প্রদান
    book_names = [b['Book Name'] for b in all_books]
    matches = process.extract(user_query, book_names, limit=5)
    buttons = [[InlineKeyboardButton(f"📖 {m[0]}", callback_data=f"get_{m[0][:40]}")] for m in matches if m[1] > 50]
    
    if buttons: 
        await update.message.reply_text("আপনার প্রদানকৃত তথ্যের ভিত্তিতে নিচের বইগুলো পাওয়া গেছে:", reply_markup=InlineKeyboardMarkup(buttons))
    else: 
        await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি। সঠিক নাম লিখে পুনরায় চেষ্টা করুন।")

# --- ৫. ফাইল আপলোড ও বাটন হ্যান্ডলার ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    if doc.mime_type == 'application/pdf':
        file_info = {'name': doc.file_name.replace(".pdf", "").replace("_", " ").strip(), 'id': doc.file_id}
        if 'pending_files' not in context.user_data: context.user_data['pending_files'] = []
        context.user_data['pending_files'].append(file_info)
        
        if len(context.user_data['pending_files']) == 1:
            keyboard = [[InlineKeyboardButton("সকল বই সেভ করুন", callback_data='save_all_bulk')]]
            await update.message.reply_text("ফাইল গ্রহণ করা হচ্ছে। শেষ হলে নিচের বাটনে ক্লিক করুন।", reply_markup=InlineKeyboardMarkup(keyboard))

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('get_'):
        target = query.data.replace("get_", "")
        book_sheet, _ = get_sheets()
        book_info = next((b for b in book_sheet.get_all_records() if str(b['Book Name']).startswith(target)), None)
        if book_info: await context.bot.send_document(chat_id=update.effective_chat.id, document=book_info['File ID'], caption=f"বই: {book_info['Book Name']}")

    elif query.data == 'save_all_bulk':
        files = context.user_data.get('pending_files', [])
        if files:
            book_sheet, _ = get_sheets()
            for f in files:
                book_sheet.append_row([f['name'], f['id']])
                await asyncio.sleep(0.5)
            await query.edit_message_text(f"সফলভাবে {len(files)}টি বই ডাটাবেজে যুক্ত করা হয়েছে।")
            context.user_data['pending_files'] = []

# --- মেইন রানার ---
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("dashboard", admin_dashboard))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Document.PDF, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
