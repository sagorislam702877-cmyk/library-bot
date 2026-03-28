import logging
import gspread
import os
import time
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. আপনার তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 

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

# ৩. গুগল শিট কানেক্ট
def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    return client.open("MyBotDB").sheet1 

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- নতুন সাজানো স্টার্ট মেসেজ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "✨ **আসসালামু আলাইকুম!** ✨\n\n"
        "আমাদের **অনলাইন লাইব্রেরি বটে** আপনাকে স্বাগতম। 📚\n\n"
        "⚠️ **সতর্কতা:** এটি ছাত্রশিবিরের কোনো অফিসিয়াল বট নয়। শুধুমাত্র সাধারণ ছাত্র-ছাত্রীদের পড়াশোনার সহযোগিতার জন্য এটি ব্যক্তিগতভাবে তৈরি করা হয়েছে।\n\n"
        "🔍 **বই খুঁজবেন যেভাবে:**\n"
        "বইয়ের নাম (আংশিক বা পুরো) লিখে মেসেজ দিন। বট আপনাকে স্বয়ংক্রিয়ভাবে পিডিএফ (PDF) ফাইলটি পাঠিয়ে দেবে।\n\n"
        "💡 *উদাহরণ: 'চরিত্র গঠনের মৌলিক উপাদান' বা শুধু 'চরিত্র' লিখে সার্চ দিন।*"
    )
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = connect_sheet()
        
        # বই আপলোড (শুধুমাত্র আপনি পিডিএফ পাঠালে)
        if update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "")
                clean_name = raw_name.replace("_", " ").replace("-", " ").strip()
                sheet.append_row([clean_name, doc.file_id])
                await update.message.reply_text(f"✅ যুক্ত হয়েছে: {clean_name}")
                return

        # বই সার্চ (সব খণ্ড সাপোর্টসহ)
        if update.message.text:
            query = update.message.text.lower().strip()
            all_books = sheet.get_all_records()
            found_books = []

            for row in all_books:
                book_name_in_sheet = str(row['Book Name']).lower()
                if query in book_name_in_sheet:
                    found_books.append(row)

            if found_books:
                await update.message.reply_text(f"🔍 মোট {len(found_books)}টি রেজাল্ট পাওয়া গেছে। পাঠানো হচ্ছে...")
                for book in found_books:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id, 
                        document=book['File ID'],
                        caption=f"📖 বই: {book['Book Name']}"
                    )
                    time.sleep(1) 
            else:
                await update.message.reply_text("❌ দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
                
    except Exception as e:
        logging.error(f"Error: {e}")

def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
                
