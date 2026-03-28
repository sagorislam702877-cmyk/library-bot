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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("আসসালামু আলাইকুম! বইয়ের নাম বা খণ্ড লিখে সার্চ দিন।\nশুধু বইয়ের নাম লিখলে সব খণ্ড পাবেন।")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = connect_sheet()
        
        # বই আপলোড (শুধুমাত্র অ্যাডমিন)
        if update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "")
                clean_name = raw_name.replace("_", " ").replace("-", " ").strip()
                sheet.append_row([clean_name, doc.file_id])
                await update.message.reply_text(f"✅ যুক্ত হয়েছে: {clean_name}")
                return

        # বই সার্চ লজিক (মাল্টিপল রেজাল্ট সাপোর্ট)
        if update.message.text:
            query = update.message.text.lower().strip()
            all_books = sheet.get_all_records()
            found_books = []

            for row in all_books:
                book_name_in_sheet = str(row['Book Name']).lower()
                # ইউজারের সার্চ করা শব্দ যদি শিটের নামের ভেতর থাকে
                if query in book_name_in_sheet:
                    found_books.append(row)

            if found_books:
                await update.message.reply_text(f"🔍 মোট {len(found_books)}টি বই/খণ্ড পাওয়া গেছে। পাঠানো হচ্ছে...")
                for book in found_books:
                    # ফাইল আইডি দিয়ে এক এক করে পাঠানো
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id, 
                        document=book['File ID'],
                        caption=f"বইয়ের নাম: {book['Book Name']}"
                    )
                    time.sleep(1) # টেলিগ্রামের লিমিট এড়াতে বিরতি
            else:
                await update.message.reply_text("দুঃখিত, এই নামে কোনো বই বা খণ্ড পাওয়া যায়নি।")
                
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
                         
