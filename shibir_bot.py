import logging
import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. আপনার তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 

# ২. Render এর জন্য ওয়েব সার্ভার (যাতে Timeout না হয়)
web_app = Flask('')

@web_app.route('/')
def home():
    return "I am alive!"

def run_web():
    # Render ডিফল্টভাবে ১০০০০ পোর্টে রান করে
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
    await update.message.reply_text("আসসালামু আলাইকুম! @ShibirOnlineLibraryBot এ স্বাগতম।\nবইয়ের নাম লিখে সার্চ দিন।")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sheet = connect_sheet()
    
    # বই আপলোড (শুধুমাত্র অ্যাডমিন)
    if update.message.document and update.effective_user.id == ADMIN_ID:
        doc = update.message.document
        if doc.mime_type == 'application/pdf':
            book_name = doc.file_name.lower().replace(".pdf", "").strip()
            sheet.append_row([book_name, doc.file_id])
            await update.message.reply_text(f"✅ লাইব্রেরিতে যুক্ত হয়েছে: {doc.file_name}")
            return

    # বই সার্চ
    if update.message.text:
        query = update.message.text.lower().strip()
        all_books = sheet.get_all_records()
        for row in all_books:
            if query in str(row['Book Name']):
                await update.message.reply_text(f"বই পাওয়া গেছে: {row['Book Name']}\nপাঠানো হচ্ছে...")
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row['File ID'])
                return
        await update.message.reply_text("দুঃখিত, বইটি পাওয়া যায়নি। সঠিক নাম লিখুন।")

def main():
    # ওয়েব সার্ভার শুরু (Render কে সচল রাখতে)
    keep_alive()
    
    # বট শুরু
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    
    print("বট এবং ওয়েব সার্ভার সচল আছে...")
    app.run_polling()

if __name__ == '__main__':
    main()
    
