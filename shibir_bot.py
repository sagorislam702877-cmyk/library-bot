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
    await update.message.reply_text("আসসালামু আলাইকুম! বইয়ের নাম লিখে সার্চ দিন।")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = connect_sheet()
        
        # বই আপলোড এবং ফরওয়ার্ড হ্যান্ডলিং
        if update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                # শুধুমাত্র বইয়ের নাম পরিষ্কার করা হচ্ছে
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "")
                clean_name = raw_name.replace("_", " ").replace("-", " ").strip()
                
                # ফাইল আইডি বা লিঙ্কটি যেমন আছে তেমনই রাখা হচ্ছে (কোনো পরিবর্তন ছাড়া)
                file_id = doc.file_id
                
                sheet.append_row([clean_name, file_id])
                await update.message.reply_text(f"✅ যুক্ত হয়েছে: {clean_name}")
                time.sleep(1) 
                return

        # বই সার্চ
        if update.message.text:
            query = update.message.text.lower().strip()
            all_books = sheet.get_all_records()
            found = False
            for row in all_books:
                book_in_sheet = str(row['Book Name']).lower()
                if query in book_in_sheet:
                    # ফাইল আইডিটি শিট থেকে হুবহু নেওয়া হচ্ছে
                    actual_file_id = row['File ID']
                    await update.message.reply_text(f"বই পাওয়া গেছে: {row['Book Name']}\nপাঠানো হচ্ছে...")
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=actual_file_id)
                    found = True
                    break
            
            if not found:
                await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
                
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
                    
