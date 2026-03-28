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
    await update.message.reply_text("আসসালামু আলাইকুম! বইয়ের নাম (আংশিক বা পুরো) লিখে সার্চ দিন।")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        sheet = connect_sheet()
        
        # বই আপলোড এবং ফরওয়ার্ড হ্যান্ডলিং
        if update.message.document and update.effective_user.id == ADMIN_ID:
            doc = update.message.document
            if doc.mime_type == 'application/pdf':
                # ফাইলের নাম থেকে আন্ডারস্কোর (_) সরিয়ে স্পেস করা হচ্ছে
                raw_name = doc.file_name.replace(".pdf", "").replace(".PDF", "")
                clean_name = raw_name.replace("_", " ").replace("-", " ").strip().lower()
                
                sheet.append_row([clean_name, doc.file_id])
                await update.message.reply_text(f"✅ যুক্ত হয়েছে: {clean_name}")
                # অনেকগুলো ফাইল একসঙ্গে ফরওয়ার্ড করলে রেট লিমিট এড়াতে ছোট বিরতি
                time.sleep(1) 
                return

        # বই সার্চ
        if update.message.text:
            query = update.message.text.lower().strip()
            # সার্চের লেখাতেও যদি ইউজার ভুল করে আন্ডারস্কোর দেয়, তা সরিয়ে চেক করা
            query_clean = query.replace("_", " ").replace("-", " ")
            
            all_books = sheet.get_all_records()
            found = False
            for row in all_books:
                book_in_sheet = str(row['Book Name']).lower()
                # আংশিক মিল (Partial Match) চেক করা হচ্ছে
                if query_clean in book_in_sheet:
                    await update.message.reply_text(f"বই পাওয়া গেছে: {row['Book Name']}\nপাঠানো হচ্ছে...")
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=row['File ID'])
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
    print("বট সচল...")
    app.run_polling()

if __name__ == '__main__':
    main()
    
