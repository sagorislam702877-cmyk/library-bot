import logging
import gspread
import os
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. কনফিগারেশন 
TOKEN = '8762483955:AAHUigW64ikrWN39Ok5l4eGPvtRVewX-zMg' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 

# ২. ওয়েব সার্ভার (রেন্ডার চালু রাখার জন্য)
web_app = Flask('')
@web_app.route('/')
def home(): return "Bot is Online and Ready!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()

# ৩. গুগল শিট কানেকশন 
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. স্টার্ট ও হেল্প সেকশন
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if user_id not in all_users:
            user_sheet.append_row([user_id])
    except: pass
    
    await update.message.reply_text(
        "আসসালামু আলাইকুম।\nঅনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে মেসেজ দিন।\n\nবট ব্যবহারের নিয়ম জানতে /help লিখুন।"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 বট ব্যবহারের গাইডলাইন:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. অ্যাডমিনের সাথে যোগাযোগ: নতুন বই বা সমস্যার জন্য /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# ৫. অটোমেটিক বই আপলোড
async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    caption = update.message.caption
    
    book_name = caption if caption else doc.file_name
    if not book_name: book_name = "Unknown_Book"
    
    if book_name.lower().endswith(".pdf"):
        book_name = book_name[:-4]

    book_name = book_name.replace("_", " ").strip()

    await update.message.reply_text("বইটি সেভ হচ্ছে...")

    try:
        book_sheet, _ = get_sheets()
        book_sheet.append_row([book_name, doc.file_id])
        await update.message.reply_text(f"✅ সফলভাবে সেভ হয়েছে!\n\nনাম: {book_name}")
    except:
        await update.message.reply_text("❌ শিটে সেভ করতে সমস্যা হয়েছে।")

# ৬. অ্যাডমিন কমান্ডস ও রিপ্লাই ফিচার
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1
        await update.message.reply_text(f"📊 স্ট্যাটাস:\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি")
    except: await update.message.reply_text("তথ্য পাওয়া যায়নি।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("/broadcast এর পর মেসেজ লিখুন।")
        return
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)[1:]
    for u_id in users:
        try: await context.bot.send_message(chat_id=u_id, text=f"📢 অ্যাডমিন নোটিশ:\n\n{msg}")
        except: continue
    await update.message.reply_text("ব্রডকাস্ট সম্পন্ন হয়েছে।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    user_id = update.effective_user.id
    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার মেসেজটি দিন।")
        return
    
    # অ্যাডমিনকে জানানো (আইডি সহ)
    await context.bot.send_message(
        chat_id=ADMIN_ID, 
        text=f"📩 নতুন মেসেজ!\nইউজার আইডি: {user_id}\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে লিখুন: /reply {user_id} আপনার বার্তা"
    )
    await update.message.reply_text("আপনার মেসেজটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    
    if len(context.args) < 2:
        await update.message.reply_text("সঠিক নিয়ম: /reply [user_id] [মেসেজ]")
        return
    
    try:
        target_id = context.args[0]
        reply_msg = " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"📩 অ্যাডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text(f"✅ ইউজার {target_id} কে রিপ্লাই পাঠানো হয়েছে।")
    except Exception as e:
        await update.message.reply_text(f"❌ পাঠানো সম্ভব হয়নি। ভুল আইডি বা ইউজার বট ব্লক করেছে।")

# ৭. মেইন রানার
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reply", reply_to_user)) # নতুন রিপ্লাই কমান্ড
    
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    
    async def search(update, context):
        query = update.message.text.strip().replace(" ", "").lower()
        try:
            book_sheet, _ = get_sheets()
            all_data = book_sheet.get_all_values()
            found = False
            for row in all_data[1:]:
                if query in row[0].replace(" ", "").lower():
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"বই: {row[0]}")
                    found = True
            if not found:
                await update.message.reply_text("দুঃখিত, বইটি পাওয়া যায়নি।")
        except: pass
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
        
