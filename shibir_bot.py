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
def home(): return "Bot is Online and Super Fast!"

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

# ৪. স্টার্ট ও হেল্প কমান্ড
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        # শুধু স্টার্ট করলেই ইউজার লিস্টে নাম উঠবে
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
        "বট ব্যবহারের গাইডলাইন:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন। নাম মিলে গেলে বা কাছাকাছি নাম থাকলেও বট সরাসরি পিডিএফ ফাইল পাঠিয়ে দেবে।\n"
        "২. অ্যাডমিনের সাথে যোগাযোগ: নতুন বইয়ের অনুরোধে /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# ৫. অ্যাডমিন প্যানেল (Stats, Broadcast, Admin, Reply)
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        
        all_users = user_sheet.col_values(1)
        total_users = len(all_users) - 1 if "User ID" in all_users else len(all_users)
        
        all_books = book_sheet.col_values(1)
        total_books = len(all_books) - 1 if "Book Name" in all_books else len(all_books)
        
        status_msg = (
            "লাইব্রেরি স্ট্যাটাস রিপোর্ট:\n\n"
            f"মোট ইউজার সংখ্যা: {max(0, total_users)} জন\n"
            f"মোট বইয়ের সংখ্যা: {max(0, total_books)} টি"
        )
        await update.message.reply_text(status_msg)
    except:
        await update.message.reply_text("তথ্য সংগ্রহ করতে সমস্যা হচ্ছে।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("মেসেজ দিতে /broadcast এর পর আপনার কথাটি লিখুন।")
        return
    
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)
    users = [u for u in users if u != "User ID"]
    
    count = 0
    await update.message.reply_text("ব্রডকাস্ট শুরু হয়েছে...")
    for user_id in users:
        try:
            await context.bot.send_message(chat_id=user_id, text=f"অ্যাডমিন থেকে নোটিশ:\n\n{msg}")
            count += 1
            await asyncio.sleep(0.05)
        except: continue
    
    await update.message.reply_text(f"সফলভাবে {count} জন ইউজারকে মেসেজ পাঠানো হয়েছে।")

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

# ৬. ফাস্ট সার্চ ও ফাইল সেন্ডিং 
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    if not user_query: return
    
    try:
        book_sheet, _ = get_sheets() # সার্চে শুধু বইয়ের ডাটাবেজ ডাকবে
        
        all_rows = book_sheet.get_all_values()
        if len(all_rows) < 2: return
        
        books = all_rows[1:] 
        matches = []

        # স্পেস সরিয়ে সার্চ লজিক (যেন 'কর্মপদ্ধতি' আর 'কর্ম পদ্ধতি' দুটোই কাজ করে)
        clean_query = user_query.replace(" ", "").lower()

        for row in books:
            db_book_name = row[0].replace(" ", "").lower()
            if clean_query in db_book_name:
                matches.append((row[0], row[1]))

        if not matches:
            await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি। বানানটি একটু চেক করে দেখুন।")
            return

        if len(matches) > 1:
            await update.message.reply_text(f"আপনার খোঁজা নামের সাথে মিল থাকা {len(matches)}টি বই পাওয়া গেছে। পাঠানো হচ্ছে...")
        
        for name, fid in matches[:10]: # একবারে সর্বোচ্চ ১০টি লিমিট
            try:
                await context.bot.send_document(chat_id=update.effective_chat.id, document=fid, caption=f"বই: {name}")
                await asyncio.sleep(0.5) 
            except: continue

    except Exception as e:
        await update.message.reply_text("সার্ভার এই মুহূর্তে একটু ব্যস্ত আছে। দয়া করে ১০ সেকেন্ড পর আবার বইয়ের নামটি লিখুন।")
        logging.error(f"Search error: {e}")

# ৭. মেইন রানার 
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
