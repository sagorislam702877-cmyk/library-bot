import logging
import gspread
import os
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread
from fuzzywuzzy import process 

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

# ৩. গুগল শিট কানেকশন (অপ্টিমাইজড)
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. কমান্ড ফাংশন (Start & Help)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if user_id not in all_users:
            user_sheet.append_row([user_id])
    except: pass
    
    await update.message.reply_text(
        "আসসালামু আলাইকুম।\nঅনলাইন লাইব্রেরিতে স্বাগতম। বইয়ের নাম লিখে মেসেজ দিন।\n\nসাহায্যের জন্য /help লিখুন।"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "বট ব্যবহারের নিয়মাবলী:\n\n"
        "১. সরাসরি বইয়ের নাম লিখুন, বট আপনাকে ফাইল পাঠিয়ে দেবে।\n"
        "২. অ্যাডমিনের সাথে যোগাযোগের জন্য /admin লিখে আপনার কথাটি লিখুন।"
    )
    await update.message.reply_text(help_text)

# ৫. উন্নত অ্যাডমিন প্যানেল (Stats, Broadcast, Reply)
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        
        # ইউজার সংখ্যা (হেডার বাদ দিয়ে নির্ভুল গণনা)
        all_users = user_sheet.col_values(1)
        total_users = len(all_users) - 1 if "User ID" in all_users else len(all_users)
        
        # বইয়ের সংখ্যা গণনা
        all_books = book_sheet.col_values(1)
        total_books = len(all_books) - 1 if "Book Name" in all_books else len(all_books)
        
        status_msg = (
            "লাইব্রেরি স্ট্যাটাস রিপোর্ট:\n\n"
            f"মোট ইউজার সংখ্যা: {max(0, total_users)} জন\n"
            f"মোট বইয়ের সংখ্যা: {max(0, total_books)} টি\n"
            "সার্ভার স্ট্যাটাস: সচল (Live)"
        )
        await update.message.reply_text(status_msg)
    except Exception as e:
        await update.message.reply_text("তথ্য সংগ্রহ করতে সমস্যা হচ্ছে।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("মেসেজ দিতে /broadcast এর পর আপনার কথাটি লিখুন।")
        return
    
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)[1:] # হেডার বাদ দিয়ে শুধু ইউজার আইডি
    
    sent_count = 0
    await update.message.reply_text(f"ব্রডকাস্ট শুরু হয়েছে {len(users)} জনের কাছে...")
    
    for user_id in users:
        try:
            # বড় ইউজার বেইজের জন্য ০.০৫ সেকেন্ড বিরতি (Rate limit এড়াতে)
            await context.bot.send_message(chat_id=user_id, text=f"অ্যাডমিন নোটিশ:\n\n{msg}")
            sent_count += 1
            if sent_count % 20 == 0: await asyncio.sleep(1) # প্রতি ২০ মেসেজ পর ১ সেকেন্ড রেস্ট
        except: continue
    
    await update.message.reply_text(f"সফলভাবে {sent_count} জন ইউজারকে মেসেজ পাঠানো হয়েছে।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("মেসেজ পাঠাতে /admin এর পর আপনার বার্তা লিখুন।")
        return
    await context.bot.send_message(chat_id=ADMIN_ID, text=f"ইউজার বার্তা (ID: {user_id}):\n{user_msg}")
    await update.message.reply_text("আপনার বার্তাটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    target_id, msg = context.args[0], " ".join(context.args[1:])
    try:
        await context.bot.send_message(chat_id=target_id, text=f"অ্যাডমিনের উত্তর:\n\n{msg}")
        await update.message.reply_text("উত্তর পাঠানো হয়েছে।")
    except: await update.message.reply_text("বার্তাটি পাঠানো যায়নি।")

# ৬. হাই-লোড সার্চ ও ফাইল ডেলিভারি সিস্টেম 
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip().lower()
    if not user_query or len(user_query) < 2: return
    
    try:
        book_sheet, _ = get_sheets()
        all_data = book_sheet.get_all_values()
        books_data = all_data[1:] # হেডার বাদ দিয়ে
        
        matches = []
        for row in books_data:
            if user_query in row[0].lower():
                matches.append((row[0], row[1]))

        # ফাজি সার্চ (যদি সরাসরি না মেলে)
        if not matches:
            names = [r[0] for r in books_data]
            top_matches = process.extract(user_query, names, limit=3)
            for name, score in top_matches:
                if score > 60:
                    fid = next((r[1] for r in books_data if r[0] == name), None)
                    if fid: matches.append((name, fid))

        if not matches:
            await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
            return

        # সরাসরি ফাইল পাঠানো (লোড ব্যালেন্সিং সহ)
        if len(matches) > 1:
            await update.message.reply_text(f"আপনার সার্চের সাথে মিল থাকা {len(matches)}টি বই পাওয়া গেছে। পাঠানো হচ্ছে...")
        
        for name, fid in matches[:10]: # একবারে সর্বোচ্চ ১০টি ফাইল লিমিট
            try:
                await context.bot.send_document(chat_id=update.effective_chat.id, document=fid, caption=f"বই: {name}")
                await asyncio.sleep(0.5) 
            except: continue

    except Exception as e:
        logging.error(f"Search error: {e}")

# ৭. মেইন ফাংশন 
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    # লোড হ্যান্ডেল করার জন্য কনফিগারেশন
    app.run_polling(drop_pending_updates=True, connect_timeout=30, read_timeout=30)

if __name__ == '__main__': main()
    
