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
def home(): 
    return "Bot is Online!"

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

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. কমান্ড ফাংশন (Start, Help)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if str(user_id) not in all_users:
            user_sheet.append_row([str(user_id)])
    except: pass
    
    await update.message.reply_text(
        "আসসালামু আলাইকুম।\nঅনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে মেসেজ দিন।\n\nবট ব্যবহারের নিয়ম জানতে /help লিখুন।"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # এখানে শুধু ইউজারের জন্য প্রয়োজনীয় তথ্য রাখা হয়েছে
    help_text = (
        "বট ব্যবহারের গাইডলাইন:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন। নাম মিলে গেলে বট সরাসরি পিডিএফ ফাইল পাঠিয়ে দেবে।\n"
        "২. অ্যাডমিনের সাথে যোগাযোগ: নতুন বইয়ের অনুরোধে /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# ৫. অ্যাডমিন ফিচার (Stats, Broadcast, Reply) - এগুলো গোপন থাকবে
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        _, user_sheet = get_sheets()
        users = user_sheet.col_values(1)
        total_users = len(users)
        await update.message.reply_text(f"বর্তমানে বটের মোট ইউজার সংখ্যা: {total_users} জন")
    except:
        await update.message.reply_text("ইউজার সংখ্যা বের করতে সমস্যা হচ্ছে।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("মেসেজ দিতে /broadcast এর পর আপনার কথাটি লিখুন।")
        return
    
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)
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

# ৬. ডাইরেক্ট সার্চ লজিক 
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip().lower()
    if not user_query: return
    
    try:
        book_sheet, _ = get_sheets()
        all_rows = book_sheet.get_all_values()
        if len(all_rows) < 2: return
        
        books = all_rows[1:] 
        matches = []

        for row in books:
            if user_query in row[0].lower():
                matches.append((row[0], row[1]))

        if not matches:
            all_names = [row[0] for row in books]
            fuzzy_results = process.extract(user_query, all_names, limit=5)
            for name, score in fuzzy_results:
                if score > 50:
                    fid = next((r[1] for r in books if r[0] == name), None)
                    if fid: matches.append((name, fid))

        if not matches:
            await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
            return

        if len(matches) > 1:
            await update.message.reply_text(f"আপনার খোঁজা নামের সাথে মিল থাকা {len(matches)}টি বই পাওয়া গেছে। সবকটি পাঠানো হচ্ছে...")
        
        for name, fid in matches:
            try:
                await context.bot.send_document(chat_id=update.effective_chat.id, document=fid, caption=f"বই: {name}")
                await asyncio.sleep(0.6) 
            except: continue

    except Exception as e:
        logging.error(f"Error: {e}")

# ৭. মেইন রানার 
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
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
