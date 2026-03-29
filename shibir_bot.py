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

# ৪. স্টার্ট ও হেল্প কমান্ড
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
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. অ্যাডমিন যোগাযোগ: /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই আমার অমুক বই প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# ৫. উন্নত অটো-আপলোড (ক্যাপশন থেকে নাম ও আন্ডারস্কোর রিমুভ)
async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    caption = update.message.caption
    
    # ক্যাপশন থাকলে সেটি নিবে, না থাকলে ফাইলের নাম নিবে
    book_name = caption if caption else doc.file_name
    if not book_name: book_name = "Unknown_Book"
    
    if book_name.lower().endswith(".pdf"):
        book_name = book_name[:-4]

    # আন্ডারস্কোর (_) সরিয়ে স্পেস দেওয়া
    book_name = book_name.replace("_", " ").strip()

    await update.message.reply_text(f"বইটি সেভ হচ্ছে...")

    try:
        book_sheet, _ = get_sheets()
        book_sheet.append_row([book_name, doc.file_id])
        await update.message.reply_text(f"✅ সফলভাবে সেভ হয়েছে!\n\nনাম: {book_name}")
    except:
        await update.message.reply_text("❌ শিটে সেভ করতে সমস্যা হয়েছে।")

# ৬. অ্যাডমিন প্যানেল কমান্ডস
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1
        await update.message.reply_text(f"মোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি")
    except: await update.message.reply_text("তথ্য সংগ্রহ করতে সমস্যা হচ্ছে।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("মেসেজ দিতে /broadcast এর পর আপনার কথাটি লিখুন।")
        return
    
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)[1:]
    
    count = 0
    await update.message.reply_text("ব্রডকাস্ট শুরু হয়েছে...")
    for u_id in users:
        try:
            await context.bot.send_message(chat_id=u_id, text=f"অ্যাডমিন নোটিশ:\n\n{msg}")
            count += 1
            await asyncio.sleep(0.05)
        except: continue
    await update.message.reply_text(f"সফলভাবে {count} জন ইউজারকে মেসেজ পাঠানো হয়েছে।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("/admin এর পর আপনার বার্তা লিখুন।")
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

# ৭. স্মার্ট সার্চ ও ফাইল ডেলিভারি
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip().replace(" ", "").lower()
    if not user_query: return
    try:
        book_sheet, _ = get_sheets()
        all_data = book_sheet.get_all_values()
        if len(all_data) < 2: return
        
        matches = []
        for row in all_data[1:]:
            if user_query in row[0].replace(" ", "").lower():
                matches.append((row[0], row[1]))

        if not matches:
            await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি।")
            return

        if len(matches) > 1:
            await update.message.reply_text(f"আপনার সার্চের সাথে মিল থাকা {len(matches)}টি বই পাঠানো হচ্ছে...")
        
        for name, fid in matches[:10]:
            try:
                await context.bot.send_document(chat_id=update.effective_chat.id, document=fid, caption=f"বই: {name}")
                await asyncio.sleep(0.5) 
            except: continue
    except: pass

# ৮. মেইন রানার (সব হ্যান্ডলার এখানে আছে)
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    # সকল কমান্ড হ্যান্ডলার
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    
    # ফাইল আপলোড হ্যান্ডলার
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    
    # মেসেজ সার্চ হ্যান্ডলার
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
               
