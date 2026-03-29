import logging
import gspread
import os
import asyncio
import google.generativeai as genai
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. কনফিগারেশন
TOKEN = '8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 
GEMINI_API_KEY = "AIzaSyAuT06iRlvTPkDtzkyaV4u7eW_rMLqXSsc" 

# AI সেটআপ
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

# ২. ওয়েব সার্ভার (রেন্ডার চালু রাখার জন্য)
web_app = Flask('')
@web_app.route('/')
def home(): return "Shibir Online Library Bot is Fully Live!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()

# ৩. গুগল শিট কানেকশন
def get_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SHEET_NAME)
        return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")
    except Exception as e:
        logging.error(f"Sheet Error: {e}")
        return None, None

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. ইউজার কমান্ডস
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        _, user_sheet = get_sheets()
        if user_id not in user_sheet.col_values(1):
            user_sheet.append_row([user_id])
    except: pass
    await update.message.reply_text("আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইটির নাম লিখে মেসেজ দিন।")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 **বট ব্যবহারের গাইডলাইন:**\n\n"
        "১. **বই খোঁজা:** সরাসরি বইয়ের নাম লিখুন (বাংলা, ইংরেজি বা আংশিক নাম হলেও চলবে)।\n"
        "২. **অ্যাডমিন:** নতুন বই বা সমস্যার জন্য `/admin` লিখে আপনার কথাটি লিখুন।"
    )
    await update.message.reply_text(help_text)

# ৫. অ্যাডমিন ফিচারস (Stats, Broadcast, Reply)
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        book_sheet, user_sheet = get_sheets()
        total_users = len(user_sheet.col_values(1)) - 1
        total_books = len(book_sheet.col_values(1)) - 1
        await update.message.reply_text(f"📊 **স্ট্যাটাস:**\nমোট ইউজার: {max(0, total_users)} জন\nমোট বই: {max(0, total_books)} টি")
    except: await update.message.reply_text("তথ্য পাওয়া যায়নি।")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("ব্যবহার: `/broadcast আপনার মেসেজ`")
        return
    _, user_sheet = get_sheets()
    users = user_sheet.col_values(1)[1:]
    count = 0
    for u_id in users:
        try: 
            await context.bot.send_message(chat_id=u_id, text=f"📢 **অ্যাডমিন নোটিশ:**\n\n{msg}")
            count += 1
        except: continue
    await update.message.reply_text(f"✅ {count} জন ইউজারের কাছে পাঠানো হয়েছে।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    user_id = update.effective_user.id
    if not user_msg:
        await update.message.reply_text("ব্যবহার: `/admin আপনার মেসেজ`")
        return
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"📩 **নতুন মেসেজ!**\nইউজার আইডি: `{user_id}`\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে: `/reply {user_id} আপনার বার্তা`",
        parse_mode='Markdown'
    )
    await update.message.reply_text("মেসেজটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if len(context.args) < 2:
        await update.message.reply_text("ব্যবহার: `/reply [user_id] [বার্তা]`")
        return
    try:
        target_id, reply_msg = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"📩 **অ্যাডমিন রিপ্লাই:**\n\n{reply_msg}")
        await update.message.reply_text(f"✅ ইউজারকে রিপ্লাই পাঠানো হয়েছে।")
    except: await update.message.reply_text("❌ পাঠানো যায়নি।")

# ৬. স্মার্ট AI সার্চ (বাংলা-ইংরেজি ও আংশিক নাম সাপোর্ট)
async def handle_ai_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    book_sheet, _ = get_sheets()
    if not book_sheet: return

    try:
        all_rows = book_sheet.get_all_values()
        available_books = [row[0] for row in all_rows[1:]]
        books_string = ", ".join(available_books)

        prompt = (
            f"ইউজার লিখেছে: '{user_text}'. লাইব্রেরির বই: {books_string}.\n"
            "নির্দেশনা:\n"
            "১. ইউজার ইংরেজি (subhe sadik) বা আংশিক (কর্মী সহায়িকা) নাম লিখলে উচ্চারণ মিলিয়ে সঠিক বাংলা বইটি খুঁজে বের করো।\n"
            "২. যদি নিশ্চিত হও এটি কোনো বই, তবে শুধু সেই সঠিক বইটির হুবহু নাম দাও।\n"
            "৩. না মিললে সম্ভাব্য সাজেশান দাও বা বাংলায় সংক্ষেপে কথা বলো।"
        )
        
        response = ai_model.generate_content(prompt)
        ai_res = response.text.strip()

        found = False
        for row in all_rows[1:]:
            if ai_res.lower() == row[0].lower() or row[0].lower() in ai_res.lower():
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"✅ **আপনার বই:** {row[0]}")
                found = True
                break
        
        if not found:
            await update.message.reply_text(ai_res)
    except Exception as e:
        logging.error(f"AI Search Error: {e}")

# ৭. বই আপলোড ও ক্যাপশন থেকে নাম সেভ (অ্যাডমিন)
async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    # ক্যাপশন থাকলে সেটা নাম হবে, না থাকলে ফাইলের নাম
    name = (update.message.caption if update.message.caption else doc.file_name)
    name = name.replace(".pdf", "").replace("_", " ").strip()
    
    try:
        book_sheet, _ = get_sheets()
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"✅ **সেভ হয়েছে:** {name}\nএখন ইউজাররা এটি সার্চ করে পাবে।")
    except: await update.message.reply_text("❌ শিটে সেভ করা যায়নি।")

# ৮. মেইন রানার
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reply", reply_to_user))
    
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ai_search))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
        
