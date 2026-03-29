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
TOKEN = '8762483955:AAHUigW64ikrWN39Ok5l4eGPvtRVewX-zMg' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 
GEMINI_API_KEY = "AIzaSyAuT06iRlvTPkDtzkyaV4u7eW_rMLqXSsc" 

# AI সেটআপ
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

# ২. ওয়েব সার্ভার (রেন্ডার চালু রাখার জন্য)
web_app = Flask('')
@web_app.route('/')
def home(): return "AI Library Bot is Live and Ready!"

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

# ৪. কমান্ডসমূহ (আগের সব ফিচার ঠিক রাখা হয়েছে)
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
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন (বানান ভুল হলেও সমস্যা নেই)।\n"
        "২. অ্যাডমিনের সাথে যোগাযোগ: নতুন বই বা সমস্যার জন্য /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# ৫. অ্যাডমিন কমান্ডস (Stats & Broadcast)
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

# ৬. যোগাযোগ ও রিপ্লাই ফিচার
async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    user_id = update.effective_user.id
    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার মেসেজটি দিন।")
        return
    await context.bot.send_message(
        chat_id=ADMIN_ID, 
        text=f"📩 নতুন মেসেজ!\nইউজার আইডি: {user_id}\nবার্তা: {user_msg}\n\nরিপ্লাই দিতে: /reply {user_id} আপনার বার্তা"
    )
    await update.message.reply_text("আপনার মেসেজটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if len(context.args) < 2:
        await update.message.reply_text("সঠিক নিয়ম: /reply [user_id] [মেসেজ]")
        return
    try:
        target_id, reply_msg = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"📩 অ্যাডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text(f"✅ ইউজার {target_id} কে রিপ্লাই পাঠানো হয়েছে।")
    except: await update.message.reply_text("❌ পাঠানো সম্ভব হয়নি।")

# ৭. AI ভিত্তিক স্মার্ট সার্চ এবং চ্যাট (বানান ভুল সংশোধন সহ)
async def handle_ai_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    try:
        book_sheet, _ = get_sheets()
        all_rows = book_sheet.get_all_values()
        available_books = [row[0] for row in all_rows[1:]]
        books_string = ", ".join(available_books)

        prompt = (
            f"ইউজার লিখেছে: '{user_text}'.\n"
            f"আমাদের লাইব্রেরিতে এই বইগুলো আছে: {books_string}.\n"
            "যদি ইউজারের লেখাটি আমাদের তালিকার কোনো বইয়ের সাথে মিলে যায় (বানান ভুল থাকলেও), "
            "তবে শুধুমাত্র সেই বইয়ের সঠিক নামটি আউটপুট হিসেবে দাও। "
            "আর যদি এটি কোনো বই না হয়, তবে সাধারণ কথা হিসেবে সংক্ষেপে বাংলায় উত্তর দাও।"
        )
        response = ai_model.generate_content(prompt)
        ai_response = response.text.strip()

        found = False
        for row in all_rows[1:]:
            if ai_response.lower() in row[0].lower() or row[0].lower() in ai_response.lower():
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"✅ আপনার বই: {row[0]}")
                found = True
                break
        if not found: await update.message.reply_text(ai_response)
    except: pass

# ৮. বই আপলোড ফিচার (অ্যাডমিন)
async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    name = update.message.caption if update.message.caption else doc.file_name
    name = name.replace(".pdf", "").replace("_", " ").strip()
    try:
        book_sheet, _ = get_sheets()
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"✅ বই সেভ হয়েছে: {name}")
    except: await update.message.reply_text("❌ শিটে সেভ করা যায়নি।")

# ৯. মেইন রানার
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    # হ্যান্ডলার রেজিস্টার
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("reply", reply_to_user))
    
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ai_search))
    
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
        
