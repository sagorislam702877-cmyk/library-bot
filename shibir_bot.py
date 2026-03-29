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
TOKEN = '8762483955:AAF9GLhTVaIZWfP0ybduNVBFVVJ5-HWHe3Y' # নতুন টোকেন বসানো হয়েছে
ADMIN_ID = 8596482199
SHEET_NAME = "MyBotDB"
GEMINI_API_KEY = "AIzaSyAuT06iRlvTPkDtzkyaV4u7eW_rMLqXSsc"

# AI সেটআপ
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel('gemini-1.5-flash')

# ২. ওয়েব সার্ভার (রেন্ডার চালু রাখার জন্য)
web_app = Flask('')
@web_app.route('/')
def home(): return "AI Library Bot is Live with New Token!"

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

# ৪. কমান্ডসমূহ
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    try:
        _, user_sheet = get_sheets()
        if user_id not in user_sheet.col_values(1):
            user_sheet.append_row([user_id])
    except: pass
    await update.message.reply_text("আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নামটি লিখুন।")

async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("/admin লিখে আপনার কথাটি লিখুন।")
        return
    await context.bot.send_message(chat_id=ADMIN_ID, text=f"📩 মেসেজ!\nID: `{update.effective_user.id}`\nবার্তা: {user_msg}", parse_mode='Markdown')
    await update.message.reply_text("মেসেজটি অ্যাডমিনের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    try:
        target_id, reply_msg = context.args[0], " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"📩 অ্যাডমিন রিপ্লাই:\n\n{reply_msg}")
        await update.message.reply_text("✅ পাঠানো হয়েছে।")
    except: await update.message.reply_text("❌ পাঠানো যায়নি।")

# ৫. স্মার্ট AI সার্চ ও সাজেস্ট ইঞ্জিন
async def handle_ai_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text.strip()
    try:
        book_sheet, _ = get_sheets()
        all_rows = book_sheet.get_all_values()
        available_books = [row[0] for row in all_rows[1:]]
        books_string = ", ".join(available_books)

        prompt = (
            f"ইউজার লিখেছে: '{user_text}'.\n"
            f"আমাদের লাইব্রেরিতে এই বইগুলো আছে: {books_string}.\n\n"
            "নির্দেশনা:\n"
            "১. ইউজার ইংরেজি (যেমন: subhe sadik) বা ভুল বানান বা আংশিক নাম (যেমন: কর্মী সহায়িকা) লিখলে উচ্চারণ ও অর্থ মিলিয়ে আমাদের তালিকার সঠিক পুরো বইটি খুঁজে বের করো।\n"
            "২. যদি নিশ্চিত হও এটি কোনো বই, তবে উত্তর হিসেবে শুধু সেই সঠিক বইটির পুরো নাম দাও (কোনো বাড়তি কথা লিখবে না)।\n"
            "৩. যদি পুরোপুরি নিশ্চিত না হও তবে সম্ভাব্য ২-৩টি বই সাজেস্ট করো সংক্ষেপে (যেমন: আপনি কি ... খুঁজছেন?)।\n"
            "৪. অন্যথায় সাধারণ কথা হিসেবে বাংলায় উত্তর দাও।"
        )
        
        response = ai_model.generate_content(prompt)
        ai_response = response.text.strip()

        found = False
        for row in all_rows[1:]:
            # AI এর উত্তরের সাথে শিটের বইয়ের নাম নিখুঁতভাবে মেলানো হচ্ছে
            if ai_response.lower() == row[0].lower() or row[0].lower() in ai_response.lower():
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row[1], caption=f"✅ আপনার বই: {row[0]}")
                found = True
                break
        
        if not found:
            await update.message.reply_text(ai_response)
            
    except Exception as e:
        logging.error(f"AI Error: {e}")

# ৬. বই আপলোড (অ্যাডমিন)
async def upload_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    doc = update.message.document
    name = (update.message.caption if update.message.caption else doc.file_name).replace(".pdf", "").replace("_", " ").strip()
    try:
        book_sheet, _ = get_sheets()
        book_sheet.append_row([name, doc.file_id])
        await update.message.reply_text(f"✅ সেভ হয়েছে: {name}")
    except: pass

# ৭. মেইন রানার
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(MessageHandler(filters.Document.ALL, upload_book))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ai_search))
    
    # drop_pending_updates=True জ্যাম ছাড়াতে সাহায্য করবে
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
