import logging
import gspread
import os
import time
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from flask import Flask
from threading import Thread

# ১. আপনার তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 

active_searches = 0

# ২. ওয়েব সার্ভার
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
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open("MyBotDB")
    return spreadsheet.sheet1, spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ৪. অটো ইউজার সেভ
def save_user(user_id):
    try:
        _, user_sheet = get_sheets()
        existing_users = user_sheet.col_values(1)
        if str(user_id) not in existing_users:
            user_sheet.append_row([str(user_id)])
    except Exception as e:
        logging.error(f"User Save Error: {e}")

# ৫. স্টার্ট কমান্ড
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id)
    welcome_text = (
        "✨ **আসসালামু আলাইকুম!** ✨\n\n"
        "আমাদের **অনলাইন লাইব্রেরি বটে** আপনাকে স্বাগত। 📚\n\n"
        "🔍 বইয়ের নাম লিখে মেসেজ দিন।\n"
        "👨‍💻 সরাসরি অ্যাডমিনের সাথে কথা বলতে চাইলে `/admin` কমান্ডটি ব্যবহার করুন।"
    )
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

# ৬. ব্রডকাস্ট ফিচার
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = " ".join(context.args)
    if not msg:
        await update.message.reply_text("⚠️ ব্যবহার: `/broadcast আপনার মেসেজ`")
        return
    _, user_sheet = get_sheets()
    user_ids = user_sheet.col_values(1)[1:]
    await update.message.reply_text(f"🚀 ব্রডকাস্ট শুরু হয়েছে...")
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=f"📢 **নোটিশ:**\n\n{msg}", parse_mode='Markdown')
            await asyncio.sleep(0.1)
        except: pass
    await update.message.reply_text("✅ সম্পন্ন!")

# ৭. হাইড এডমিন কন্টাক্ট সিস্টেম
async def admin_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("⚠️ **অ্যাডমিনকে মেসেজ দিতে এভাবে লিখুন:**\n`/admin আপনার কথাটি এখানে লিখুন`")
        return

    admin_alert = (
        f"📩 **নতুন মেসেজ এসেছে!**\n\n"
        f"👤 ইউজার: {update.effective_user.first_name}\n"
        f"🆔 আইডি: `{update.effective_user.id}`\n"
        f"💬 মেসেজ: {user_msg}\n\n"
        f"📝 রিপ্লাই দিতে লিখুন: `/reply {update.effective_user.id} আপনার উত্তর`"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=admin_alert, parse_mode='Markdown')
    await update.message.reply_text("✅ আপনার মেসেজটি গোপনীয়ভাবে অ্যাডমিনের কাছে পাঠানো হয়েছে।")

# ৮. অ্যাডমিন রিপ্লাই দেওয়ার সিস্টেম
async def admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        target_id = context.args[0]
        reply_msg = " ".join(context.args[1:])
        await context.bot.send_message(chat_id=target_id, text=f"👨‍💻 **অ্যাডমিন থেকে উত্তর:**\n\n{reply_msg}", parse_mode='Markdown')
        await update.message.reply_text(f"✅ উত্তর পাঠানো হয়েছে।")
    except:
        await update.message.reply_text("⚠️ ব্যবহার: `/reply UserID আপনার উত্তর`")

# ৯. হেল্প কমান্ড (নতুন ইনস্ট্রাকশনসহ)
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 **বট ব্যবহারের নিয়মাবলী:**\n\n"
        "১. **বই খোঁজা:** সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. **অ্যাডমিনের সাথে যোগাযোগ:** আপনার পরিচয় গোপন রেখে সরাসরি অ্যাডমিনকে কিছু জানাতে চাইলে এভাবে লিখুন:\n"
        "`/admin আপনার মেসেজ` (যেমন: `/admin ভাই এই বইটি দরকার`)\n\n"
        "🛡️ **গোপনীয়তা:** অ্যাডমিন কমান্ড ব্যবহার করলে আপনার ব্যক্তিগত প্রোফাইল বা ইউজারনেম অ্যাডমিন দেখতে পাবেন না। শুধু আপনার মেসেজটি তার কাছে পৌঁছাবে।"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')

# ১০. পরিসংখ্যান ও মেসেজ হ্যান্ডলার
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        b_sheet, u_sheet = get_sheets()
        await update.message.reply_text(f"📊 ইউজার: {len(u_sheet.col_values(1))-1}\n📚 বই: {len(b_sheet.col_values(1))-1}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_searches
    try:
        if update.message.text:
            query = update.message.text.lower().strip()
            active_searches += 1
            book_sheet, _ = get_sheets()
            all_books = book_sheet.get_all_records()
            found_books = [row for row in all_books if query in str(row['Book Name']).lower()]

            if found_books:
                for book in found_books:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=book['File ID'], caption=f"📖 {book['Book Name']}")
            else:
                await update.message.reply_text("❌ বই পাওয়া যায়নি।")
            active_searches -= 1
        elif update.message.document and update.effective_user.id == ADMIN_ID:
            book_sheet, _ = get_sheets()
            raw_name = update.message.document.file_name.replace(".pdf", "").replace("_", " ").strip()
            book_sheet.append_row([raw_name, update.message.document.file_id])
            await update.message.reply_text(f"✅ যুক্ত হয়েছে: {raw_name}")
    except:
        if active_searches > 0: active_searches -= 1

def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("admin", admin_contact))
    app.add_handler(CommandHandler("reply", admin_reply))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
    
