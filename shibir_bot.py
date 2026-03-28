import logging
import gspread
import os
import asyncio
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from flask import Flask
from threading import Thread
from fuzzywuzzy import process 

# --- কনফিগারেশন ---
TOKEN = '8762483955:AAFai0evS1PBKMK1X6dVVa-dCzIc3oZEMCo' 
ADMIN_ID = 8596482199 
SHEET_NAME = "MyBotDB" 

# --- ওয়েব সার্ভার ---
web_app = Flask('')
@web_app.route('/')
def home(): return "Library Bot is Online!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()

# --- গুগল শিট কানেক্ট ---
def get_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    return spreadsheet.worksheet("Sheet1"), spreadsheet.worksheet("Users")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ১. স্টার্ট ও বিস্তারিত হেল্প কমান্ড ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        _, user_sheet = get_sheets()
        all_users = user_sheet.col_values(1)
        if str(user_id) not in all_users:
            user_sheet.append_row([str(user_id)])
    except: pass
    
    await update.message.reply_text(
        "আসসালামু আলাইকুম।\nঅনলাইন লাইব্রেরিতে আপনাকে স্বাগতম। আপনার প্রয়োজনীয় বইয়ের নাম লিখে আমাদের মেসেজ দিন।\n\n"
        "বট ব্যবহারের বিস্তারিত নিয়ম জানতে /help কমান্ডটি ব্যবহার করুন।"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 অনলাইন লাইব্রেরি বট ব্যবহারের পূর্ণাঙ্গ নির্দেশিকা:\n\n"
        "১. বই অনুসন্ধান:\n"
        "সরাসরি বইয়ের নাম লিখে মেসেজ দিন। যেমন: 'সংবিধান'। বইয়ের নামের অংশবিশেষ লিখলেও বট আপনাকে বই খুঁজে দেবে।\n\n"
        "২. একক বা তালিকাভুক্ত সব বই সংগ্রহ:\n"
        "সার্চ রেজাল্টে যদি একাধিক বইয়ের সাজেশন আসে, তবে আপনি নির্দিষ্ট কোনো বইয়ের নামের ওপর ক্লিক করে শুধুমাত্র সেই বইটি নিতে পারেন। আবার '📥 সাজেস্ট করা সব বই একসাথে নিন' বাটনে ক্লিক করে তালিকার সব বই এক ক্লিকেই সংগ্রহ করতে পারেন।\n\n"
        "৩. কর্তৃপক্ষকে বার্তা পাঠানো:\n"
        "কোনো নতুন বইয়ের অনুরোধ বা অভিযোগ জানাতে মেসেজের শুরুতে /admin লিখে আপনার কথাটি লিখুন।\n"
        "উদাহরণ: /admin ভাই, আমার অমুক বইটি প্রয়োজন।"
    )
    await update.message.reply_text(help_text)

# --- ২. অ্যাডমিন যোগাযোগ ---
async def contact_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_msg = " ".join(context.args)
    if not user_msg:
        await update.message.reply_text("মেসেজ পাঠাতে /admin এর পর আপনার কথাটি লিখুন।")
        return
    await context.bot.send_message(chat_id=ADMIN_ID, text=f"ইউজার মেসেজ (ID: {user_id}):\n{user_msg}")
    await update.message.reply_text("আপনার মেসেজটি কর্তৃপক্ষের কাছে পাঠানো হয়েছে।")

async def reply_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID or len(context.args) < 2: return
    target_id, msg = context.args[0], " ".join(context.args[1:])
    try:
        await context.bot.send_message(chat_id=target_id, text=f"কর্তৃপক্ষের উত্তর:\n\n{msg}")
        await update.message.reply_text("উত্তর পাঠানো হয়েছে।")
    except: await update.message.reply_text("বার্তাটি পাঠানো যায়নি।")

# --- ৩. উন্নত সার্চ লজিক (সাজেশনসহ) ---
async def search_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    if not user_query: return
    
    book_sheet, _ = get_sheets()
    all_rows = book_sheet.get_all_values()
    if len(all_rows) < 2: return
    
    books = all_rows[1:]
    matches = []

    # কি-ওয়ার্ড ও অংশবিশেষ সার্চ
    for row in books:
        if user_query.lower() in row[0].lower():
            matches.append((row[0], row[1]))

    # ফাজি সার্চ (বানান ভুল হলে)
    if not matches:
        all_names = [row[0] for row in books]
        fuzzy_results = process.extract(user_query, all_names, limit=8)
        for name, score in fuzzy_results:
            if score > 45:
                fid = next((r[1] for r in books if r[0] == name), None)
                if fid: matches.append((name, fid))

    if not matches:
        await update.message.reply_text("দুঃখিত, এই নামে কোনো বই পাওয়া যায়নি। সঠিক নাম লিখে পুনরায় চেষ্টা করুন।")
        return

    # রেজাল্ট প্রদর্শন
    if len(matches) == 1:
        await context.bot.send_document(chat_id=update.effective_chat.id, document=matches[0][1], caption=f"বই: {matches[0][0]}")
    else:
        # ইউনিক কি ব্যবহার করে সাজেশন লিস্ট সেভ রাখা
        search_id = str(hash(user_query + str(update.effective_user.id)))[-8:]
        context.user_data[f"list_{search_id}"] = matches[:10] 
        
        buttons = []
        # 'সাজেস্ট করা সব বই' বাটন
        buttons.append([InlineKeyboardButton("📥 সাজেস্ট করা সব বই একসাথে নিন", callback_data=f"all_{search_id}")])
        
        # একক বইয়ের বাটন
        for name, fid in matches[:10]:
            buttons.append([InlineKeyboardButton(f"📖 {name}", callback_data=f"one_{fid}")])
        
        await update.message.reply_text(
            f"আপনার অনুসন্ধানের ভিত্তিতে নিচের {len(matches[:10])}টি বই সাজেস্ট করা হলো। আপনি চাইলে নির্দিষ্ট একটি বই নিতে পারেন অথবা সবগুলো একসাথে সংগ্রহ করতে পারেন:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

# --- ৪. বাটন হ্যান্ডলার ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # নির্দিষ্ট একটি বই পাঠানো
    if query.data.startswith('one_'):
        file_id = query.data.replace("one_", "")
        try:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=file_id)
        except:
            await query.message.reply_text("দুঃখিত, ফাইলটি পাঠানো সম্ভব হচ্ছে না।")

    # সাজেস্ট করা সব বই পাঠানো
    elif query.data.startswith('all_'):
        search_id = query.data.replace("all_", "")
        suggested_books = context.user_data.get(f"list_{search_id}")
        
        if suggested_books:
            await query.edit_message_text(f"সাজেস্ট করা {len(suggested_books)}টি ফাইল পাঠানো হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...")
            for name, fid in suggested_books:
                try:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=fid, caption=f"বই: {name}")
                    await asyncio.sleep(0.6) # টেলিগ্রাম সার্ভারের রেট লিমিট এড়াতে বিরতি
                except: continue
        else:
            await query.edit_message_text("সেশনের মেয়াদ শেষ হয়েছে। অনুগ্রহ করে আবার সার্চ করুন।")

# --- ৫. মেইন রানার ---
def main():
    keep_alive()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", contact_admin))
    app.add_handler(CommandHandler("reply", reply_to_user))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_book))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
    
