import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ১. আপনার সরবরাহকৃত তথ্য
TOKEN = '8762483955:AAFSG9blBOjRFbO2S5rDY2U3NxMX9y9oEgo'
ADMIN_ID = 8596482199 

# ২. গুগল শিট কানেক্ট করার ফাংশন
def connect_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # নিশ্চিত করুন creds.json ফাইলটি একই ফোল্ডারে আছে
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    # শিটের নাম 'MyBotDB' হতে হবে
    return client.open("MyBotDB").sheet1 

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("আসসালামু আলাইকুম! @ShibirOnlineLibraryBot এ স্বাগতম।\nবইয়ের নাম লিখে সার্চ দিন।")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sheet = connect_sheet()
    
    # বই আপলোড (শুধুমাত্র আপনি আপনার আইডি থেকে পিডিএফ পাঠালে সেভ হবে)
    if update.message.document and update.effective_user.id == ADMIN_ID:
        doc = update.message.document
        if doc.mime_type == 'application/pdf':
            # ফাইলের নাম থেকে '.pdf' বাদ দিয়ে পরিষ্কার নাম নেওয়া
            book_name = doc.file_name.lower().replace(".pdf", "").strip()
            file_id = doc.file_id
            
            # গুগল শিটে কলাম অনুযায়ী সেভ করা
            sheet.append_row([book_name, file_id])
            await update.message.reply_text(f"✅ লাইব্রেরিতে যুক্ত হয়েছে: {doc.file_name}")
            return

    # বই সার্চ (যেকোনো ইউজার টেক্সট পাঠালে)
    if update.message.text:
        query = update.message.text.lower().strip()
        all_books = sheet.get_all_records()
        
        found = False
        for row in all_books:
            # যদি ইউজারের লেখা নাম গুগল শিটের কোনো নামের সাথে আংশিক মিলে যায়
            if query in str(row['Book Name']):
                await update.message.reply_text(f"বই পাওয়া গেছে: {row['Book Name']}\nপাঠানো হচ্ছে...")
                await context.bot.send_document(chat_id=update.effective_chat.id, document=row['File ID'])
                found = True
                break
        
        if not found:
            await update.message.reply_text("দুঃখিত, এই নামের কোনো বই আমাদের ডাটাবেসে নেই। সঠিক নাম লিখে চেষ্টা করুন।")

def main():
    # বট অ্যাপ্লিকেশন তৈরি
    app = Application.builder().token(TOKEN).build()
    
    # কমান্ড এবং মেসেজ হ্যান্ডলার যুক্ত করা
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.PDF | (filters.TEXT & ~filters.COMMAND), handle_message))
    
    print("বট সচল আছে এবং আপনার আইডির জন্য অপেক্ষা করছে...")
    app.run_polling()

if __name__ == '__main__':
    main()
      
