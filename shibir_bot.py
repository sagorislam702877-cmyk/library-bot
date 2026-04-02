from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from html import escape
from threading import Thread
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

# ✅ GOOGLE CREDS AUTO CREATE
google_creds_json = os.environ.get("GOOGLE_CREDS_JSON", "").strip()

if google_creds_json and not os.path.exists("creds.json"):
    with open("creds.json", "w", encoding="utf-8") as f:
        f.write(google_creds_json)

# ✅ NOW IMPORT GOOGLE LIBS
import gspread
from flask import Flask
from oauth2client.service_account import ServiceAccountCredentials

# ✅ TELEGRAM IMPORTS
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Additional imports
# The bot uses get_close_matches and several Telegram-specific exceptions which
# must be imported explicitly. Without these imports the code will raise
# NameError at runtime. We also import standard difflib for fuzzy matching.
from difflib import get_close_matches
from telegram.error import RetryAfter, TimedOut, NetworkError, Forbidden, BadRequest

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip("@")
SHEET_NAME = os.environ.get("SHEET_NAME", "MyBotDB").strip()
BOOK_SHEET_NAME = os.environ.get("BOOK_SHEET_NAME", "Sheet1").strip()
USER_SHEET_NAME = os.environ.get("USER_SHEET_NAME", "Users").strip()
LOG_SHEET_NAME = os.environ.get("LOG_SHEET_NAME", "Logs").strip()
MONTHLY_OVERVIEW_SHEET_NAME = os.environ.get("MONTHLY_OVERVIEW_SHEET_NAME", "Monthly Overview").strip()
MONTHLY_ITEM_SHEET_NAME = os.environ.get("MONTHLY_ITEM_SHEET_NAME", "Monthly Items").strip()
MONTHLY_DATA_URL = os.environ.get("MONTHLY_DATA_URL", "").strip()
MONTHLY_DATA_FILE = os.environ.get("MONTHLY_DATA_FILE", "").strip()
DEFAULT_YEAR = os.environ.get("DEFAULT_MONTHLY_YEAR", "2026").strip() or "2026"
AUTO_SYNC_PLAN_ON_START = os.environ.get("AUTO_SYNC_PLAN_ON_START", "true").strip().lower() == "true"
PORT = int(os.environ.get("PORT", "8080"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if ADMIN_ID == 0:
    raise RuntimeError("ADMIN_ID missing")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

MONTH_ORDER = ["january","february","march","april","may","june","july","august","september","october","november","december"]
MONTH_BN = {"january":"জানুয়ারি","february":"ফেব্রুয়ারি","march":"মার্চ","april":"এপ্রিল","may":"মে","june":"জুন","july":"জুলাই","august":"আগস্ট","september":"সেপ্টেম্বর","october":"অক্টোবর","november":"নভেম্বর","december":"ডিসেম্বর"}
CATEGORY_BN = {"kormi":"কর্মী", "sathi":"সাথী", "sodosso":"সদস্য"}
SECTION_ORDER = ["hadith", "literature", "class_topic", "discussion_topic"]

web_app = Flask(__name__)
@web_app.route("/")
def home():
    return "Bot running"

def run_web():
    import logging as flask_logging
    flask_logging.getLogger("werkzeug").setLevel(flask_logging.ERROR)
    web_app.run(host="0.0.0.0", port=PORT, use_reloader=False)

_cached_book_sheet = None
_cached_user_sheet = None
_cached_log_sheet = None
_cached_monthly_overview_sheet = None
_cached_monthly_item_sheet = None
BOOK_CACHE = {"rows": [], "lookup": {}, "indexed": [], "by_id": {}, "ts": 0.0}
MONTHLY_CACHE = {"overview": {}, "items_by_month": {}, "items_by_id": {}, "ts": 0.0}
USER_IDS: set[int] = set()
LOG_QUEUE: list[list[str]] = []
NEW_USER_QUEUE: set[int] = set()
_callback_cache: dict[str, list[str]] = {}
_admin_reply_map: dict[int, int] = {}

# --------------------------------------------------------------------------------------
# Helper for Telegram deep links
#
# When monthly study sections include books, we convert those book titles to
# clickable links that redirect back to the bot via the /start command.  This
# helper function constructs such a deep link using the bot's username and
# the monthly item ID.  If either is missing, an empty string is returned.
def build_deep_link(monthly_item_id: str) -> str:
    """
    Build a Telegram deep link for a monthly item ID.

    When a user clicks on a linked book title, the bot should receive a
    /start command with the payload "mi_{monthly_item_id}". This helper
    constructs the appropriate URL using the bot's username. If either the
    bot username or the item ID is missing, an empty string is returned.

    Args:
        monthly_item_id: The unique identifier for the monthly item.

    Returns:
        A full t.me deep link URL or an empty string if not applicable.
    """
    if not BOT_USERNAME or not monthly_item_id:
        return ""
    return f"https://t.me/{BOT_USERNAME}?start=mi_{monthly_item_id}"


def main_reply_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["📚 মাসিক পড়াশোনা"]], resize_keyboard=True, one_time_keyboard=False)


def monthly_category_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👨‍💼 কর্মী", callback_data="ms|cat|kormi"), InlineKeyboardButton("🤝 সাথী", callback_data="ms|cat|sathi")],
        [InlineKeyboardButton("🧑‍🎓 সদস্য", callback_data="ms|cat|sodosso")],
        [InlineKeyboardButton("🔙 ফিরে যান", callback_data="ms|back|root")],
    ])


def monthly_month_keyboard(category: str, year: str = DEFAULT_YEAR) -> InlineKeyboardMarkup:
    rows = []
    cur = []
    for month in MONTH_ORDER:
        cur.append(InlineKeyboardButton(MONTH_BN[month], callback_data=f"ms|month|{category}|{year}|{month}"))
        if len(cur) == 2:
            rows.append(cur)
            cur = []
    if cur:
        rows.append(cur)
    rows.append([InlineKeyboardButton("🔙 ক্যাটাগরি", callback_data="ms|home")])
    return InlineKeyboardMarkup(rows)


def monthly_pdf_keyboard(category: str, year: str, month: str, items: list[dict[str, str]]) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        title = item.get("item_name", "")
        item_id = item.get("monthly_item_id", "")
        if not title or not item_id:
            continue
        rows.append([InlineKeyboardButton(f"📘 {title[:45] + ('...' if len(title) > 45 else '')}", callback_data=f"ms|pdf|{item_id}")])
    rows.append([InlineKeyboardButton("🔙 মাসে ফিরে যান", callback_data=f"ms|month|{category}|{year}|{month}")])
    return InlineKeyboardMarkup(rows)


def make_inline_keyboard(titles: list[str]) -> InlineKeyboardMarkup:
    token = str(time.time_ns())[-10:]
    _callback_cache[token] = titles[:10]
    return InlineKeyboardMarkup([[InlineKeyboardButton(title, callback_data=f"pick|{token}|{i}")] for i, title in enumerate(titles[:10])])


def normalize(text: Any) -> str:
    if not text:
        return ""
    return re.sub(r"[\s\W_]+", "", str(text).lower().strip(), flags=re.UNICODE)


def clean_text(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text).strip())


def normalize_month(text: Any) -> str:
    t = normalize(text)
    mapping = {"jan":"january","january":"january","জানুয়ারি":"january","জানুয়ারি":"january","feb":"february","february":"february","ফেব্রুয়ারি":"february","ফেব্রুয়ারি":"february","mar":"march","march":"march","মার্চ":"march","apr":"april","april":"april","এপ্রিল":"april","may":"may","মে":"may","jun":"june","june":"june","জুন":"june","jul":"july","july":"july","জুলাই":"july","aug":"august","august":"august","আগস্ট":"august","sep":"september","september":"september","সেপ্টেম্বর":"september","oct":"october","october":"october","অক্টোবর":"october","nov":"november","november":"november","নভেম্বর":"november","dec":"december","december":"december","ডিসেম্বর":"december"}
    return mapping.get(t, t)


def split_list_text(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [clean_text(x) for x in raw if clean_text(x)]
    raw = str(raw).replace("•", "\n")
    parts = re.split(r"\n|;|\|", raw)
    return [clean_text(x) for x in parts if clean_text(x)]


def get_sheets():
    global _cached_book_sheet, _cached_user_sheet, _cached_log_sheet, _cached_monthly_overview_sheet, _cached_monthly_item_sheet
    if _cached_book_sheet and _cached_user_sheet and _cached_log_sheet and _cached_monthly_overview_sheet and _cached_monthly_item_sheet:
        return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet, _cached_monthly_overview_sheet, _cached_monthly_item_sheet
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open(SHEET_NAME)
    _cached_book_sheet = spreadsheet.worksheet(BOOK_SHEET_NAME)
    _cached_user_sheet = spreadsheet.worksheet(USER_SHEET_NAME)
    try:
        _cached_log_sheet = spreadsheet.worksheet(LOG_SHEET_NAME)
    except Exception:
        _cached_log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows=5000, cols=10)
        _cached_log_sheet.append_row(["timestamp","event","user_id","username","chat_id","query","normalized_query","status","matched_title","note"])
    try:
        _cached_monthly_overview_sheet = spreadsheet.worksheet(MONTHLY_OVERVIEW_SHEET_NAME)
    except Exception:
        _cached_monthly_overview_sheet = spreadsheet.add_worksheet(title=MONTHLY_OVERVIEW_SHEET_NAME, rows=1000, cols=20)
        _cached_monthly_overview_sheet.append_row(["category","year","month","monthly_topic","quran","hadith","literature","class_topic","discussion_topic","memorize","dua"])
    try:
        _cached_monthly_item_sheet = spreadsheet.worksheet(MONTHLY_ITEM_SHEET_NAME)
    except Exception:
        _cached_monthly_item_sheet = spreadsheet.add_worksheet(title=MONTHLY_ITEM_SHEET_NAME, rows=5000, cols=20)
        _cached_monthly_item_sheet.append_row(["monthly_item_id","category","year","month","section","item_name","book_id","book_title_hint"])
    return _cached_book_sheet, _cached_user_sheet, _cached_log_sheet, _cached_monthly_overview_sheet, _cached_monthly_item_sheet


async def load_users_initial():
    _, user_sheet, _, _, _ = get_sheets()
    values = await asyncio.to_thread(user_sheet.col_values, 1)
    for v in values[1:]:
        if str(v).strip().isdigit():
            USER_IDS.add(int(str(v).strip()))


async def ensure_user_saved(user_id: int):
    if user_id not in USER_IDS:
        USER_IDS.add(user_id)
        NEW_USER_QUEUE.add(user_id)


async def append_log(event, user_id, username, chat_id, query, normalized_query, status, matched_title="", note=""):
    LOG_QUEUE.append([time.strftime("%Y-%m-%d %H:%M:%S"), str(event), str(user_id), str(username), str(chat_id), str(query), str(normalized_query), str(status), str(matched_title), str(note)])


async def background_sync_task():
    while True:
        await asyncio.sleep(10)
        _, user_sheet, log_sheet, _, _ = get_sheets()
        if LOG_QUEUE:
            batch = LOG_QUEUE[:100]
            del LOG_QUEUE[:100]
            try:
                await asyncio.to_thread(log_sheet.append_rows, batch, value_input_option="USER_ENTERED")
            except Exception:
                LOG_QUEUE[:0] = batch
        if NEW_USER_QUEUE:
            ids = list(NEW_USER_QUEUE)[:100]
            rows = [[str(x)] for x in ids]
            for x in ids:
                NEW_USER_QUEUE.discard(x)
            try:
                await asyncio.to_thread(user_sheet.append_rows, rows, value_input_option="RAW")
            except Exception:
                for x in ids:
                    NEW_USER_QUEUE.add(x)


def build_book_cache(rows: list[list[str]]):
    lookup: dict[str, list[list[str]]] = {}
    indexed = []
    by_id = {}
    for row in rows:
        if len(row) < 2:
            continue
        title = str(row[0]).strip(); file_id = str(row[1]).strip()
        if not title or not file_id:
            continue
        title_norm = normalize(title)
        indexed.append((row, title_norm))
        book_id = str(row[3]).strip() if len(row) >= 4 else ""
        if book_id:
            by_id[book_id] = row
        aliases = {title_norm}
        if len(row) >= 3:
            for part in re.split(r"[,;\n|/]+", str(row[2])):
                part = part.strip()
                if part:
                    aliases.add(normalize(part))
        for alias in aliases:
            if alias:
                lookup.setdefault(alias, []).append(row)
    BOOK_CACHE.update({"rows": rows, "lookup": lookup, "indexed": indexed, "by_id": by_id, "ts": time.time()})


async def get_book_cache(force: bool = False):
    if (not force) and BOOK_CACHE["rows"] and time.time() - BOOK_CACHE["ts"] < 300:
        return BOOK_CACHE["rows"], BOOK_CACHE["lookup"], BOOK_CACHE["indexed"], BOOK_CACHE["by_id"]
    book_sheet, _, _, _, _ = get_sheets()
    values = await asyncio.to_thread(book_sheet.get_all_values)
    rows = values[1:] if len(values) > 1 else []
    build_book_cache(rows)
    return BOOK_CACHE["rows"], BOOK_CACHE["lookup"], BOOK_CACHE["indexed"], BOOK_CACHE["by_id"]


def fetch_monthly_json_source() -> dict[str, Any]:
    if MONTHLY_DATA_FILE and os.path.exists(MONTHLY_DATA_FILE):
        with open(MONTHLY_DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    if MONTHLY_DATA_URL:
        req = Request(MONTHLY_DATA_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as res:
            return json.loads(res.read().decode("utf-8"))
    raise RuntimeError("MONTHLY_DATA_URL বা MONTHLY_DATA_FILE set করা হয়নি")


def make_monthly_overview_rows(data_obj: dict[str, Any]) -> list[list[str]]:
    rows = []
    for category, years in data_obj.get("data", {}).items():
        for year, months in years.items():
            for month, info in months.items():
                rows.append([str(category).strip(), str(year).strip(), normalize_month(month), clean_text(info.get("monthly_topic", "")), "; ".join(split_list_text(info.get("quran", []))), "; ".join(split_list_text(info.get("hadith", []))), "; ".join(split_list_text(info.get("literature", []))), "; ".join(split_list_text(info.get("class_topic", []))), "; ".join(split_list_text(info.get("discussion_topic", []))), "; ".join(split_list_text(info.get("memorize", []))), "; ".join(split_list_text(info.get("dua", [])))])
    rows.sort(key=lambda x: (x[0], x[1], MONTH_ORDER.index(x[2]) if x[2] in MONTH_ORDER else 99))
    return rows


def make_monthly_item_rows(data_obj: dict[str, Any]) -> list[list[str]]:
    rows = []
    for category, years in data_obj.get("data", {}).items():
        for year, months in years.items():
            for month, info in months.items():
                for item in info.get("items", []):
                    rows.append([clean_text(item.get("monthly_item_id", "")), str(category).strip(), str(year).strip(), normalize_month(month), clean_text(item.get("section", "")), clean_text(item.get("item_name", "")), clean_text(item.get("book_id", "")), clean_text(item.get("book_title_hint", ""))])
    rows.sort(key=lambda x: (x[1], x[2], MONTH_ORDER.index(x[3]) if x[3] in MONTH_ORDER else 99, x[0]))
    return rows


async def sync_monthly_json_to_sheets() -> tuple[bool, str]:
    _, _, _, overview_sheet, item_sheet = get_sheets()
    data_obj = await asyncio.to_thread(fetch_monthly_json_source)
    overview_values = [["category","year","month","monthly_topic","quran","hadith","literature","class_topic","discussion_topic","memorize","dua"]] + make_monthly_overview_rows(data_obj)
    item_values = [["monthly_item_id","category","year","month","section","item_name","book_id","book_title_hint"]] + make_monthly_item_rows(data_obj)
    await asyncio.to_thread(overview_sheet.clear)
    await asyncio.to_thread(item_sheet.clear)
    await asyncio.to_thread(overview_sheet.update, overview_values, value_input_option="USER_ENTERED")
    await asyncio.to_thread(item_sheet.update, item_values, value_input_option="USER_ENTERED")
    MONTHLY_CACHE["ts"] = 0.0
    await get_monthly_cache(force=True)
    return True, f"Synced overview={len(overview_values)-1}, items={len(item_values)-1}"


async def get_monthly_cache(force: bool = False):
    if (not force) and MONTHLY_CACHE["overview"] and time.time() - MONTHLY_CACHE["ts"] < 300:
        return MONTHLY_CACHE["overview"], MONTHLY_CACHE["items_by_month"], MONTHLY_CACHE["items_by_id"]
    _, _, _, overview_sheet, item_sheet = get_sheets()
    overview_values = await asyncio.to_thread(overview_sheet.get_all_values)
    item_values = await asyncio.to_thread(item_sheet.get_all_values)
    overview_rows = overview_values[1:] if len(overview_values) > 1 else []
    item_rows = item_values[1:] if len(item_values) > 1 else []
    overview_lookup = {}
    items_by_month: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    items_by_id: dict[str, dict[str, str]] = {}
    for row in overview_rows:
        row = row + [""] * (11 - len(row))
        cat, year, month = normalize(row[0]), str(row[1]).strip(), normalize_month(row[2])
        if cat and year and month:
            overview_lookup[(cat, year, month)] = {"category":cat, "year":year, "month":month, "monthly_topic":row[3].strip(), "quran":row[4].strip(), "hadith":row[5].strip(), "literature":row[6].strip(), "class_topic":row[7].strip(), "discussion_topic":row[8].strip(), "memorize":row[9].strip(), "dua":row[10].strip()}
    for row in item_rows:
        row = row + [""] * (8 - len(row))
        if len(row) < 6:
            continue
        item_id, cat, year, month, section, item_name, book_id, hint = str(row[0]).strip(), normalize(row[1]), str(row[2]).strip(), normalize_month(row[3]), normalize(row[4]), row[5].strip(), row[6].strip(), row[7].strip()
        if item_id and cat and year and month and section and item_name:
            item = {"monthly_item_id":item_id, "category":cat, "year":year, "month":month, "section":section, "item_name":item_name, "book_id":book_id, "book_title_hint":hint}
            items_by_month[(cat, year, month)].append(item)
            items_by_id[item_id] = item
    for key in list(items_by_month.keys()):
        items_by_month[key] = sorted(items_by_month[key], key=lambda x: (SECTION_ORDER.index(x["section"]) if x["section"] in SECTION_ORDER else 99, x["item_name"]))
    MONTHLY_CACHE.update({"overview":overview_lookup, "items_by_month":dict(items_by_month), "items_by_id":items_by_id, "ts":time.time()})
    return MONTHLY_CACHE["overview"], MONTHLY_CACHE["items_by_month"], MONTHLY_CACHE["items_by_id"]


def format_plain_bullets(raw: Any) -> str:
    items = split_list_text(raw)
    return "\n".join(f"• {escape(x)}" for x in items) if items else "• নেই"


def format_linked_items(items: list[dict[str, str]]) -> str:
    if not items:
        return "• নেই"
    lines = []
    for item in items:
        title = escape(item.get("item_name", ""))
        link = build_deep_link(item.get("monthly_item_id", ""))
        lines.append(f'• <a href="{link}">{title}</a>' if link else f"• {title}")
    return "\n".join(lines)


async def build_monthly_message_html(category: str, year: str, month: str) -> tuple[str, list[dict[str, str]]]:
    overview_lookup, items_by_month, _items_by_id = await get_monthly_cache()
    key = (normalize(category), str(year).strip(), normalize_month(month))
    overview = overview_lookup.get(key); items = items_by_month.get(key, [])
    if not overview:
        return "", []
    by_section: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in items:
        by_section[item.get("section", "")].append(item)
    bot_footer = f'🤖 <a href="https://t.me/{BOT_USERNAME}">{escape(BOT_USERNAME)}</a>' if BOT_USERNAME else ""
    lines = [
        f"<b>🌙 {escape(CATEGORY_BN.get(normalize(category), category))}দের মাসিক পড়াশোনা | {escape(str(year))}</b>",
        f"<b>📅 মাস:</b> {escape(MONTH_BN.get(normalize_month(month), month))}",
        "", "━━━━━━━━━━━━━━━━━━", "", "<b>🟢 মাসিক বিষয়</b>", escape(overview.get("monthly_topic", "") or "নেই"),
        "", "━━━━━━━━━━━━━━━━━━", "", "<b>📖 মাসিক অধ্যয়ন</b>", "",
        "<b>🕋 কুরআন:</b>", format_plain_bullets(overview.get("quran", "")), "",
        "<b>📚 হাদীস:</b>", format_linked_items(by_section.get("hadith", [])) if by_section.get("hadith") else format_plain_bullets(overview.get("hadith", "")), "",
        "<b>📘 সাহিত্য:</b>", format_linked_items(by_section.get("literature", [])) if by_section.get("literature") else format_plain_bullets(overview.get("literature", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🎓 কুরআন/হাদীস ক্লাসের পড়া</b>", format_linked_items(by_section.get("class_topic", [])) if by_section.get("class_topic") else format_plain_bullets(overview.get("class_topic", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>👥 পাঠচক্র / আলোচনা চক্রের পড়া</b>", format_linked_items(by_section.get("discussion_topic", [])) if by_section.get("discussion_topic") else format_plain_bullets(overview.get("discussion_topic", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🧠 আয়াত-হাদীস মুখস্থ</b>", format_plain_bullets(overview.get("memorize", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🤲 মাসনূন দোয়া</b>", format_plain_bullets(overview.get("dua", "")), "", "━━━━━━━━━━━━━━━━━━", "", "📥 যদি PDF চাও, তাহলে বইগুলোর নামের উপর ক্লিক করো বা নিচের বাটনে ক্লিক করো।"
    ]
    if bot_footer:
        lines.extend(["", bot_footer])
    return "\n".join(lines), items


async def resolve_book_row_from_monthly_item(item: dict[str, str]):
    _rows, lookup, indexed, by_id = await get_book_cache()
    if item.get("book_id") and item["book_id"] in by_id:
        return by_id[item["book_id"]]
    for cand in [item.get("item_name", ""), item.get("book_title_hint", "")]:
        c = normalize(cand)
        if not c:
            continue
        if c in lookup and lookup[c]:
            return lookup[c][0]
        for row, title_norm in indexed:
            if c in title_norm or title_norm in c:
                return row
    return None

# ================= COMMANDS =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user:
        await ensure_user_saved(user.id)
    payload = context.args[0].strip() if context.args else ""
    if payload.startswith("mi_"):
        item_id = payload[3:]
        _overview, _items_by_month, items_by_id = await get_monthly_cache()
        item = items_by_id.get(item_id)
        if item:
            await send_monthly_message(update.message, item["category"], item["year"], item["month"], item_id)
            return
    await update.message.reply_text(
       "আসসালামু আলাইকুম। অনলাইন লাইব্রেরিতে স্বাগতম।\n\n"
        "আপনার প্রয়োজনীয় বইয়ের নাম লিখুন।\n"
        "মাসিক পড়াশোনা দেখতে নিচের বাটনে ক্লিক করুন।\n"
        "এডমিনের সাথে কথা বলতে /admin + আপনার টেক্সট লিখুন",
        reply_markup=main_reply_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("বট ব্যবহারের নিয়মাবলী:\n\n"
        "১. বই খোঁজা: সরাসরি বইয়ের নাম লিখে মেসেজ দিন।\n"
        "২. মাসিক পড়াশোনা: /monthly লিখুন বা Start-এর বাটন চাপুন।\n"
        "৩. এডমিন: /admin লিখে আপনার কথা লিখুন।\n"
        "   যেমন: /admin ভাই আমার অমুক বই প্রয়োজন", 
        reply_markup=main_reply_keyboard())


async def monthly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📚 মাসিক পড়াশোনা দেখতে ক্যাটাগরি নির্বাচন করুন:", reply_markup=main_reply_keyboard())
    await update.message.reply_text("ক্যাটাগরি নির্বাচন করুন:", reply_markup=monthly_category_keyboard())


async def syncplan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    msg = await update.message.reply_text("⏳ GitHub/JSON plan sync চলছে...")
    ok, note = await sync_monthly_json_to_sheets()
    await msg.edit_text(("✅ " if ok else "⚠️ ") + note)


async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    book_sheet, _, _, _, _ = get_sheets()
    target = update.message.reply_to_message
    if not target or not target.document:
        await update.message.reply_text("ডকুমেন্টে reply দিয়ে /upload বা /upload বইয়ের নাম ব্যবহার করো")
        return
    parts = (update.message.text or "").split(maxsplit=1)
    book_name = parts[1].strip() if len(parts) > 1 else (target.document.file_name or "অজানা বই")
    aliases = target.caption or ""
    await asyncio.to_thread(book_sheet.append_row, [book_name, target.document.file_id, aliases, ""])
    BOOK_CACHE["ts"] = 0.0
    await update.message.reply_text(f"✅ বই আপলোড হয়েছে: {book_name}")


async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await get_book_cache(force=True)
    await get_monthly_cache(force=True)
    await update.message.reply_text("✅ cache refresh হয়েছে")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    overview, _items_by_month, items_by_id = await get_monthly_cache()
    await update.message.reply_text(f"ইউজার: {len(USER_IDS)}\nবই: {len(BOOK_CACHE['rows'])}\nমাসিক সেকশন: {len(overview)}\nমাসিক আইটেম: {len(items_by_id)}")


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    body = parts[1].strip() if len(parts) > 1 else ""
    if not body:
        await update.message.reply_text("ব্যবহার: /admin আপনার মেসেজ")
        return
    user = update.effective_user
    chat_id = update.effective_chat.id
    username = f"@{user.username}" if user and user.username else "No username"
    sent = await context.bot.send_message(chat_id=ADMIN_ID, text=f"📩 নতুন ইউজার মেসেজ\n\nনাম: {user.full_name if user else 'Unknown'}\nইউজারনেম: {username}\nচ্যাট আইডি: {chat_id}\n\nমেসেজ:\n{body}")
    _admin_reply_map[sent.message_id] = chat_id
    await update.message.reply_text("✅ মেসেজ এডমিনের কাছে পাঠানো হয়েছে")

# ================= ERROR HANDLER =================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Global error handler to log unexpected exceptions and inform users gracefully.

    If an exception occurs during update handling and no other error handlers are
    registered, this callback will capture the error.  It logs the full
    traceback for debugging and, when possible, sends a user-friendly message
    indicating that an unexpected error occurred.

    Args:
        update: The incoming update that caused the exception.
        context: The context for the callback, which includes the exception.
    """
    logger.exception("Exception while handling update:", exc_info=context.error)
    # Notify the user if we have a message target
    try:
        if isinstance(update, Update) and update.message:
            await update.message.reply_text("⚠️ অনাকাঙ্খিত ত্রুটি ঘটেছে, পরে আবার চেষ্টা করুন।")
    except Exception:
        # If notifying the user fails (e.g., because chat is invalid), ignore
        pass

# ================= BOOK SEARCH =================
async def send_document_with_retry(bot, chat_id: int, file_id: str, caption: str):
    for attempt in range(4):
        try:
            await bot.send_document(chat_id=chat_id, document=file_id, caption=caption)
            return True
        except RetryAfter as e:
            await asyncio.sleep(float(getattr(e, "retry_after", 2)) + 0.5)
        except (TimedOut, NetworkError):
            await asyncio.sleep(1 + attempt)
        except (Forbidden, BadRequest):
            return False
        except Exception:
            await asyncio.sleep(1)
    return False


async def process_book_search(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_text_raw: str, user_id: int, username: str = ""):
    await ensure_user_saved(user_id)
    rows, lookup, indexed, _by_id = await get_book_cache()
    if not rows:
        await context.bot.send_message(chat_id=chat_id, text="❌ ডাটাবেসে কোনো বই নেই")
        return
    user_norm = normalize(user_text_raw)
    if not user_norm:
        return
    matched = lookup.get(user_norm, []) or [row for row, title_norm in indexed if user_norm in title_norm or title_norm in user_norm]
    if matched:
        uniq = []
        seen = set()
        for row in matched:
            key = (str(row[0]).strip(), str(row[1]).strip())
            if key not in seen:
                seen.add(key)
                uniq.append(row)
        matched = uniq
        if len(matched) <= 5:
            await context.bot.send_message(chat_id=chat_id, text="📚 বই পাওয়া গেছে, পাঠানো শুরু করছি...")
            for row in matched:
                if len(row) >= 2:
                    await send_document_with_retry(context.bot, chat_id, str(row[1]).strip(), f"📘 {str(row[0]).strip()}")
            return
        suggestions = [str(row[0]).strip() for row in matched[:10]]
        await context.bot.send_message(chat_id=chat_id, text="📚 অনেকগুলো বই পাওয়া গেছে। নিচ থেকে বেছে নিন:", reply_markup=make_inline_keyboard(suggestions))
        return
    close_keys = get_close_matches(user_norm, list(lookup.keys()), n=5, cutoff=0.72)
    if close_keys:
        suggestions = []
        for key in close_keys:
            for row in lookup.get(key, []):
                title = str(row[0]).strip()
                if title and title not in suggestions:
                    suggestions.append(title)
        if suggestions:
            await context.bot.send_message(chat_id=chat_id, text="🤖 সম্ভাব্য বইগুলো:", reply_markup=make_inline_keyboard(suggestions[:10]))
            return
    await context.bot.send_message(chat_id=chat_id, text="❌ বইটি খুঁজে পাওয়া যাচ্ছে না")

# ================= MONTHLY SEND =================
async def send_monthly_message(message_target, category: str, year: str, month: str, target_item_id: str = ""):
    overview_lookup, items_by_month, items_by_id = await get_monthly_cache()
    key = (normalize(category), str(year).strip(), normalize_month(month))
    overview = overview_lookup.get(key)
    items = items_by_month.get(key, [])
    if not overview:
        await message_target.reply_text("⚠️ এই মাসের তথ্য যোগ করা হয়নি")
        return
    by_section: dict[str, list[dict[str, str]]] = defaultdict(list)
    for item in items:
        by_section[item["section"]].append(item)
    bot_footer = f'🤖 <a href="https://t.me/{BOT_USERNAME}">{escape(BOT_USERNAME)}</a>' if BOT_USERNAME else ""
    html = "\n".join([
        f"<b>🌙 {escape(CATEGORY_BN.get(normalize(category), category))}দের মাসিক পড়াশোনা | {escape(str(year))}</b>",
        f"<b>📅 মাস:</b> {escape(MONTH_BN.get(normalize_month(month), month))}",
        "", "━━━━━━━━━━━━━━━━━━", "", "<b>🟢 মাসিক বিষয়</b>", escape(overview.get("monthly_topic", "") or "নেই"),
        "", "━━━━━━━━━━━━━━━━━━", "", "<b>📖 মাসিক অধ্যয়ন</b>", "",
        "<b>🕋 কুরআন:</b>", format_plain_bullets(overview.get("quran", "")), "",
        "<b>📚 হাদীস:</b>", format_linked_items(by_section.get("hadith", [])) if by_section.get("hadith") else format_plain_bullets(overview.get("hadith", "")), "",
        "<b>📘 সাহিত্য:</b>", format_linked_items(by_section.get("literature", [])) if by_section.get("literature") else format_plain_bullets(overview.get("literature", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🎓 কুরআন/হাদীস ক্লাসের পড়া</b>", format_linked_items(by_section.get("class_topic", [])) if by_section.get("class_topic") else format_plain_bullets(overview.get("class_topic", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>👥 পাঠচক্র / আলোচনা চক্রের পড়া</b>", format_linked_items(by_section.get("discussion_topic", [])) if by_section.get("discussion_topic") else format_plain_bullets(overview.get("discussion_topic", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🧠 আয়াত-হাদীস মুখস্থ</b>", format_plain_bullets(overview.get("memorize", "")), "",
        "━━━━━━━━━━━━━━━━━━", "", "<b>🤲 মাসনূন দোয়া</b>", format_plain_bullets(overview.get("dua", "")), "", "━━━━━━━━━━━━━━━━━━", "", "📥 যদি PDF চাও, তাহলে বইগুলোর নামের উপর ক্লিক করো বা নিচের বাটনে ক্লিক করো।",
        bot_footer,
    ])
    if target_item_id and target_item_id in items_by_id:
        html += f"\n\n📌 আপনি <b>{escape(items_by_id[target_item_id]['item_name'])}</b> বইটির জন্য এসেছেন।"
    await message_target.reply_text(html, parse_mode="HTML", disable_web_page_preview=True, reply_markup=monthly_pdf_keyboard(category, year, month, items))

# ================= CALLBACKS =================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if data.startswith("pick|"):
        _, token, idx = data.split("|", 2)
        titles = _callback_cache.get(token, [])
        try:
            selected_title = titles[int(idx)]
        except Exception:
            await query.message.reply_text("⚠️ সেশন শেষ, আবার সার্চ করুন।")
            return
        await process_book_search(context, query.message.chat.id, selected_title, query.from_user.id, f"@{query.from_user.username}" if query.from_user.username else "")
        return
    if data == "ms|home":
        await query.message.reply_text("ক্যাটাগরি নির্বাচন করুন:", reply_markup=monthly_category_keyboard())
        return
    if data == "ms|back|root":
        await query.message.reply_text("মূল মেনু", reply_markup=main_reply_keyboard())
        return
    if data.startswith("ms|cat|"):
        _, _, category = data.split("|", 2)
        await query.message.reply_text(f"📅 {CATEGORY_BN.get(category, category)}দের {DEFAULT_YEAR} সালের মাস নির্বাচন করুন:", reply_markup=monthly_month_keyboard(category, DEFAULT_YEAR))
        return
    if data.startswith("ms|month|"):
        _, _, category, year, month = data.split("|", 4)
        await send_monthly_message(query.message, category, year, month)
        return
    if data.startswith("ms|pdf|"):
        _, _, item_id = data.split("|", 2)
        _overview, _items_by_month, items_by_id = await get_monthly_cache()
        item = items_by_id.get(item_id)
        if not item:
            await query.message.reply_text("⚠️ এই বইটির তথ্য পাওয়া যায়নি")
            return
        row = await resolve_book_row_from_monthly_item(item)
        if not row or len(row) < 2:
            await query.message.reply_text("⚠️ এই বইটির PDF এখনও যুক্ত করা হয়নি")
            return
        await send_document_with_retry(context.bot, query.message.chat.id, str(row[1]).strip(), f"📘 {str(row[0]).strip()}")
        return

# ================= MESSAGE HANDLER =================
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    user = update.effective_user
    text = update.message.text.strip()
    if not user or not text:
        return
    if user.id == ADMIN_ID and update.message.reply_to_message:
        target_chat_id = _admin_reply_map.get(update.message.reply_to_message.message_id)
        if target_chat_id:
            await context.bot.send_message(chat_id=target_chat_id, text=f"👨‍💼 এডমিন: {text}")
            await update.message.reply_text("✅ রিপ্লাই পাঠানো হয়েছে")
            return
    if text == "📚 মাসিক পড়াশোনা":
        await update.message.reply_text("ক্যাটাগরি নির্বাচন করুন:", reply_markup=monthly_category_keyboard())
        return
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await process_book_search(context, update.effective_chat.id, text, user.id, f"@{user.username}" if user.username else "")

# ================= STARTUP =================
async def startup_task(app: Application):
    await load_users_initial()
    await get_book_cache(force=True)
    if AUTO_SYNC_PLAN_ON_START:
        try:
            ok, note = await sync_monthly_json_to_sheets()
            logger.info("startup sync: %s %s", ok, note)
        except Exception as e:
            logger.exception("startup plan sync failed: %s", e)
    else:
        await get_monthly_cache(force=True)
    app.create_task(background_sync_task())

# ================= APP =================
def build_application() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(256).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("monthly", monthly_command))
    app.add_handler(CommandHandler("syncplan", syncplan_command))
    app.add_handler(CommandHandler("upload", upload_command))
    app.add_handler(CommandHandler("refresh", refresh_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    # Register the global error handler.  Without this, unhandled exceptions
    # could crash the bot and leave the service in an unstable state.
    app.add_error_handler(error_handler)
    app.post_init = startup_task
    return app


def main():
    Thread(target=run_web, daemon=True).start()
    app = build_application()
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
