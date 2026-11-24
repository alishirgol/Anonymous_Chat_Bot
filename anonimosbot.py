import asyncio
import json
import os
from collections import deque, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
from rubka.asynco import Robot
from rubka.context import Message
from rubka.keypad import ChatKeypadBuilder, InlineBuilder
import aiohttp
import base64
import random
# ============================================================
#  تنظیمات ربات (فقط این بخش را برای دوستان ویرایش کنید)
# ============================================================

# توکن ربات روبیکا (از بات‌فادر روبیکا گرفته می‌شود). در فایل اصلی «+++++++» بود؛ توکن واقعی را اینجا بگذارید.
TOKEN = "+++++++"

# لیست GUID ادمین‌های ربات.
# این افراد به پنل مدیریت، پیام همگانی، بن/آنبن، هدیه‌ی اشتراک و گزارش‌های کاربران دسترسی دارند.
# ⚠️ برای دوستان حتماً با GUID خودشان جایگزین کنید.
chat_owner_ids = ["b0FvdsD0MKg04dd00b83ae5f1ac05d75"]

# آیدی بدون @ کانال ربات؛ سه متغیر پایین از روی همین ساخته می‌شوند.
CHANNEL_ID = "Anonymous_r_bot1"

# آیدی کانال با @ — در پیام‌های تبلیغاتی و دکمه‌ی «📢 کانال» نمایش داده می‌شود.
CHANNEL_USERNAME = "@" + CHANNEL_ID

# لینک کانال (بدون https) که در پیام خوش‌آمد، ثبت‌نام و تبلیغات می‌آید.
CHANNEL_LINK = "rubika.ir/" + CHANNEL_ID

# لینک کامل کانال (با https) که داخل متن دعوت به کانال فرستاده می‌شود.
CHANNEL_URL = "https://" + CHANNEL_LINK
user_states = defaultdict(lambda: {
    "state": None,
    "temp_data": {},
    "last_action": None,
    "awaiting": None
})
DB_FILE = "users_db.json"
LOG_FILE = "chat_logs.json"
PROFILE_PICS_DIR = Path("profile_pics")
PROFILE_PICS_DIR.mkdir(exist_ok=True)
AUTOSAVE_INTERVAL = 30
BAN_DURATION = timedelta(hours=1)
REPORT_THRESHOLD = 3
ONLINE_THRESHOLD = timedelta(minutes=5)
SAME_AGE_DELTA = 3
DAILY_CONNECT_LIMIT = 5
PREMIUM_DAILY_LIMIT = 50
text_link = f"""🌟 به کانال چت ناشناس لوپ بپیوندید! 🌟  
برای دسترسی به گفتگوهای جذاب و اطلاعات بیشتر، از لینک زیر استفاده کنید:  
🖇️ کانال چت ناشناس لوپ

{CHANNEL_URL}"""
bot = Robot(TOKEN)
BOT_NAME = "لوپ"
STATE_NONE = "none"
STATE_REPORT = "report"
STATE_BROADCAST = "broadcast"
STATE_BAN_USER = "ban_user"
STATE_UNBAN_USER = "unban_user"
STATE_PREMIUM_GIFT = "premium_gift"
STATE_ADMIN_REPLY = "admin_reply"
STATE_EDIT_PROFILE = "edit_profile"
STATE_SETTING_NAME = "setting_name"
STATE_SETTING_AGE = "setting_age"
STATE_SETTING_GENDER = "setting_gender"
STATE_SETTING_PROVINCE = "setting_province"
STATE_SETTING_CITY = "setting_city"
STATE_SETTING_HEIGHT = "setting_height"
STATE_SETTING_BIO = "setting_bio"
STATE_SETTING_PHOTO = "setting_photo"
def default_user() -> Dict[str, Any]:
    return {
        "registered": False,
        "name": None,
        "age": None,
        "gender": None,
        "province": None,
        "city": None,
        "height": None,
        "bio": None,
        "status": "idle",
        "wait_type": None,
        "active_chat": None,
        "reported_by": set(),
        "report_count": 0,
        "is_banned": False,
        "ban_time": None,
        "admin_state": STATE_NONE,
        "profile_state": STATE_NONE,
        "last_seen": None,
        "profile_photo_path": None,
        "daily_connects": 0,
        "last_connect_reset": None,
        "is_premium": False,
        "premium_expiry": None,
        "total_chats": 0,
        "total_reports_sent": 0,
        "total_reports_received": 0,
    }
USERS: defaultdict = defaultdict(default_user)
WAIT_QUEUES = {
    "same_age": deque(),
    "same_city": deque(),
    "same_province": deque(),
    "same_gender": deque(),
    "random": deque(),
}
queues_lock = asyncio.Lock()
queue_stats = defaultdict(int)
def _serialize_user(u: Dict[str, Any]) -> Dict[str, Any]:
    copy = dict(u)
    rb = copy.get("reported_by")
    if isinstance(rb, set):
        copy["reported_by"] = list(rb)
    bt = copy.get("ban_time")
    if isinstance(bt, datetime):
        copy["ban_time"] = bt.isoformat()
    ls = copy.get("last_seen")
    if isinstance(ls, datetime):
        copy["last_seen"] = ls.isoformat()
    pe = copy.get("premium_expiry")
    if isinstance(pe, datetime):
        copy["premium_expiry"] = pe.isoformat()
    lcr = copy.get("last_connect_reset")
    if isinstance(lcr, datetime):
        copy["last_connect_reset"] = lcr.isoformat()
    return copy
def save_db() -> None:
    out = {}
    for uid, u in USERS.items():
        out[uid] = _serialize_user(u)
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("💾 خطا در ذخیره دیتابیس:", e)
def load_db() -> None:
    if not os.path.exists(DB_FILE):
        return
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError:
        print("🗃️ دیتابیس خالی یا خراب")
        return
    except Exception as e:
        print("💾 خطا در بارگذاری دیتابیس:", e)
        return
    for uid, data in raw.items():
        base = default_user()
        rb = data.get("reported_by", [])
        base["reported_by"] = set(rb) if isinstance(rb, list) else set()
        bt = data.get("ban_time")
        if isinstance(bt, str):
            try:
                base["ban_time"] = datetime.fromisoformat(bt)
            except Exception:
                base["ban_time"] = None
        else:
            base["ban_time"] = data.get("ban_time")
        ls = data.get("last_seen")
        if isinstance(ls, str):
            try:
                base["last_seen"] = datetime.fromisoformat(ls)
            except Exception:
                base["last_seen"] = None
        else:
            base["last_seen"] = data.get("last_seen")
        pe = data.get("premium_expiry")
        if isinstance(pe, str):
            try:
                base["premium_expiry"] = datetime.fromisoformat(pe)
            except Exception:
                base["premium_expiry"] = None
        lcr = data.get("last_connect_reset")
        if isinstance(lcr, str):
            try:
                base["last_connect_reset"] = datetime.fromisoformat(lcr)
            except Exception:
                base["last_connect_reset"] = None
        for k, v in data.items():
            if k in ("reported_by", "ban_time", "last_seen", "premium_expiry", "last_connect_reset"):
                continue
            base[k] = v
        USERS[uid] = base
def log_event(log_type: str, sender: str, receiver: Optional[str] = None, content: str = "") -> None:
    entry = {
        "timestamp": datetime.now().isoformat(),
        "type": log_type,
        "from": sender,
        "to": receiver,
        "content": content,
    }
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("📝 خطا در ثبت لاگ:", e)
def update_last_seen(uid: str) -> None:
    USERS[uid]["last_seen"] = datetime.now()
def online_count() -> int:
    now = datetime.now()
    c = 0
    for u in USERS.values():
        ls = u.get("last_seen")
        if isinstance(ls, datetime):
            if now - ls < ONLINE_THRESHOLD:
                c += 1
    return c
def reset_daily_connect(uid: str) -> None:
    u = USERS[uid]
    today = datetime.now().date()
    last_reset = u.get("last_connect_reset")
    if not last_reset or (isinstance(last_reset, datetime) and last_reset.date() < today):
        u["daily_connects"] = 0
        u["last_connect_reset"] = datetime.now()
def can_connect(uid: str) -> tuple[bool, str]:
    u = USERS[uid]
    reset_daily_connect(uid)
    limit = PREMIUM_DAILY_LIMIT if u.get("is_premium") else DAILY_CONNECT_LIMIT
    if u["daily_connects"] >= limit:
        if u.get("is_premium"):
            return False, f"❌ شما امروز {limit} بار متصل شده‌اید. فردا دوباره امتحان کنید.\n\n{uid}"
        else:
            return False, f"❌ شما امروز {limit} بار متصل شده‌اید. برای اتصال بیشتر می‌توانید اشتراک {BOT_NAME} تهیه کنید.\n\n{uid}"
    return True, ""
def increment_connect(uid: str) -> None:
    USERS[uid]["daily_connects"] += 1
    USERS[uid]["total_chats"] += 1
def is_admin(uid: str) -> bool:
    return uid in chat_owner_ids
def check_premium(uid: str) -> None:
    u = USERS[uid]
    if u.get("premium_expiry") and isinstance(u["premium_expiry"], datetime):
        if datetime.now() > u["premium_expiry"]:
            u["is_premium"] = False
            u["premium_expiry"] = None
def set_user_state(uid: str, state: str, temp_data: dict = None) -> None:
    user_states[uid]["state"] = state
    user_states[uid]["last_action"] = datetime.now().isoformat()
    if temp_data:
        user_states[uid]["temp_data"] = temp_data
    else:
        user_states[uid]["temp_data"] = {}
def get_user_state(uid: str) -> dict:
    return user_states[uid]
def clear_user_state(uid: str) -> None:
    user_states[uid] = {
        "state": STATE_NONE,
        "temp_data": {},
        "last_action": None,
        "awaiting": None
    }
def is_in_state(uid: str, state: str) -> bool:
    return user_states[uid]["state"] == state
def build_inline_keypad(users_count):
    return (
        InlineBuilder()
        .row(
            InlineBuilder().button_simple(
                id="stats", text=f"🌸 {BOT_NAME} | کاربران: {users_count} | آنلاین: {online_count()}"
            )
        )
        .build()
    )
def build_waiting_stats() -> str:
    stats = "📊 **آمار صف انتظار:**\n"
    stats += f"┣ 🎯 هم‌سن: {len(WAIT_QUEUES['same_age'])} نفر\n"
    stats += f"┣ 🏙️ هم‌شهری: {len(WAIT_QUEUES['same_city'])} نفر\n"
    stats += f"┣ 🗺️ هم‌استان: {len(WAIT_QUEUES['same_province'])} نفر\n"
    stats += f"┣ ⚧️ هم‌جنسیت: {len(WAIT_QUEUES['same_gender'])} نفر\n"
    stats += f"┗ 🎲 تصادفی: {len(WAIT_QUEUES['random'])} نفر\n"
    return stats
def build_main_menu(uid: str):
    u = USERS[uid]
    builder = ChatKeypadBuilder()
    reset_daily_connect(uid)
    check_premium(uid)
    remaining = (PREMIUM_DAILY_LIMIT if u.get("is_premium") else DAILY_CONNECT_LIMIT) - u["daily_connects"]
    premium_badge = "👑 VIP" if u.get("is_premium") else "⭐ عادی"
    header_text = f"🌸 {BOT_NAME} | {premium_badge} | باقی‌مانده: {remaining}"
    builder.row(builder.button(id="header", text=header_text))
    promo_text = f"📢 {CHANNEL_USERNAME}"
    builder.row(builder.button(id="channel_promo", text=promo_text))
    if u["status"] == "chatting":
        builder.row(
            builder.button(id="end_chat", text="🔚 پایان گفتگو"),
            builder.button(id="report", text="🚨 گزارش"),
        )
        builder.row(builder.button(id="view_peer_profile", text="👁️ پروفایل مخاطب"))
    elif u["status"] == "waiting":
        builder.row(builder.button(id="cancel_search", text="❌ لغو جستجو"))
    else:
        builder.row(
            builder.button(id="same_age", text="🎯 هم‌سن"),
            builder.button(id="same_city", text="🏙️ هم‌شهری"),
        )
        builder.row(
            builder.button(id="same_province", text="🗺️ هم‌استان"),
            builder.button(id="same_gender", text="⚧️ هم‌جنسیت"),
        )
        builder.row(
            builder.button(id="random", text="🎲 تصادفی"),
            builder.button(id="stats", text="📊 آمار"),
        )
        builder.row(
            builder.button(id="my_profile", text="👤 پروفایل من"),
            builder.button(id="edit_profile", text="⚙️ ویرایش"),
        )
        builder.row(
            builder.button(id="link", text="📎 اشتراک لینک"),
            builder.button(id="sup", text="☎️ پشتیبانی"),
        )
    if is_admin(uid):
        builder.row(builder.button(id="admin_panel", text="👑 پنل مدیریت"))
    return builder.build(resize_keyboard=True)
def build_admin_panel():
    builder = ChatKeypadBuilder()
    builder.row(
        builder.button(id="admin_stats", text="📊 آمار کلی"),
        builder.button(id="admin_users", text="👥 کاربران"),
    )
    builder.row(
        builder.button(id="admin_broadcast", text="📢 پیام همگانی"),
        builder.button(id="admin_ban", text="🚫 مسدود کاربر"),
    )
    builder.row(
        builder.button(id="admin_unban", text="✅ رفع مسدود"),
        builder.button(id="admin_premium", text="👑 اعطای VIP"),
    )
    builder.row(
        builder.button(id="admin_reports", text="🚨 گزارشات"),
        builder.button(id="admin_logs", text="📋 لاگ‌ها"),
    )
    builder.row(
        builder.button(id="admin_reply", text="💬 پاسخ به کاربر"),
        builder.button(id="admin_help", text="❓ راهنما"),
    )
    builder.row(builder.button(id="back_main", text="🔙 بازگشت"))
    return builder.build(resize_keyboard=True)
def ensure_registered(uid: str) -> bool:
    return USERS[uid]["registered"] is True
async def ask_field(uid: str, field: str):
    prompts = {
        "name": f"✨ {BOT_NAME} جان! لطفاً نام زیبایتان را وارد کنید:",
        "age": f"🎂 {BOT_NAME} عزیز! سن خود را به عدد وارد کنید:",
        "gender": f"⚧️ {BOT_NAME} جان! جنسیت خود را انتخاب کنید:\n• مرد\n• زن",
        "province": f"🗺️ {BOT_NAME} عزیز! استان محل سکونت خود را وارد کنید:",
        "city": f"🏙️ {BOT_NAME} جان! شهر محل سکونت خود را وارد کنید:",
        "height": f"📏 {BOT_NAME} عزیز! قد خود را به سانتیمتر وارد کنید (اختیاری):",
        "photo": f"🖼️ {BOT_NAME} جان! عکس پروفایل خود را ارسال کنید یا 'ندارم' را تایپ کنید:",
        "bio": f"💫 {BOT_NAME} عزیز! یک معرفی کوتاه و جذاب درباره خودتان بنویسید:",
    }
    await bot.send_message(uid, prompts[field], chat_keypad=None)
async def start_registration(uid: str):
    USERS[uid]["profile_state"] = STATE_SETTING_NAME
    await ask_field(uid, "name")
def _user_available_for_matching(uid: str) -> bool:
    u = USERS[uid]
    if u["is_banned"]:
        return False
    if u["status"] != "waiting":
        return False
    return True
def try_match_for(uid: str, wait_type: str) -> Optional[str]:
    user = USERS[uid]
    if wait_type == "random":
        for other in list(WAIT_QUEUES["random"]):
            if other == uid:
                continue
            if not _user_available_for_matching(other):
                continue
            return other
        return None
    if wait_type == "same_city":
        for other in list(WAIT_QUEUES["same_city"]):
            if other == uid:
                continue
            if not _user_available_for_matching(other):
                continue
            if USERS[other].get("city") == user.get("city") and USERS[other].get("city"):
                return other
        return None
    if wait_type == "same_province":
        for other in list(WAIT_QUEUES["same_province"]):
            if other == uid:
                continue
            if not _user_available_for_matching(other):
                continue
            if USERS[other].get("province") == user.get("province") and USERS[other].get("province"):
                return other
        return None
    if wait_type == "same_age":
        try:
            age = int(user.get("age"))
        except Exception:
            return None
        low = age - SAME_AGE_DELTA
        high = age + SAME_AGE_DELTA
        for other in list(WAIT_QUEUES["same_age"]):
            if other == uid:
                continue
            if not _user_available_for_matching(other):
                continue
            try:
                oage = int(USERS[other].get("age"))
            except Exception:
                continue
            if low <= oage <= high:
                return other
        return None
    if wait_type == "same_gender":
        for other in list(WAIT_QUEUES["same_gender"]):
            if other == uid:
                continue
            if not _user_available_for_matching(other):
                continue
            if USERS[other].get("gender") == user.get("gender") and USERS[other].get("gender"):
                return other
        return None
    return None
async def connect(u1: str, u2: str):
    if not _user_available_for_matching(u1) or not _user_available_for_matching(u2):
        return
    can1, msg1 = can_connect(u1)
    can2, msg2 = can_connect(u2)
    if not can1:
        await bot.send_message(u1, msg1, chat_keypad=build_main_menu(u1))
        return
    if not can2:
        await bot.send_message(u2, msg2, chat_keypad=build_main_menu(u2))
        return
    USERS[u1]["status"] = "chatting"
    USERS[u2]["status"] = "chatting"
    USERS[u1]["active_chat"] = u2
    USERS[u2]["active_chat"] = u1
    USERS[u1]["wait_type"] = None
    USERS[u2]["wait_type"] = None
    increment_connect(u1)
    increment_connect(u2)
    async with queues_lock:
        for q in WAIT_QUEUES.values():
            if u1 in q:
                try:
                    q.remove(u1)
                except ValueError:
                    pass
            if u2 in q:
                try:
                    q.remove(u2)
                except ValueError:
                    pass
    promo_msg = f"\n\n📢 برای اخبار و آپدیت‌های {BOT_NAME} حتما جوین بدین: {CHANNEL_LINK}"
    u1_remaining = (PREMIUM_DAILY_LIMIT if USERS[u1].get("is_premium") else DAILY_CONNECT_LIMIT) - USERS[u1]["daily_connects"]
    u2_remaining = (PREMIUM_DAILY_LIMIT if USERS[u2].get("is_premium") else DAILY_CONNECT_LIMIT) - USERS[u2]["daily_connects"]
    try:
        await bot.send_message(
            u1,
            f"🎉 هم‌صحبت جدید پیدا شد!\n💬 اکنون می‌توانید گفتگو را شروع کنید\n🌸 باقی‌مانده امروز: {u1_remaining}{promo_msg}",
            chat_keypad=build_main_menu(u1),
        )
    except Exception:
        pass
    try:
        await bot.send_message(
            u2,
            f"🎉 هم‌صحبت جدید پیدا شد!\n💬 اکنون می‌توانید گفتگو را شروع کنید\n🌸 باقی‌مانده امروز: {u2_remaining}{promo_msg}",
            chat_keypad=build_main_menu(u2),
        )
    except Exception:
        pass
    log_event("connect", u1, u2, "matched")
async def enqueue_and_try(uid: str, wait_type: str):
    if USERS[uid]["status"] == "waiting":
        await bot.send_message(
            uid,
            f"⏳ {BOT_NAME} جان! شما در حال حاضر در صف انتظار هستید...",
            chat_keypad=build_main_menu(uid),
        )
        return
    can, msg = can_connect(uid)
    if not can:
        await bot.send_message(uid, msg, chat_keypad=build_main_menu(uid))
        return
    USERS[uid]["status"] = "waiting"
    USERS[uid]["wait_type"] = wait_type
    async with queues_lock:
        if uid not in WAIT_QUEUES[wait_type]:
            WAIT_QUEUES[wait_type].append(uid)
            queue_stats[wait_type] = len(WAIT_QUEUES[wait_type])
    wait_type_names = {
        "same_age": "هم‌سن",
        "same_city": "هم‌شهری",
        "same_province": "هم‌استان",
        "same_gender": "هم‌جنسیت",
        "random": "تصادفی"
    }
    await bot.send_message(
        uid,
        f"🔍 {BOT_NAME} در حال جستجوی هم‌صحبت {wait_type_names.get(wait_type, wait_type)}...\n\n{build_waiting_stats()}",
        chat_keypad=build_main_menu(uid),
    )
    other = try_match_for(uid, wait_type)
    if other:
        await connect(uid, other)
async def end_chat(uid: str, reason_peer: str = f"💔 {BOT_NAME} جان! هم‌صحبت گفتگو را ترک کرد"):
    peer = USERS[uid].get("active_chat")
    if peer and peer in USERS:
        USERS[peer]["active_chat"] = None
        USERS[peer]["status"] = "idle"
        try:
            await bot.send_message(
                peer, f"🔚 {reason_peer}", chat_keypad=build_main_menu(peer)
            )
        except Exception:
            pass
        log_event("disconnect", peer, uid, reason_peer)
    USERS[uid]["active_chat"] = None
    USERS[uid]["status"] = "idle"
    try:
        await bot.send_message(
            uid, f"🏁 گفتگو به پایان رسید. {BOT_NAME} منتظر بازگشت شماست!", chat_keypad=build_main_menu(uid)
        )
    except Exception:
        pass
async def handle_admin_broadcast(uid: str, text: str) -> None:
    count_ok = 0
    count_fail = 0
    await bot.send_message(uid, f"📢 در حال ارسال پیام همگانی به {len(USERS)} کاربر...")
    for user_id in list(USERS.keys()):
        try:
            await bot.send_message(user_id, f"📢 **پیام همگانی از {BOT_NAME}** 📢\n\n{text}\n\n━━━━━━━━━━━━━━\n🌸 با عشق، تیم {BOT_NAME}")
            count_ok += 1
            await asyncio.sleep(0.05)
        except Exception:
            count_fail += 1
    await bot.send_message(uid, f"✅ **ارسال پیام همگانی کامل شد**\n┣ 📨 موفق: {count_ok}\n┗ ❌ ناموفق: {count_fail}")
    log_event("broadcast", uid, None, f"success:{count_ok}, fail:{count_fail}")
async def handle_admin_ban(uid: str, target: str) -> None:
    target = target.strip()
    if target in USERS:
        USERS[target]["is_banned"] = True
        USERS[target]["ban_time"] = datetime.now()
        if USERS[target]["active_chat"]:
            await end_chat(target, f"🔒 حساب کاربری توسط {BOT_NAME} مسدود شد")
        save_db()
        await bot.send_message(uid, f"✅ **کاربر مسدود شد**\n┗ 🆔 {target}")
        log_event("ban", uid, target, "admin_ban")
    else:
        await bot.send_message(uid, f"❌ کاربر با آیدی `{target}` یافت نشد")
async def handle_admin_unban(uid: str, target: str) -> None:
    target = target.strip()
    if target in USERS:
        USERS[target]["is_banned"] = False
        USERS[target]["ban_time"] = None
        save_db()
        await bot.send_message(uid, f"✅ **رفع مسدود انجام شد**\n┗ 🆔 {target}")
        log_event("unban", uid, target, "admin_unban")
    else:
        await bot.send_message(uid, f"❌ کاربر با آیدی `{target}` یافت نشد")
async def handle_admin_premium(uid: str, text: str) -> None:
    parts = text.strip().split()
    if len(parts) >= 2:
        target = parts[0]
        try:
            days = int(parts[1])
            if target in USERS:
                USERS[target]["is_premium"] = True
                USERS[target]["premium_expiry"] = datetime.now() + timedelta(days=days)
                save_db()
                expiry_date = USERS[target]["premium_expiry"].strftime("%Y/%m/%d")
                await bot.send_message(
                    target,
                    f"🎉 **تبریک! اشتراک VIP {BOT_NAME}** 🎉\n\n"
                    f"🌸 شما {days} روز اشتراک ویژه دریافت کردید!\n"
                    f"✨ لیمیت روزانه شما به {PREMIUM_DAILY_LIMIT} اتصال افزایش یافت\n"
                    f"📅 تاریخ انقضا: {expiry_date}\n\n"
                    f"🙏 از حمایت شما سپاسگزاریم"
                )
                await bot.send_message(
                    uid,
                    f"✅ **اشتراک VIP اعطا شد**\n"
                    f"┣ 👤 کاربر: {target}\n"
                    f"┣ 📅 مدت: {days} روز\n"
                    f"┗ 📆 انقضا: {expiry_date}"
                )
                log_event("premium_gift", uid, target, f"{days}days")
            else:
                await bot.send_message(uid, f"❌ کاربر با آیدی `{target}` یافت نشد")
        except ValueError:
            await bot.send_message(uid, "❌ تعداد روز باید عدد باشد")
    else:
        await bot.send_message(uid, "❌ فرمت صحیح: `آیدی_کاربر تعداد_روز`")
async def handle_admin_reply(uid: str, text: str) -> None:
    parts = text.strip().split(maxsplit=1)
    if len(parts) >= 2:
        target = parts[0]
        reply_text = parts[1]
        if target in USERS:
            try:
                await bot.send_message(
                    target,
                    f"💬 **پاسخ پشتیبانی {BOT_NAME}** 💬\n\n{reply_text}\n\n━━━━━━━━━━━━━━\n📨 پشتیبانی: @{CHANNEL_USERNAME}"
                )
                await bot.send_message(uid, f"✅ پاسخ به {target} ارسال شد")
                log_event("admin_reply", uid, target, reply_text[:50])
            except Exception as e:
                await bot.send_message(uid, f"❌ خطا در ارسال: {str(e)}")
        else:
            await bot.send_message(uid, f"❌ کاربر با آیدی `{target}` یافت نشد")
    else:
        await bot.send_message(uid, "❌ فرمت صحیح: `آیدی_کاربر متن پاسخ`")
async def send_admin_stats(uid: str):
    total_users = len(USERS)
    online = online_count()
    banned = sum(1 for u in USERS.values() if u.get("is_banned"))
    premium = sum(1 for u in USERS.values() if u.get("is_premium"))
    chatting = sum(1 for u in USERS.values() if u.get("status") == "chatting")
    waiting = sum(1 for u in USERS.values() if u.get("status") == "waiting")
    registered = sum(1 for u in USERS.values() if u.get("registered"))
    today_connects = sum(1 for u in USERS.values() if u.get("last_connect_reset") and
                        isinstance(u["last_connect_reset"], datetime) and
                        u["last_connect_reset"].date() == datetime.now().date())
    stats_text = f"""👑 **پنل مدیریت {BOT_NAME}** 👑

📊 **آمار کلی:**
━━━━━━━━━━━━━━
👥 **کل کاربران:** {total_users}
✅ **ثبت‌نام شده:** {registered}
🟢 **آنلاین:** {online}
🔴 **آفلاین:** {total_users - online}

💬 **وضعیت گفتگوها:**
━━━━━━━━━━━━━━
🎯 **در حال گفتگو:** {chatting}
⏳ **در صف انتظار:** {waiting}
🚫 **مسدود شده:** {banned}
👑 **VIP:** {premium}
📞 **اتصال امروز:** {today_connects}

{'-'*30}
{build_waiting_stats()}"""
    await bot.send_message(uid, stats_text)
async def send_profile_editor(uid: str):
    builder = ChatKeypadBuilder()
    builder.row(
        builder.button(id="set_name", text="✏️ نام"),
        builder.button(id="set_age", text="🎂 سن"),
    )
    builder.row(
        builder.button(id="set_gender", text="⚧️ جنسیت"),
        builder.button(id="set_province", text="🗺️ استان"),
    )
    builder.row(
        builder.button(id="set_city", text="🏙️ شهر"),
        builder.button(id="set_height", text="📏 قد"),
    )
    builder.row(builder.button(id="set_bio", text="💫 بیو"))
    builder.row(builder.button(id="set_photo", text="🖼️ عکس"))
    builder.row(builder.button(id="back_main", text="🔙 بازگشت"))
    await bot.send_message(
        uid, f"⚡ {BOT_NAME} - ویرایش پروفایل:", chat_keypad=builder.build(resize_keyboard=True)
    )
async def handle_profile_input(uid: str, text: str, msg: Message) -> bool:
    profile_state = USERS[uid].get("profile_state", STATE_NONE)
    if profile_state == STATE_SETTING_NAME:
        USERS[uid]["name"] = text.strip()
        USERS[uid]["profile_state"] = STATE_SETTING_AGE
        await ask_field(uid, "age")
        return True
    if profile_state == STATE_SETTING_AGE:
        try:
            a = int(text.strip())
            USERS[uid]["age"] = a
            USERS[uid]["profile_state"] = STATE_SETTING_GENDER
            await bot.send_message(uid, f"⚧️ {BOT_NAME} جان! لطفاً جنسیت خود را وارد کنید:\n• مرد\n• زن")
        except Exception:
            await bot.send_message(uid, "❌ لطفاً یک عدد معتبر وارد کنید")
        return True
    if profile_state == STATE_SETTING_GENDER:
        t = text.strip().lower()
        if t in ("male", "female", "مرد", "زن"):
            USERS[uid]["gender"] = t
            USERS[uid]["profile_state"] = STATE_SETTING_PROVINCE
            await ask_field(uid, "province")
        else:
            await bot.send_message(uid, "❌ لطفاً 'مرد' یا 'زن' وارد کنید")
        return True
    if profile_state == STATE_SETTING_PROVINCE:
        USERS[uid]["province"] = text.strip()
        USERS[uid]["profile_state"] = STATE_SETTING_CITY
        await ask_field(uid, "city")
        return True
    if profile_state == STATE_SETTING_CITY:
        USERS[uid]["city"] = text.strip()
        USERS[uid]["profile_state"] = STATE_SETTING_HEIGHT
        await ask_field(uid, "height")
        return True
    if profile_state == STATE_SETTING_HEIGHT:
        USERS[uid]["height"] = text.strip()
        USERS[uid]["profile_state"] = STATE_SETTING_BIO
        await ask_field(uid, "bio")
        return True
    if profile_state == STATE_SETTING_BIO:
        USERS[uid]["bio"] = text.strip()
        USERS[uid]["profile_state"] = STATE_SETTING_PHOTO
        save_db()
        await bot.send_message(
            uid,
            f"🖼️ {BOT_NAME} جان! حالا لطفاً یک عکس برای پروفایلت بفرست یا بنویس 'ندارم' اگر نمی‌خوای عکس داشته باشی:",
            chat_keypad=None,
        )
        return True
    if profile_state == STATE_SETTING_PHOTO and text.strip().lower() == "ندارم":
        USERS[uid]["profile_state"] = STATE_NONE
        USERS[uid]["registered"] = True
        save_db()
        promo_msg = f"\n\n🌟 برای حمایت از {BOT_NAME}، حتما کانالمون رو فالو کنید:\n{CHANNEL_LINK}"
        await bot.send_message(
            uid, f"🎉 ثبت‌نام با موفقیت تکمیل شد! به خانواده {BOT_NAME} خوش آمدید{promo_msg}",
            chat_keypad=build_main_menu(uid)
        )
        return True
    edit_map = {
        STATE_SETTING_NAME: ("name", "نام"),
        STATE_SETTING_AGE: ("age", "سن"),
        STATE_SETTING_GENDER: ("gender", "جنسیت"),
        STATE_SETTING_PROVINCE: ("province", "استان"),
        STATE_SETTING_CITY: ("city", "شهر"),
        STATE_SETTING_HEIGHT: ("height", "قد"),
        STATE_SETTING_BIO: ("bio", "بیوگرافی"),
    }
    if profile_state in edit_map:
        field = edit_map[profile_state][0]
        if field == "age":
            try:
                USERS[uid][field] = int(text.strip())
            except Exception:
                await bot.send_message(uid, "❌ لطفاً یک عدد معتبر وارد کنید")
                return True
        elif field == "gender":
            t = text.strip().lower()
            if t not in ("male", "female", "مرد", "زن"):
                await bot.send_message(uid, "❌ لطفاً 'مرد' یا 'زن' وارد کنید")
                return True
            USERS[uid][field] = t
        else:
            USERS[uid][field] = text.strip()
        USERS[uid]["profile_state"] = STATE_NONE
        save_db()
        await bot.send_message(
            uid,
            f"✅ {edit_map[profile_state][1]} با موفقیت آپدیت شد",
            chat_keypad=build_main_menu(uid),
        )
        return True
    return False
@bot.on_message()
async def message_handler(bot_instance: Robot, msg: Message):
    users_count = len(USERS)
    inline_keypad = build_inline_keypad(users_count)
    uid = str(msg.chat_id)
    text = msg.text.strip() if msg.text else ""
    update_last_seen(uid)
    if uid not in USERS:
        USERS[uid] = default_user()
    if not USERS[uid].get("name"):
        try:
            USERS[uid]["name"] = await bot.get_name(msg.chat_id)
        except Exception:
            pass
    if USERS[uid]["is_banned"] and USERS[uid].get("ban_time"):
        bt = USERS[uid]["ban_time"]
        if isinstance(bt, str):
            try:
                bt = datetime.fromisoformat(bt)
            except Exception:
                bt = None
        if bt:
            if datetime.now() - bt >= BAN_DURATION:
                USERS[uid]["is_banned"] = False
                USERS[uid]["ban_time"] = None
                save_db()
    if USERS[uid]["is_banned"]:
        await msg.reply(f"🚫 دسترسی شما محدود شده است. با پشتیبانی {BOT_NAME} تماس بگیرید.")
        return
    check_premium(uid)
    reset_daily_connect(uid)
    user_state = get_user_state(uid)
    current_state = user_state["state"]
    temp_data = user_state["temp_data"]
    if current_state == STATE_REPORT:
        report_text = text
        user_info = f"{USERS[uid].get('name', 'ناشناس')} | {uid}"
        for owner_id in chat_owner_ids:
            try:
                await bot.send_message(
                    chat_id=owner_id,
                    text=f"📬 **گزارش جدید از {BOT_NAME}** 📬\n\n"
                         f"👤 **کاربر:** {user_info}\n"
                         f"📝 **متن گزارش:**\n```\n{report_text}\n```\n"
                         f"🕐 **زمان:** {datetime.now().strftime('%H:%M:%S - %Y/%m/%d')}",
                )
            except Exception as e:
                print(f"خطا در ارسال گزارش به ادمین: {e}")
        await msg.reply(f"✅ {BOT_NAME} جان! گزارش شما با موفقیت ارسال شد.\n📨 به زودی پاسخ شما را میدهیم.")
        clear_user_state(uid)
        return
    elif current_state == STATE_BROADCAST and is_admin(uid):
        await handle_admin_broadcast(uid, text)
        clear_user_state(uid)
        return
    elif current_state == STATE_BAN_USER and is_admin(uid):
        await handle_admin_ban(uid, text)
        clear_user_state(uid)
        return
    elif current_state == STATE_UNBAN_USER and is_admin(uid):
        await handle_admin_unban(uid, text)
        clear_user_state(uid)
        return
    elif current_state == STATE_PREMIUM_GIFT and is_admin(uid):
        await handle_admin_premium(uid, text)
        clear_user_state(uid)
        return
    elif current_state == STATE_ADMIN_REPLY and is_admin(uid):
        await handle_admin_reply(uid, text)
        clear_user_state(uid)
        return
    if await handle_profile_input(uid, text, msg):
        return
    if getattr(msg, "file", None) and USERS[uid].get("profile_state") == STATE_SETTING_PHOTO:
        file_obj = msg.file
        file_id = getattr(file_obj, "file_id", None) or getattr(file_obj, "id", None)
        if not file_id:
            await bot.send_message(uid, "❌ خطا در دریافت فایل")
            return
        file_path = PROFILE_PICS_DIR / f"{uid}.jpg"
        try:
            await bot.download(file_id=file_id, save_as=str(file_path), verbose=True)
            USERS[uid]["profile_photo_path"] = str(file_path)
            USERS[uid]["profile_state"] = STATE_NONE
            USERS[uid]["registered"] = True
            save_db()
            await bot.send_message(
                uid, f"✅ عکس پروفایل ذخیره شد. به {BOT_NAME} خوش آمدید!",
                chat_keypad=build_main_menu(uid)
            )
        except Exception:
            await bot.send_message(uid, "❌ خطا در ذخیره عکس")
        return
    if text == "/start":
        if not USERS[uid]["registered"]:
            welcome_msg = f"""🌸 **به ربات {BOT_NAME} خوش آمدید!** 🌸

✨ اینجا می‌تونی با افراد جدید آشنا بشی و گفتگوهای جذاب داشته باشی.
💫 {BOT_NAME} همیشه همراه توئه!

📢 حتما کانال ما رو دنبال کن:
{CHANNEL_LINK}

━━━━━━━━━━━━━━
لطفاً نام خود را وارد کنید:"""
            USERS[uid]["profile_state"] = STATE_SETTING_NAME
            await bot.send_message(uid, welcome_msg)
            return
        else:
            remaining = (PREMIUM_DAILY_LIMIT if USERS[uid].get("is_premium") else DAILY_CONNECT_LIMIT) - USERS[uid]["daily_connects"]
            welcome_back = f"🌸 {BOT_NAME} جان! خوش برگشتی!\n✨ امروز {remaining} بار فرصت گفتگو داری."
            await bot.send_message(
                uid, welcome_back, chat_keypad=build_main_menu(uid), inline_keypad=inline_keypad
            )
            return
    if text == "📢 " + CHANNEL_USERNAME or text == "channel_promo":
        await msg.reply(f"📢 **کانال رسمی {BOT_NAME}** 📢\n\n{CHANNEL_LINK}\n\n🌸 با ما همراه باشید!")
        return
    btn_map = {
        "🎯 هم‌سن": "same_age",
        "🏙️ هم‌شهری": "same_city",
        "🗺️ هم‌استان": "same_province",
        "⚧️ هم‌جنسیت": "same_gender",
        "🎲 تصادفی": "random",
        "❌ لغو جستجو": "cancel_search",
        "🔚 پایان گفتگو": "end_chat",
        "👤 پروفایل من": "my_profile",
        "⚙️ ویرایش": "edit_profile",
        "🔙 بازگشت": "back_main",
        "☎️ پشتیبانی": "sup",
        "📎 اشتراک لینک": "link",
        "🖼️ عکس": "set_photo",
        "💫 بیو": "set_bio",
        "🚨 گزارش": "report",
        "👁️ پروفایل مخاطب": "view_peer_profile",
        "✏️ نام": "set_name",
        "🎂 سن": "set_age",
        "⚧️ جنسیت": "set_gender",
        "🗺️ استان": "set_province",
        "🏙️ شهر": "set_city",
        "📏 قد": "set_height",
        "📊 آمار": "stats",
        "👑 پنل مدیریت": "admin_panel",
        "📊 آمار کلی": "admin_stats",
        "👥 کاربران": "admin_users",
        "📢 پیام همگانی": "admin_broadcast",
        "🚫 مسدود کاربر": "admin_ban",
        "✅ رفع مسدود": "admin_unban",
        "👑 اعطای VIP": "admin_premium",
        "🚨 گزارشات": "admin_reports",
        "📋 لاگ‌ها": "admin_logs",
        "💬 پاسخ به کاربر": "admin_reply",
        "❓ راهنما": "admin_help",
    }
    action = btn_map.get(text, None)
    if action in ("same_age", "same_city", "same_province", "same_gender", "random"):
        if not USERS[uid]["registered"]:
            await msg.reply(f"❌ {BOT_NAME} جان! لطفاً ابتدا پروفایل خود را تکمیل کنید")
            return
        await enqueue_and_try(uid, action)
        return
    if action == "stats":
        await msg.reply(build_waiting_stats(), chat_keypad=build_main_menu(uid))
        return
    if action == "cancel_search":
        async with queues_lock:
            for q in WAIT_QUEUES.values():
                if uid in q:
                    try:
                        q.remove(uid)
                    except ValueError:
                        pass
        USERS[uid]["status"] = "idle"
        USERS[uid]["wait_type"] = None
        await msg.reply("✅ جستجو لغو شد", chat_keypad=build_main_menu(uid))
        return
    if action == "end_chat":
        if USERS[uid]["status"] != "chatting":
            await msg.reply(
                f"💬 {BOT_NAME} جان! شما در حال گفتگو نیستید",
                chat_keypad=build_main_menu(uid)
            )
            return
        await end_chat(uid, f"💔 {BOT_NAME} جان! هم‌صحبت گفتگو را ترک کرد")
        return
    if action == "my_profile":
        u = USERS[uid]
        remaining = (PREMIUM_DAILY_LIMIT if u.get("is_premium") else DAILY_CONNECT_LIMIT) - u["daily_connects"]
        premium_status = "✅ فعال" if u.get("is_premium") else "❌ غیرفعال"
        profile_text = f"""👤 **پروفایل {BOT_NAME}** 👤

━━━━━━━━━━━━━━
✨ **نام:** {u.get('name') or 'ثبت نشده'}
🎂 **سن:** {u.get('age') or 'ثبت نشده'}
⚧️ **جنسیت:** {u.get('gender') or 'ثبت نشده'}
🗺️ **استان:** {u.get('province') or 'ثبت نشده'}
🏙️ **شهر:** {u.get('city') or 'ثبت نشده'}
📏 **قد:** {u.get('height') or 'ثبت نشده'}
━━━━━━━━━━━━━━
💫 **بیو:** {u.get('bio') or 'ثبت نشده'}
━━━━━━━━━━━━━━
👑 **VIP:** {premium_status}
💬 **کل گفتگوها:** {u.get('total_chats', 0)}
📊 **امروز:** {u.get('daily_connects', 0)}/{PREMIUM_DAILY_LIMIT if u.get('is_premium') else DAILY_CONNECT_LIMIT}
📈 **باقی‌مانده:** {remaining}"""
        photo = u.get("profile_photo_path")
        if photo and os.path.exists(photo):
            try:
                await bot.send_image(uid, path=photo, text=profile_text)
            except Exception:
                await msg.reply(profile_text, chat_keypad=build_main_menu(uid))
        else:
            await msg.reply(profile_text, chat_keypad=build_main_menu(uid))
        return
    if action == "edit_profile":
        await send_profile_editor(uid)
        return
    if action == "link":
        await msg.reply(text_link)
        return
    if action == "sup":
        set_user_state(uid, STATE_REPORT, {"type": "support"})
        await msg.reply(f"📝 {BOT_NAME} جان! لطفاً پیام خود را برای پشتیبانی وارد کنید:\n(هر پیامی که بفرستید مستقیماً به ادمین ارسال میشه)")
        return
    if action == "admin_panel" and is_admin(uid):
        await bot.send_message(uid, f"👑 **پنل مدیریت {BOT_NAME}** 👑\nلطفاً یکی از گزینه‌ها را انتخاب کنید:", chat_keypad=build_admin_panel())
        return
    if action == "admin_stats" and is_admin(uid):
        await send_admin_stats(uid)
        return
    if action == "admin_users" and is_admin(uid):
        active_users = sum(1 for u in USERS.values() if u.get("last_seen") and
                          isinstance(u["last_seen"], datetime) and
                          datetime.now() - u["last_seen"] < timedelta(days=1))
        today_users = sum(1 for u in USERS.values() if u.get("last_seen") and
                         isinstance(u["last_seen"], datetime) and
                         u["last_seen"].date() == datetime.now().date())
        await msg.reply(f"👥 **آمار کاربران {BOT_NAME}**\n\n"
                       f"┣ 📊 کل کاربران: {len(USERS)}\n"
                       f"┣ 🟢 فعال امروز: {today_users}\n"
                       f"┗ 🌙 فعال ۲۴ ساعت: {active_users}")
        return
    if action == "admin_broadcast" and is_admin(uid):
        set_user_state(uid, STATE_BROADCAST)
        await msg.reply(f"📢 **ارسال پیام همگانی**\n\n"
                       f"لطفاً متن پیام خود را وارد کنید:\n"
                       f"⚠️ این پیام برای {len(USERS)} کاربر ارسال خواهد شد.")
        return
    if action == "admin_ban" and is_admin(uid):
        set_user_state(uid, STATE_BAN_USER)
        await msg.reply(f"🚫 **مسدود کردن کاربر**\n\n"
                       f"لطفاً آیدی کاربر مورد نظر را وارد کنید:\n"
                       f"(مثال: `b0FvdsD0MKg...`)")
        return
    if action == "admin_unban" and is_admin(uid):
        set_user_state(uid, STATE_UNBAN_USER)
        await msg.reply(f"✅ **رفع مسدود کاربر**\n\n"
                       f"لطفاً آیدی کاربر مورد نظر را وارد کنید:")
        return
    if action == "admin_premium" and is_admin(uid):
        set_user_state(uid, STATE_PREMIUM_GIFT)
        await msg.reply(f"👑 **اعطای اشتراک VIP**\n\n"
                       f"لطفاً آیدی کاربر و تعداد روز را وارد کنید:\n"
                       f"مثال: `b0FvdsD0MKg... 30`")
        return
    if action == "admin_reply" and is_admin(uid):
        set_user_state(uid, STATE_ADMIN_REPLY)
        await msg.reply(f"💬 **پاسخ به کاربر**\n\n"
                       f"لطفاً آیدی کاربر و متن پاسخ را وارد کنید:\n"
                       f"مثال: `b0FvdsD0MKg... سلام، پیامت دریافت شد`")
        return
    if action == "admin_reports" and is_admin(uid):
        reported_users = []
        for uid2, u2 in USERS.items():
            if u2.get("report_count", 0) > 0:
                name = u2.get('name', 'ناشناس')
                reported_users.append(f"👤 **{name}**\n┣ 🆔 `{uid2[:8]}...`\n┣ 📊 گزارشات: {u2['report_count']}\n┗ 🚫 وضعیت: {'مسدود' if u2.get('is_banned') else 'فعال'}")
        if reported_users:
            chunks = ["🚨 **کاربران گزارش شده** 🚨\n\n" + "\n\n".join(reported_users[i:i+5]) for i in range(0, len(reported_users), 5)]
            for chunk in chunks:
                await msg.reply(chunk)
        else:
            await msg.reply("✅ هیچ کاربری گزارش نشده است.")
        return
    if action == "admin_help" and is_admin(uid):
        help_text = f"""❓ **راهنمای پنل مدیریت {BOT_NAME}** ❓

📢 **پیام همگانی:** ارسال پیام به همه کاربران
🚫 **مسدود کاربر:** جلوگیری از دسترسی کاربر
✅ **رفع مسدود:** فعالسازی مجدد کاربر
👑 **اعطای VIP:** افزایش لیمیت روزانه کاربر
💬 **پاسخ به کاربر:** ارسال پیام مستقیم
🚨 **گزارشات:** مشاهده کاربران گزارش شده

📌 **نکات:**
• از آیدی کامل کاربر استفاده کنید
• در اعطای VIP تعداد روز را مشخص کنید
• پیام همگانی با احتیاط ارسال شود"""
        await msg.reply(help_text)
        return
    if action and action.startswith("set_"):
        field = action.split("_", 1)[1]
        field_map = {
            "name": STATE_SETTING_NAME,
            "age": STATE_SETTING_AGE,
            "gender": STATE_SETTING_GENDER,
            "province": STATE_SETTING_PROVINCE,
            "city": STATE_SETTING_CITY,
            "height": STATE_SETTING_HEIGHT,
            "bio": STATE_SETTING_BIO,
        }
        if field in field_map:
            USERS[uid]["profile_state"] = field_map[field]
            prompts = {
                "name": f"✏️ {BOT_NAME} جان! نام جدید خود را وارد کنید:",
                "age": f"🎂 {BOT_NAME} جان! سن جدید خود را وارد کنید:",
                "gender": f"⚧️ {BOT_NAME} جان! جنسیت جدید خود را وارد کنید:\n• مرد\n• زن",
                "province": f"🗺️ {BOT_NAME} جان! استان جدید خود را وارد کنید:",
                "city": f"🏙️ {BOT_NAME} جان! شهر جدید خود را وارد کنید:",
                "height": f"📏 {BOT_NAME} جان! قد جدید خود را وارد کنید (سانتیمتر):",
                "bio": f"💫 {BOT_NAME} جان! بیوگرافی جدید خود را بنویسید:",
            }
            await bot.send_message(uid, prompts[field])
            return
    if action == "set_photo":
        USERS[uid]["profile_state"] = STATE_SETTING_PHOTO
        await bot.send_message(uid, f"🖼️ {BOT_NAME} جان! لطفاً عکس پروفایل جدید خود را ارسال کنید:")
        return
    if action == "report":
        peer = USERS[uid].get("active_chat")
        if not peer:
            await msg.reply("❌ هم‌صحبتی برای گزارش ندارید")
            return
        if uid in USERS[peer]["reported_by"]:
            await msg.reply("⚠️ شما قبلاً این کاربر را گزارش کرده‌اید")
            return
        USERS[peer]["reported_by"].add(uid)
        USERS[peer]["report_count"] = len(USERS[peer]["reported_by"])
        USERS[peer]["total_reports_received"] += 1
        USERS[uid]["total_reports_sent"] += 1
        log_event("report", uid, peer, "user_report")
        if USERS[peer]["report_count"] >= REPORT_THRESHOLD:
            USERS[peer]["is_banned"] = True
            USERS[peer]["ban_time"] = datetime.now()
            if USERS[peer]["active_chat"]:
                await end_chat(peer, f"🚫 کاربر به دلیل گزارش‌های متعدد توسط {BOT_NAME} مسدود شد")
            await msg.reply("✅ کاربر گزارش و مسدود شد")
            for admin in chat_owner_ids:
                try:
                    await bot.send_message(admin, f"🚨 **گزارش خودکار**\n\nکاربر `{peer[:8]}...` با {REPORT_THRESHOLD} گزارش مسدود شد.")
                except:
                    pass
        else:
            await msg.reply(f"✅ گزارش شما ثبت شد. {REPORT_THRESHOLD - USERS[peer]['report_count']} گزارش دیگر تا مسدودیت")
        save_db()
        return
    if action == "view_peer_profile":
        peer = USERS[uid].get("active_chat")
        if not peer:
            await msg.reply("❌ هم‌صحبتی ندارید")
            return
        p = USERS[peer]
        profile_text = f"""👤 **پروفایل مخاطب** 👤

━━━━━━━━━━━━━━
✨ **نام:** {p.get('name') or 'ناشناس'}
🎂 **سن:** {p.get('age') or 'ثبت نشده'}
⚧️ **جنسیت:** {p.get('gender') or 'ثبت نشده'}
🗺️ **استان:** {p.get('province') or 'ثبت نشده'}
🏙️ **شهر:** {p.get('city') or 'ثبت نشده'}
━━━━━━━━━━━━━━
💫 **بیو:** {p.get('bio') or 'ثبت نشده'}
━━━━━━━━━━━━━━"""
        photo = p.get("profile_photo_path")
        if photo and os.path.exists(photo):
            try:
                await bot.send_image(uid, path=photo, text=profile_text)
            except Exception:
                await msg.reply(profile_text)
        else:
            await msg.reply(profile_text)
        return
    if USERS[uid]["status"] == "chatting":
        peer = USERS[uid].get("active_chat")
        if not peer or peer not in USERS:
            USERS[uid]["status"] = "idle"
            USERS[uid]["active_chat"] = None
            await msg.reply(f"🔌 ارتباط قطع شد", chat_keypad=build_main_menu(uid))
            return
        if text:
            if text.startswith("/"):
                await msg.reply("❌ دستور در حین گفتگو مجاز نیست")
                return
            try:
                await bot.send_message(peer, text)
                log_event("message", uid, peer, text)
            except Exception:
                await msg.reply("❌ ارسال پیام ناموفق")
            return
        if getattr(msg, "file", None):
            file_obj = msg.file
            file_id = getattr(file_obj, "file_id", None) or getattr(file_obj, "id", None)
            file_name = getattr(file_obj, "file_name", "file")
            if not file_id:
                await msg.reply("❌ خطا در دریافت فایل")
                return
            temp_dir = Path("temp_files")
            temp_dir.mkdir(exist_ok=True)
            temp_file_path = temp_dir / f"{file_id}_{file_name}"
            try:
                await bot.download(file_id=file_id, save_as=str(temp_file_path), verbose=True)
                try:
                    await bot.send_image(
                        peer,
                        path=str(temp_file_path),
                        text=msg.text if msg.text else None,
                    )
                    log_event("image", uid, peer, file_name)
                except Exception:
                    try:
                        await bot.send_file(
                            peer,
                            path=str(temp_file_path),
                            caption=msg.text if msg.text else None,
                        )
                        log_event("file", uid, peer, file_name)
                    except Exception as e:
                        await msg.reply(f"❌ ارسال فایل ناموفق: {str(e)}")
            except Exception as e:
                await msg.reply(f"❌ دانلود فایل ناموفق: {str(e)}")
            finally:
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass
            return
        await msg.reply("❌ پیام نامعتبر")
        return
    await msg.reply(
        f"⚡ {BOT_NAME} جان! لطفاً از منوی اصلی استفاده کنید",
        chat_keypad=build_main_menu(uid), inline_keypad=inline_keypad
    )
async def autosave_loop():
    while True:
        try:
            save_db()
        except Exception as e:
            print("💾 خطا در ذخیره خودکار:", e)
        await asyncio.sleep(AUTOSAVE_INTERVAL)
async def cleanup_queues_loop():
    while True:
        async with queues_lock:
            for key, q in WAIT_QUEUES.items():
                for uid in list(q):
                    u = USERS.get(uid)
                    if not u or u.get("is_banned") or u.get("status") != "waiting":
                        try:
                            q.remove(uid)
                        except ValueError:
                            pass
                queue_stats[key] = len(q)
        await asyncio.sleep(30)
async def cleanup_states_loop():
    while True:
        now = datetime.now()
        for uid, state_data in list(user_states.items()):
            last_action = state_data.get("last_action")
            if last_action:
                try:
                    last_time = datetime.fromisoformat(last_action)
                    if now - last_time > timedelta(minutes=30):
                        clear_user_state(uid)
                except:
                    pass
        await asyncio.sleep(300)
def boot_message():
    print(f"""
    🌸══════════════════════════════🌸
    
          🚀 ربات {BOT_NAME} 🚀
          ✨ نسخه 2.0 پیشرفته ✨
    
    🌸══════════════════════════════🌸
    
    📊 وضعیت:
    ┣ 👑 پنل مدیریت: فعال
    ┣ 📱 محدودیت روزانه: فعال
    ┣ 🎯 هم‌جنسیت: اضافه شد
    ┣ 💾 ذخیره خودکار: {AUTOSAVE_INTERVAL} ثانیه
    ┗ 🌸 ساخته شده با عشق
    
    📢 کانال: {CHANNEL_LINK}
    
    🌸══════════════════════════════🌸
    """)
async def main():
    load_db()
    asyncio.create_task(autosave_loop())
    asyncio.create_task(cleanup_queues_loop())
    asyncio.create_task(cleanup_states_loop())
    boot_message()
    try:
        await bot.run()
    finally:
        save_db()
        print(f"💾 دیتابیس {BOT_NAME} ذخیره شد...")
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"🔚 خروج از ربات {BOT_NAME}...")
        save_db()