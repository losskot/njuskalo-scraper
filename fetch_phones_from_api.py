
import os
import re
import json
import asyncio
import sqlite3
import logging
import time
from datetime import datetime
from curl_cffi.requests import AsyncSession

# Comprehensive logging setup
def setup_comprehensive_logging():
    """Setup comprehensive logging with info and error loggers"""
    logs_dir = os.path.join(os.path.dirname(__file__), "backend", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    info_log_file = os.path.join(logs_dir, f"info_{date_str}.log")
    error_log_file = os.path.join(logs_dir, f"error_{date_str}.log")
    
    # Setup info logger
    global info_logger
    info_logger = logging.getLogger('phone_api_info')
    info_logger.setLevel(logging.INFO)
    if not info_logger.handlers:
        info_handler = logging.FileHandler(info_log_file, encoding='utf-8')
        info_formatter = logging.Formatter('%(asctime)s - %(message)s')
        info_handler.setFormatter(info_formatter)
        info_logger.addHandler(info_handler)
        info_logger.propagate = False
    
    # Setup error logger
    global error_logger
    error_logger = logging.getLogger('phone_api_error')
    error_logger.setLevel(logging.ERROR)
    if not error_logger.handlers:
        error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
        error_formatter = logging.Formatter('%(asctime)s - %(message)s')
        error_handler.setFormatter(error_formatter)
        error_logger.addHandler(error_handler)
        error_logger.propagate = False

# Global counters
http_success_count = 0
http_failure_count = 0
parsing_success_count = 0
parsing_failure_count = 0

def log_process_start(process_name):
    """Log process start"""
    info_logger.info(f"PROCESS_START: {process_name} started at {datetime.now().isoformat()}")

def log_process_end(process_name, start_time):
    """Log process end with duration and summary"""
    duration = time.time() - start_time
    summary = f"Duration: {duration:.2f}s, HTTP Success: {http_success_count}, HTTP Failures: {http_failure_count}, Parsing Success: {parsing_success_count}, Parsing Failures: {parsing_failure_count}"
    info_logger.info(f"PROCESS_END: {process_name} completed at {datetime.now().isoformat()} - {summary}")

def log_http_completion(url, status_code, response_size, method="GET"):
    """Log successful HTTP request"""
    global http_success_count
    http_success_count += 1
    info_logger.info(f"HTTP_SUCCESS: {method} {url} -> {status_code} ({response_size} bytes)")

def log_http_failure(url, error_msg, duration_ms, method="GET"):
    """Log failed HTTP request"""
    global http_failure_count
    http_failure_count += 1
    error_logger.error(f"HTTP_FAILURE: {method} {url} failed after {duration_ms}ms - {error_msg}")

def log_parsing_completion(operation, items_parsed, data_type="json"):
    """Log successful parsing operation"""
    global parsing_success_count
    parsing_success_count += 1
    info_logger.info(f"PARSING_SUCCESS: {operation} parsed {items_parsed} items of type {data_type}")

def log_parsing_failure(operation, error_msg, html_snippet=""):
    """Log failed parsing operation with HTML snippet"""
    global parsing_failure_count
    parsing_failure_count += 1
    snippet = html_snippet[:500] + "..." if len(html_snippet) > 500 else html_snippet
    error_logger.error(f"PARSING_FAILURE: {operation} failed - {error_msg} | HTML: {snippet}")

def log_exception(operation, exception):
    """Log exception with full traceback"""
    import traceback
    error_logger.error(f"EXCEPTION: {operation} - {str(exception)} | Traceback: {traceback.format_exc()}")


# ---- CONFIGURATION ----
# No hardcoded BEARER_TOKEN or COOKIES; always use fresh from Playwright script


# Directory containing all entry HTMLs (use backend/website)
target_dir = os.path.join(os.path.dirname(__file__), "backend", "website")




 # SQLite DB setup in backend/phoneDB folder
phone_db_dir = os.path.join(os.path.dirname(__file__), "backend", "phoneDB")
os.makedirs(phone_db_dir, exist_ok=True)
db_path = os.path.join(phone_db_dir, "phones.db")

# --- LOGGING SETUP ---
log_path = os.path.join(phone_db_dir, "phones.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
def init_db():
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS phones (
            ad_id TEXT PRIMARY KEY,
            phones TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_phones_to_db(ad_id, phone_list):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Always overwrite (upsert)
    c.execute("REPLACE INTO phones (ad_id, phones) VALUES (?, ?)",
              (ad_id, json.dumps(phone_list, ensure_ascii=False) if phone_list is not None else None))
    conn.commit()
    conn.close()



# Extract ad id as the number from the filename (before .html extension)
ad_id_re = re.compile(r"^([0-9]+)\.html$")

# Njuskalo phone API endpoint
def phone_api_url(ad_id):
    return f"https://www.njuskalo.hr/ccapi/v4/phone-numbers/ad/{ad_id}"

import random

import importlib.util
import sys
import asyncio

spec = importlib.util.spec_from_file_location("bearer_token_finder", os.path.join(os.path.dirname(__file__), "bearer_token_finder.py"))
if spec is None or spec.loader is None:
    raise ImportError("Could not load bearer_token_finder.py module spec or loader.")
bearer_token_finder = importlib.util.module_from_spec(spec)
sys.modules["bearer_token_finder"] = bearer_token_finder
spec.loader.exec_module(bearer_token_finder)

PROXY_LIST = [
    None  # Local system (no proxy)
]


# --- Token/cookie refresh logic ---
async def get_token_and_cookies():
    # Call the Playwright async function directly
    return await bearer_token_finder.get_bearer_token_and_cookies()

async def fetch_phone_number(session, ad_id, bearer_token, cookies):
    url = phone_api_url(ad_id)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": f"https://www.njuskalo.hr/nekretnine/*-oglas-{ad_id}",
    }
    proxy_cfg = None  # Always use local, no proxy
    try:
        resp = await session.get(url, headers=headers, cookies=cookies, timeout=15, proxies=proxy_cfg)
        resp.raise_for_status()
        
        # Log HTTP success
        data = resp.json()
        log_http_completion(url, resp.status_code, len(str(data)))
        
        return data
    except Exception as e:
        # Log HTTP failure
        log_http_failure(url, str(e), 0)
        logging.error(f"ad_id {ad_id}: {e}")
        if '401' in str(e):
            return 'REFRESH_TOKEN'
        return None

def find_all_html_files():
    html_files = []
    for root, dirs, files in os.walk(target_dir):
        for fname in files:
            if fname.endswith(".html"):
                html_files.append(os.path.join(root, fname))
    return html_files


def extract_ad_id_from_filename(filename):
    # Extract ad_id from filename format: "12345.html"
    fname = os.path.basename(filename)
    m = ad_id_re.match(fname)
    return m.group(1) if m else None

def extract_time_from_html(html_path):
    # Try to extract the time from the HTML file (from meta or script tags)
    # If not found, use file modified time
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        # Try to find ISO date in the HTML (e.g. 2025-07-25T15:30:00)
        m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', html)
        if m:
            return m.group(1)
    except Exception:
        pass
    # Fallback: file modified time
    ts = os.path.getmtime(html_path)
    return datetime.fromtimestamp(ts).isoformat()

async def process_file(session, html_path, bearer_token, cookies):
    ad_id = extract_ad_id_from_filename(html_path)
    if not ad_id:
        logging.warning(f"[SKIP] Could not extract ad_id from {html_path}")
        return 'OK'

    data = await fetch_phone_number(session, ad_id, bearer_token, cookies)

    if data == 'REFRESH_TOKEN':
        return 'REFRESH_TOKEN'

    numbers = []
    try:
        numbers = [
            n["formattedNumber"]
            for n in data["data"]["attributes"]["numbers"]
            if n.get("formattedNumber")
        ]
        # Log successful parsing
        log_parsing_completion("phone_extraction", len(numbers), "phone_numbers")
    except Exception as e:
        # Log parsing failure
        log_parsing_failure("phone_extraction", str(e), str(data)[:1000] if data else "")
        logging.warning(f"[WARN] Failed to parse phone data for ad {ad_id}: {e}")

    if numbers:
        logging.info(f"[OK] Found {len(numbers)} phone(s) for ad {ad_id}")
    else:
        logging.info(f"[INFO] No phone numbers found for ad {ad_id}, saving null")

    # Save to DB (even if empty/null)
    save_phones_to_db(ad_id, numbers if numbers else None)

    return 'OK'


async def main():
    # Setup comprehensive logging
    setup_comprehensive_logging()
    
    # Log process start
    start_time = time.time()
    log_process_start("phone_fetching")
    
    html_files = find_all_html_files()
    logging.info(f"Found {len(html_files)} HTML files.")


    # --- FLAG: re-scrape ad_ids with null phone numbers ---
    RESCRAPE_NULL_PHONES = False  # Set to False to skip nulls, True to re-scrape nulls

    # Load ad_ids and their phone values from DB
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT ad_id, phones FROM phones")
    adid_to_phones = {row[0]: row[1] for row in c.fetchall()}
    conn.close()

    files_to_process = []
    skipped = 0
    rescrape_count = 0
    for path in html_files:
        ad_id = extract_ad_id_from_filename(path)
        if not ad_id:
            continue
        if ad_id in adid_to_phones:
            phones_val = adid_to_phones[ad_id]
            if phones_val is None or phones_val == 'null':
                if RESCRAPE_NULL_PHONES:
                    rescrape_count += 1
                    files_to_process.append(path)
                else:
                    skipped += 1
                continue
            skipped += 1
            continue
        files_to_process.append(path)
    logging.info(f"Skipping {skipped} files already in DB with phones. Re-scraping {rescrape_count} with null phones. {len(files_to_process)} files left to process.")

    # Get initial token and cookies
    bearer_token, cookies = await get_token_and_cookies()
    logging.info("\n[INFO] Using Bearer token:")
    logging.info(bearer_token)
    logging.info("\n[INFO] Using cookies:")
    logging.info(cookies)
    if not bearer_token or not cookies:
        logging.error("Could not get Bearer token or cookies. Exiting.")
        return
    async with AsyncSession() as session:
        i = 0
        BATCH_SIZE = 50
        while i < len(files_to_process):
            batch = files_to_process[i:i+BATCH_SIZE]
            results = await asyncio.gather(*[process_file(session, path, bearer_token, cookies) for path in batch])
            # If any batch result is 'REFRESH_TOKEN', refresh and retry that batch
            if 'REFRESH_TOKEN' in results:
                logging.warning("Refreshing Bearer token and cookies due to 401 error...")
                bearer_token, cookies = await get_token_and_cookies()
                logging.info("\n[INFO] Using Bearer token:")
                logging.info(bearer_token)
                logging.info("\n[INFO] Using cookies:")
                logging.info(cookies)
                if not bearer_token or not cookies:
                    logging.error("Could not refresh Bearer token or cookies. Exiting.")
                    return
                # Retry the same batch
                continue
            i += BATCH_SIZE
            # await asyncio.sleep(random.uniform(0.8, 1.2))
    
    # Log process end
    log_process_end("phone_fetching", start_time)


if __name__ == "__main__":
    init_db()
    asyncio.run(main())
