# This file was renamed from RealState copy.py to njuskalo_category_tree_scraper.py
# ...existing code from RealState copy.py will be placed here...
import sys
import argparse

# --- Safe print for Unicode output in all terminals ---
def safe_print(s):
    try:
        sys.stdout.buffer.write((str(s) + "\n").encode("utf-8"))
        sys.stdout.flush()
    except Exception:
        print(s)

import json
import asyncio
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
import random
import time
import os
from curl_cffi.requests import AsyncSession

# Import Playwright token/cookie fetcher
import importlib.util
spec = importlib.util.spec_from_file_location("bearer_token_finder", os.path.join(os.path.dirname(__file__), "bearer_token_finder.py"))
bearer_token_finder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bearer_token_finder)


from datetime import datetime
import threading
import logging
import traceback

# --- Comprehensive Logging System ---
def setup_comprehensive_logging():
    """Setup comprehensive logging to backend/logs/ directory"""
    logs_dir = os.path.join(os.path.dirname(__file__), "backend", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Info logger
    info_logger = logging.getLogger('njuskalo_info')
    info_logger.setLevel(logging.INFO)
    info_logger.handlers.clear()
    
    info_handler = logging.FileHandler(os.path.join(logs_dir, f"info_{today_str}.log"), encoding='utf-8')
    info_formatter = logging.Formatter('%(message)s')
    info_handler.setFormatter(info_formatter)
    info_logger.addHandler(info_handler)
    info_logger.propagate = False
    
    # Error logger
    error_logger = logging.getLogger('njuskalo_error')
    error_logger.setLevel(logging.ERROR)
    error_logger.handlers.clear()
    
    error_handler = logging.FileHandler(os.path.join(logs_dir, f"error_{today_str}.log"), encoding='utf-8')
    error_formatter = logging.Formatter('%(message)s')
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)
    error_logger.propagate = False
    
    return info_logger, error_logger

# Initialize comprehensive logging
comprehensive_info_logger, comprehensive_error_logger = setup_comprehensive_logging()

def log_process_start(process_name):
    """Log process start"""
    timestamp = datetime.now().isoformat()
    comprehensive_info_logger.info(f"[{timestamp}] PROCESS_START {process_name}")

def log_process_end(process_name, start_time, http_count=0, parse_count=0):
    """Log process end with summary"""
    timestamp = datetime.now().isoformat()
    duration = time.time() - start_time
    comprehensive_info_logger.info(f"[{timestamp}] PROCESS_END {process_name} (Duration: {duration:.2f}s)")
    comprehensive_info_logger.info(f"[{timestamp}] PROCESS_SUMMARY {process_name} - HTTP_Requests: {http_count}, Parsing_Operations: {parse_count}, Total_Duration: {duration:.2f}s")

def log_http_completion(url, status, duration_ms, status_code=None):
    """Log HTTP request completion"""
    timestamp = datetime.now().isoformat()
    display_url = url if len(url) <= 80 else url[:40] + "..." + url[-37:]
    status_info = f"[{status_code}]" if status_code else ""
    comprehensive_info_logger.info(f"[{timestamp}] HTTP {display_url} {status} {status_info} {duration_ms}ms")

def log_http_failure(url, error, duration_ms, response_details=None):
    """Log HTTP request failure"""
    timestamp = datetime.now().isoformat()
    display_url = url if len(url) <= 80 else url[:40] + "..." + url[-37:]
    comprehensive_info_logger.info(f"[{timestamp}] HTTP {display_url} FAILED {duration_ms}ms")
    
    # Detailed error logging
    error_lines = [
        f"[{timestamp}] HTTP_REQUEST_FAILED",
        f"URL: {url}",
        f"Duration: {duration_ms}ms",
        f"Error: {str(error)}",
        f"Error Type: {type(error).__name__}"
    ]
    
    if response_details:
        error_lines.append(f"Response Details: {response_details}")
    
    error_lines.append("Stack Trace:")
    error_lines.append(traceback.format_exc())
    comprehensive_error_logger.error("\n".join(error_lines))

def log_parsing_completion(filename, status, duration_ms, items_count=None):
    """Log parsing completion"""
    timestamp = datetime.now().isoformat()
    items_info = f"[{items_count} items]" if items_count is not None else ""
    comprehensive_info_logger.info(f"[{timestamp}] PARSE {filename} {status} {items_info} {duration_ms}ms")

def log_parsing_failure(filename, error, duration_ms, html_snippet=None):
    """Log parsing failure"""
    timestamp = datetime.now().isoformat()
    comprehensive_info_logger.info(f"[{timestamp}] PARSE {filename} FAILED {duration_ms}ms")
    
    error_lines = [
        f"[{timestamp}] PARSING_FAILED",
        f"Filename: {filename}",
        f"Duration: {duration_ms}ms",
        f"Error: {str(error)}",
        f"Error Type: {type(error).__name__}"
    ]
    
    if html_snippet:
        if len(html_snippet) > 1000:
            html_snippet = html_snippet[:500] + "\n... [TRUNCATED] ...\n" + html_snippet[-500:]
        error_lines.append(f"Problematic HTML Snippet:\n{html_snippet}")
    
    error_lines.append("Stack Trace:")
    error_lines.append(traceback.format_exc())
    comprehensive_error_logger.error("\n".join(error_lines))

def log_exception(context, error, additional_info=None):
    """Log any exception with context"""
    timestamp = datetime.now().isoformat()
    error_lines = [
        f"[{timestamp}] EXCEPTION",
        f"Context: {context}",
        f"Error: {str(error)}",
        f"Error Type: {type(error).__name__}"
    ]
    
    if additional_info:
        error_lines.append(f"Additional Info: {additional_info}")
    
    error_lines.append("Stack Trace:")
    error_lines.append(traceback.format_exc())
    comprehensive_error_logger.error("\n".join(error_lines))

# Global counters for process summary
http_request_count = 0
parsing_operation_count = 0
process_start_time = None

# --- Load proxies from file ---
def load_proxies_from_file():
    """Load proxies from proxies.txt file"""
    proxies = []
    proxy_file = os.path.join(os.path.dirname(__file__), "proxies.txt")
    try:
        with open(proxy_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Format: ip:port:username:password
                    parts = line.split(":")
                    if len(parts) == 4:
                        ip, port, username, password = parts
                        proxy_dict = {
                            "http": f"http://{username}:{password}@{ip}:{port}",
                            "https": f"http://{username}:{password}@{ip}:{port}"
                        }
                        proxies.append(proxy_dict)
        print(f"[PROXY] Loaded {len(proxies)} proxies from {proxy_file}")
        return proxies
    except Exception as e:
        print(f"[PROXY ERROR] Could not load proxies: {e}")
        return []

# --- Cycling system variables ---
LOADED_PROXIES = load_proxies_from_file()
current_proxy_index = 0
proxy_rotation_lock = threading.Lock()

# Timing for cycling system
LOCAL_SCRAPING_DURATION = 10 * 60  # 10 minutes
PROXY_SCRAPING_DURATION = 5 * 60   # 5 minutes
cycle_start_time = time.time()
is_using_local = True  # Start with local

def get_next_proxy():
    """Get the next proxy in rotation"""
    global current_proxy_index
    if not LOADED_PROXIES:
        return None
    
    with proxy_rotation_lock:
        proxy = LOADED_PROXIES[current_proxy_index]
        current_proxy_index = (current_proxy_index + 1) % len(LOADED_PROXIES)
        return proxy

def should_use_local_connection():
    """Determine if we should use local connection based on cycling schedule"""
    global cycle_start_time, is_using_local
    
    current_time = time.time()
    elapsed_time = current_time - cycle_start_time
    
    if is_using_local:
        # Currently using local for 10 minutes
        if elapsed_time >= LOCAL_SCRAPING_DURATION:
            # Switch to proxy mode
            is_using_local = False
            cycle_start_time = current_time
            print(f"[CYCLE] Switching to PROXY mode for {PROXY_SCRAPING_DURATION//60} minutes")
            return False
        return True
    else:
        # Currently using proxy for 5 minutes
        if elapsed_time >= PROXY_SCRAPING_DURATION:
            # Switch to local mode
            is_using_local = True
            cycle_start_time = current_time
            print(f"[CYCLE] Switching to LOCAL mode for {LOCAL_SCRAPING_DURATION//60} minutes")
            return True
        return False


# --- Configuration ---
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7,en-GB;q=0.6,en-US;q=0.5',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"138.0.7204.158"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.158", "Google Chrome";v="138.0.7204.158"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
}

# Function to refresh headers and cookies using Playwright
async def refresh_headers_and_cookies():
    safe_print("[INFO] Refreshing headers and cookies using Playwright...")
    token, cookies = await bearer_token_finder.get_bearer_token_and_cookies(headless=True)
    if token:
        HEADERS['authorization'] = f"Bearer {token}"
    if cookies:
        COOKIES.clear()
        COOKIES.update(cookies)
    safe_print("[INFO] Headers and cookies refreshed.")

COOKIES = {
    '__adroll_fpc': 'a2416a49f6f7378b087e7435bf007acc-1752145023646',
    '__q_state_ZB9yNHnAdpJRvvbF': 'eyJ1dWlkIjoiNWU2MTk3Y2MtOWYzMC00ZjNiLWI1YTYtNTQ5Y2E4ZGQ4ZTg0IiwiY29va2llRG9tYWluIjoicGl0Y2hib29rLmNvbSIsImFjdGl2ZVNlc3Npb25JZCI6bnVsbCwic2NyaXB0SWQiOm51bGwsIm1lc3NlbmdlckV4cGFuZGVkIjpudWxsLCJwcm9tcHREaXNtaXNzZWQiOmZhbHNlLCJjb252ZXJzYXRpb25JZCI6bnVsbH0=',
    '_biz_uid': '3d7c2eec29e64eee8d550819fc010826',
    '_fbp': 'fb.1.1752137860526.288799038843292594',
    '_hjSessionUser_77093': 'eyJpZCI6ImEzMmFjZGYzLTcwOGEtNWFmYS1hMmYyLTgyYTc2Y2ExZmNlMSIsImNyZWF0ZWQiOjE3NTIxMzc4NjA2MzEsImV4aXN0aW5nIjp0cnVlfQ==',
    '_mkto_trk': 'id:942-MYM-356&token:_mch-pitchbook.com-7ef89d5fe0e0a2da4f5c188daf40b66',
    '_zitok': '9bb2a0fb60df42f076601752145067',
    'fpid': '91da21c9c133c709ebdc67cd6369832b',
    'OptanonAlertBoxClosed': '2025-07-10T08:57:44.239Z',
    'optimizelyEndUserId': 'oeu1752137859884r0.26892967302775395',
    'optimizelySession': '0',
    'XSRF-TOKEN': '808fa5f4-35c5-4c94-bd68-06380c1692ed',
    'sa-user-id': 's%253A0-7a080047-fd7b-4849-5d1d-3005c1f46888.jzFsrPcUwlsB3Ssma%252FgfuPOH6743yWjTe%252BQDS3HzhF8',
    'sa-user-id-v2': 's%253AeggAR_17SEldHTAFwfRoiA.xK%252FMqBdap7RHXEAl8Zw1VOL3i3RxL%252F2MD1j2abWWMG0',
    'sa-user-id-v3': 's%253AAQAKIK5CCLecp_FZzaCJFBBYk6U9z05C22QRVePC79wM-dOKEHwYAiD8ieO1BjoEKG_H40IEdXp5xA.tAVI0ZYldC9PdMaM5QB1TZJHQdbBymx8dxUts7AdJ0o',
    'cf_clearance': 'frbpj0WEyh6Ih1Jhq2fN7YPIQq8zgcwW8UK6BCwxYfY-1752223874-1.2.1.1-RqA8XalRzKzhbJ8qZ8MyDO3FpZDWaAq0xU6sVLmuuaw3l7QQN9LMOFzJuKyjXm4gVfqhq7r9jjn0VO9awT8sMiSSWT5d7sbYUetVgOJn9QA.GFfoJX6SkMuyiPROSVFaKlbTDtxu5TpP88Mw5l2vpwDOfidRmOgy4ejGsmjcKOgUqhG9ZRkU72eyTi7mdoB_ZkXNRZObRPSg9lKSCsqdvVup0JLIrzEFMypBOqA60fo',
    '_biz_ABTestA': '%5B-1762637899%2C1429036774%2C-147638774%5D',
    '_gcl_au': '1.1.1967049274.1752137861.1471010926.1753003887.1753003886',
    '_biz_flagsA': '%7B%22Version%22%3A1%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%2C%22Mkto%22%3A%221%22%2C%22Frm%22%3A%221%22%7D',
    'USESSIONID': 'NTk3OGVjNDctOTJiOS00YTgyLTkyZmQtM2QyMDBhNzA4NTc2',
    '_gid': 'GA1.2.1754270387.1753172145',
    'highDensity': 'true',
    '_hjSession_77093': 'eyJpZCI6ImViYTQyODEzLWQyNDItNDA5Ni04MDY0LTllZjBhZGYxMjMzZiIsImMiOjE3NTMxNzIxNDQ5NTYsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=',
    '_clck': 'ibk904%7C2%7Cfxt%7C0%7C2017',
    'sa-r-source': 'cosmetic-prozac-vault-very.trycloudflare.com',
    'sa-r-date': '2025-07-22T08:23:20.525Z',
    '__cf_bm': 'ITTSXJoTZyrSkXfY.cH8mIBg3ZHkuGqp9GFY2Kn80sE-1753173774-1.0.1.1-eN.BYUF5L3hdRYezBSSp2zqFb3gmwD1cPc4Y3F0gqSNtVtIwZcq_s4uv.GWJFjQCYwBVmr_Myk3Y0NsGSOsw3njNbyl8gNlUFl4Qe6zrZV8',
    '_biz_nA': '244',
    'sourceType': 'DIRECT',
    'sourceUrl': '',
    '_gat': '1',
    '_biz_pendingA': '%5B%5D',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Jul+22+2025+14%3A12%3A55+GMT%2B0530+(India+Standard+Time)&version=202504.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0004%3A1%2CC0002%3A1%2CC0003%3A1&geolocation=IN%3BGJ&AwaitingReconsent=false',
    '_ga': 'GA1.1.1782411264.1752137860',
    '_rdt_uuid': '1752137864446.f758cd68-62f8-4742-b0fa-3f95dec21d99',
    '_uetsid': '113fdc8066d411f0bb62c9966b109c6e',
    '_uetvid': 'f178a0405d6b11f09b84a1481d37c5e5',
    '__ar_v4': '3LYSOYTQT5G77JYZ2SLG34%3A20250710%3A1%7C2HN5SB32U5B7RKLIF5GUQE%3A20250709%3A161%7C5S2POJ2OE5GPZNGPI6HCQ6%3A20250709%3A161%7CVVXWAEVNXVBAVDH5T75XAQ%3A20250709%3A150%7CABSQS3OE7JFQRP56UD4C6C%3A20250710%3A9%7C3E3FHCM2ZVDZPBG3A3WOZD%3A20250719%3A1',
    '_ga_DS3177N6CK': 'GS2.1.s1753172145$o39$g1$t1753173776$j59$l0$h0',
    '_clsk': '169b6bk%7C1753173777438%7C11%7C1%7Ce.clarity.ms%2Fcollect'
}

# --- Category list to scrape ---
CATEGORIES = [
    # "prodaja-kuca",
    # "iznajmljivanje-kuca",
    # "prodaja-stanova",
    # "iznajmljivanje-stanova",
    # "prodaja-zemljista",
    # "zakup-zemljista",
    # "prodaja-poslovnih-prostora",
    # "iznajmljivanje-poslovnih-prostora",
    "novogradnja",                                    # currently running only for this category for testing. uncomment all to test for full run.
    # "vikendice",
    # "montazni-objekti",
    # "prodaja-luksuznih-nekretnina",
    # "iznajmljivanje-luksuznih-nekretnina",
    # "prodaja-garaza",
    # "iznajmljivanje-garaza",
    # "iznajmljivanje-soba",
    # "cimeri"
]


today_str = datetime.now().strftime("%Y-%m-%d")
BACKEND_DIR = os.path.join(os.path.dirname(__file__), "backend")
CATEGORIES_DIR = os.path.join(BACKEND_DIR, "categories")
CATEGORIES_HTMLS_DIR = os.path.join(CATEGORIES_DIR, "htmls")
CATEGORIES_LOGS_DIR = os.path.join(CATEGORIES_DIR, "logs")
CATEGORIES_TREE_DIR = os.path.join(CATEGORIES_DIR, "tree_jsons")
os.makedirs(CATEGORIES_HTMLS_DIR, exist_ok=True)
os.makedirs(CATEGORIES_LOGS_DIR, exist_ok=True)
os.makedirs(CATEGORIES_TREE_DIR, exist_ok=True)


def is_proxy_forbidden(response_text):
    """Check if proxy response indicates blocking or forbidden access"""
    if not response_text:
        return False
    forbidden_signals = ["forbidden", "insufficient flow", "errorMsg"]
    return any(sig in response_text.lower() for sig in forbidden_signals)

# --- Concurrency argument ---
def get_concurrency():
    parser = argparse.ArgumentParser()
    parser.add_argument('--concurrency', type=int, default=10, help='Number of concurrent requests (default: 4)')
    args, _ = parser.parse_known_args()
    return args.concurrency

SEM = asyncio.Semaphore(get_concurrency())

class CategoryLogger:
    def __init__(self, log_file_path=None):
        self.lines = []
        self.stack = []  # Track (is_last_child) for each level
        self.current_names = []  # Track category/subcategory path for folder structure
        self.log_file_path = log_file_path
        self._log_file = None
        if log_file_path:
            self._log_file = open(log_file_path, "a", encoding="utf-8")

    def log(self, name, leaf_count=None, is_last=False):
        # Build prefix with tree symbols
        prefix = ''
        for is_last_level in self.stack[:-1]:
            prefix += '    ' if is_last_level else '│   '
        if self.stack:
            prefix += '└── ' if is_last else '├── '
        leaf_info = f" ({leaf_count} leafs)" if leaf_count is not None else ""
        line = f"{prefix}{name}{leaf_info}"
        self.lines.append(line)
        # Print Unicode safely in all terminals
        try:
            import sys
            sys.stdout.buffer.write((line + "\n").encode("utf-8"))
            sys.stdout.flush()
        except Exception:
            # Fallback to print (may error in some terminals)
            print(line)
        if self._log_file:
            self._log_file.write(line + "\n")
            self._log_file.flush()

    def enter(self, is_last):
        self.stack.append(is_last)

    def exit(self):
        if self.stack:
            self.stack.pop()

    def print_log(self):
        log_text = "\n".join(self.lines)
        try:
            import sys
            sys.stdout.buffer.write((log_text + "\n").encode("utf-8"))
            sys.stdout.flush()
        except Exception:
            print(log_text)
        if self._log_file:
            self._log_file.write(log_text + "\n")
            self._log_file.flush()

    def close(self):
        if self._log_file:
            self._log_file.close()
            self._log_file = None

def extract_category_links_from_html(html):
    try:
        # Anti-bot detection: only if selector fails AND anti-bot keyword is present
        antibot_signals = [
            # 'captcha',
            # 'prove you are human',
            # 'robot check',
            # 'cloudflare',
            # 'unusual traffic',
            # 'access denied',
            # 'are you a human',
            # 'please verify',
            # 'security check',
            # 'blocked',
        ]
        soup = BeautifulSoup(html, "html.parser")
        categories_div = soup.find("div", class_="entity-list-categories")
        if not categories_div:
            lower_html = html.lower()
            for signal in antibot_signals:
                if signal in lower_html:
                    log_parsing_failure("extract_category_links", "Anti-bot detection", html[:1000])
                    return 'ANTIBOT_DETECTED'
            # Log successful empty parsing
            log_parsing_completion("extract_category_links", 0, "leaf_category")
            return []  # treat as leaf if no subcategories and no anti-bot
        links = []
        # Find all li elements for categories (works for both single and multi-column)
        for li in categories_div.find_all("li", class_=["CategoryListing-topCategoryItem", "CategoryListing-topCategoryItemFauxAnchor"]):
            a = li.find("a", class_="CategoryListing-topCategoryLink")
            if a and a.get("href"):
                name = a.get_text(strip=True)
                href = a.get("href")
                links.append({"name": name, "url": href})
        
        # Log successful parsing
        log_parsing_completion("extract_category_links", len(links), "category_list")
        return links
        
    except Exception as e:
        log_parsing_failure("extract_category_links", str(e), html[:1000])
        return []

async def fetch_html(session, url):
    """Fetch HTML with cycling between local and proxy connections"""
    timeout = 10  # seconds
    
    use_local = should_use_local_connection()
    
    try:
        if use_local:
            print(f"[LOCAL] Fetching {url}")
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                timeout=timeout
            )
        else:
            # Use proxy from loaded proxy list
            current_proxy = get_next_proxy()
            if current_proxy:
                proxy_info = current_proxy["http"].split("@")[1] if "@" in current_proxy["http"] else "unknown"
                print(f"[PROXY] Fetching {url} via {proxy_info}")
                response = await asyncio.wait_for(
                    session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=current_proxy),
                    timeout=timeout
                )
            else:
                # No proxies available, fallback to local
                print(f"[LOCAL FALLBACK] No proxies available, using local for {url}")
                response = await asyncio.wait_for(
                    session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                    timeout=timeout
                )
        
        response.raise_for_status()
        text = getattr(response, "text", "")
        
        # Log HTTP completion
        log_http_completion(url, response.status_code, len(text), "proxy" if not use_local else "local")
        
        # Check for proxy-specific blocks
        if is_proxy_forbidden(text):
            if not use_local:
                print(f"[PROXY BLOCKED] Proxy blocked, trying next proxy...")
                # Try next proxy
                next_proxy = get_next_proxy()
                if next_proxy:
                    proxy_info = next_proxy["http"].split("@")[1] if "@" in next_proxy["http"] else "unknown"
                    print(f"[PROXY RETRY] Trying next proxy {proxy_info}")
                    response = await asyncio.wait_for(
                        session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=next_proxy),
                        timeout=timeout
                    )
                    response.raise_for_status()
                    text = getattr(response, "text", "")
                    
                    # Log retry HTTP completion
                    log_http_completion(url, response.status_code, len(text), "proxy_retry")
                    
                    if is_proxy_forbidden(text):
                        print(f"[PROXY FAILED] Next proxy also blocked, continuing with current response")
                else:
                    print(f"[PROXY EXHAUSTED] No more proxies available")
        
        return text
        
    except Exception as e:
        log_http_failure(url, str(e), 0)
        safe_print(f"[fetch_html] Error fetching {url}: {e}")
        
        if not use_local:
            print(f"[PROXY ERROR] Exception with proxy, trying next proxy...")
            # Try next proxy on error
            next_proxy = get_next_proxy()
            if next_proxy:
                try:
                    proxy_info = next_proxy["http"].split("@")[1] if "@" in next_proxy["http"] else "unknown"
                    print(f"[PROXY RETRY] Trying next proxy {proxy_info}")
                    response = await asyncio.wait_for(
                        session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=next_proxy),
                        timeout=timeout
                    )
                    response.raise_for_status()
                    text = getattr(response, "text", "")
                    
                    # Log successful retry
                    log_http_completion(url, response.status_code, len(text), "proxy_retry_success")
                    
                    return text
                except Exception as e2:
                    log_http_failure(url, str(e2), 0, "proxy_retry_failed")
                    safe_print(f"[PROXY RETRY FAILED] Next proxy also failed: {e2}")
        
        return None

async def fetch_and_save_html(url, out_file, log_dir):
    """Fetch and save HTML with cycling between local and proxy connections"""
    import time
    t0 = time.time()
    
    async with AsyncSession() as client:
        try:
            use_local = should_use_local_connection()
            
            if use_local:
                print(f"[LOCAL] Fetching and saving {url}")
                response = await client.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110")
            else:
                # Use proxy from loaded proxy list
                current_proxy = get_next_proxy()
                if current_proxy:
                    proxy_info = current_proxy["http"].split("@")[1] if "@" in current_proxy["http"] else "unknown"
                    print(f"[PROXY] Fetching and saving {url} via {proxy_info}")
                    response = await asyncio.wait_for(
                        client.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=current_proxy),
                        timeout=10
                    )
                else:
                    # No proxies available, fallback to local
                    print(f"[LOCAL FALLBACK] No proxies available, using local for {url}")
                    response = await client.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110")
            
            response.raise_for_status()
            text = getattr(response, "text", "")
            
            # Check for proxy-specific blocks
            if is_proxy_forbidden(text):
                if not use_local:
                    print(f"[PROXY BLOCKED] Proxy blocked, trying next proxy...")
                    # Try next proxy
                    next_proxy = get_next_proxy()
                    if next_proxy:
                        proxy_info = next_proxy["http"].split("@")[1] if "@" in next_proxy["http"] else "unknown"
                        print(f"[PROXY RETRY] Trying next proxy {proxy_info}")
                        response = await asyncio.wait_for(
                            client.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=next_proxy),
                            timeout=10
                        )
                        response.raise_for_status()
                        text = getattr(response, "text", "")
            
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(text)
            status = "SUCCESS"
            
        except Exception as e:
            safe_print(f"Error scraping {url}: {e}")
            
            if not should_use_local_connection():
                print(f"[PROXY ERROR] Exception with proxy, trying next proxy...")
                # Try next proxy on error
                next_proxy = get_next_proxy()
                if next_proxy:
                    try:
                        proxy_info = next_proxy["http"].split("@")[1] if "@" in next_proxy["http"] else "unknown"
                        print(f"[PROXY RETRY] Trying next proxy {proxy_info}")
                        response = await asyncio.wait_for(
                            client.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=next_proxy),
                            timeout=10
                        )
                        response.raise_for_status()
                        with open(out_file, "w", encoding="utf-8") as f:
                            f.write(response.text)
                        status = "SUCCESS"
                    except Exception as e2:
                        safe_print(f"Error scraping with next proxy: {e2}")
                        status = "FAILED"
                else:
                    status = "FAILED"
            else:
                status = "FAILED"
        
        duration_ms = int((time.time() - t0) * 1000)
        timestamp = datetime.now().isoformat()
        log_line = f"{timestamp} HTML EXTRACTION {os.path.basename(out_file)} {status} {duration_ms}ms\n"
        log_file = os.path.join(log_dir, os.path.basename(out_file).replace('.html', '.log'))
        with open(log_file, "w", encoding="utf-8") as logf:
            logf.write(log_line)
        return status == "SUCCESS"

global_subcat_counter = 0
SLEEP_AFTER_SUBCATS = 15
SLEEP_DURATION = 1  # 1 minute in seconds

async def build_category_tree(session, url, name, depth=0, max_depth=10, logger=None, main_category=None):
    if logger is None:
        logger = CategoryLogger()
    indent = '  ' * depth
    safe_print(f"{indent}Processing: {name} (depth={depth})")
    if depth > max_depth:
        logger.log(name, is_last=True)
        return {"name": name, "url": url, "children": []}
    # Fetch HTML
    # Build tree-like folder path for HTML saving that mirrors the website's category/subcategory hierarchy
    tree_html_base = os.path.join(CATEGORIES_HTMLS_DIR, 'tree_htmls')
    os.makedirs(tree_html_base, exist_ok=True)
    # Clean up names for filesystem
    def clean_name(n):
        return ''.join(c for c in n if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
    # Use logger.stack to build the hierarchy path (category, subcategory, ...)
    # logger.stack contains is_last flags, but we need the actual names for the path
    # We'll build the path from the current recursion: pass down a path argument
    # Instead, reconstruct the path from parent names
    # We'll use an additional argument: parent_names
    parent_names = getattr(logger, 'current_names', []) if hasattr(logger, 'current_names') else []
    # For root, parent_names is empty; for subcategories, it's the chain of parent names
    full_path = parent_names + [name]
    html_folder = os.path.join(tree_html_base, *(clean_name(n) for n in full_path))
    os.makedirs(html_folder, exist_ok=True)
    html_file_path = os.path.join(html_folder, f"{clean_name(name)}.html")
    # Determine main_category for leaf URL file naming
    if main_category is None:
        # If not set, use the first name in parent_names or current name
        main_category = parent_names[0] if parent_names else name
    # Block detection and retry logic
    def is_block_page(html):
        if not html:
            return False
        # Only match <title>ShieldSquare Captcha</title>
        import re
        match = re.search(r'<title>\s*ShieldSquare Captcha\s*</title>', html, re.IGNORECASE)
        return bool(match)

    html = None
    async with SEM:
        html = await fetch_html(session, url)
    if is_block_page(html):
        safe_print(f"[BLOCK DETECTED] {name} ({url}) - Exiting script and pausing for 1 minute...")
        await asyncio.sleep(60)
        sys.exit(99)  # Custom exit code for blockage

    # Save HTML for every node, even if it's None or error response
    try:
        with open(html_file_path, "w", encoding="utf-8") as f:
            f.write(html if html is not None else "")
    except Exception as e:
        safe_print(f"[ERROR] Could not save HTML for {name}: {e}")
    if not html or is_block_page(html):
        # Try to get error code from previous fetch attempt (if available)
        error_code = getattr(session, 'last_status', None)
        error_reason = getattr(session, 'last_reason', None)
        msg = f"[ERROR] Failed to fetch: {name} ({url})"
        if error_code:
            msg += f" [HTTP {error_code} - {error_reason}]"
        safe_print(msg)
        if logger:
            logger.log(msg)
        logger.log(name, is_last=True)
        return {"name": name, "url": url, "children": []}
    children = extract_category_links_from_html(html)
    if children == 'ANTIBOT_DETECTED':
        logger.log(f"[ANTIBOT BLOCKED] {name}", is_last=True)
        safe_print(f"{indent}[ANTIBOT DETECTED] {name} ({url}) - Skipping this branch!")
        return {"name": name, "url": url, "children": [], "antibot": True}
    if not children:
        msg = f"{name} ({url}) - LEAF-NODE"
        safe_print(msg)
        if logger:
            logger.log(msg)
        safe_print(f"{indent}Leaf: {name}")
        logger.log(name, leaf_count=1, is_last=True)
        # Save leaf URL to category-specific file in leaf_urls folder
        leaf_urls_dir = os.path.join(os.path.dirname(__file__), "backend", "categories", "leaf_urls")
        os.makedirs(leaf_urls_dir, exist_ok=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        leaf_url_file = os.path.join(leaf_urls_dir, f"{clean_name(main_category)}_leaf_urls_{today_str}.txt")
        with open(leaf_url_file, "a", encoding="utf-8") as f:
            f.write(f"{url}\n")
        return {"name": name, "url": url, "children": []}
    tree_children = []
    batch_size = 20
    global global_subcat_counter
    # Track parent names for folder structure
    if not hasattr(logger, 'current_names'):
        logger.current_names = []
    logger.current_names.append(name)

    # Date-based checkpoint file for this node
    checkpoint_folder = html_folder
    today_str = datetime.now().strftime("%Y-%m-%d")
    checkpoint_file = os.path.join(checkpoint_folder, f"checkpoint_{today_str}.json")
    completed_subcats = set()
    # Remove old checkpoint files (not for today)
    for fname in os.listdir(checkpoint_folder):
        if fname.startswith("checkpoint_") and not fname.endswith(f"{today_str}.json"):
            try:
                os.remove(os.path.join(checkpoint_folder, fname))
            except Exception:
                pass
    # Load today's checkpoint if exists
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                completed_subcats = set(json.load(f))
        except Exception:
            completed_subcats = set()

    for idx, child in enumerate(children):
        subcat_name = child["name"]
        if subcat_name in completed_subcats:
            safe_print(f"[CHECKPOINT] Skipping already completed subcategory: {subcat_name}")
            continue
        is_last = (idx == len(children) - 1)
        safe_print(f"{indent}  Subcategory: {subcat_name} (is_last={is_last})")
        logger.log(subcat_name, is_last=is_last)
        # Only count subcategories at depth==0 (first level under root)
        if depth == 0:
            global_subcat_counter += 1
            if global_subcat_counter % SLEEP_AFTER_SUBCATS == 0:
                msg = f"[RATE LIMIT] Sleeping for {SLEEP_DURATION//60} minutes after {global_subcat_counter} subcategories..."
                safe_print(msg)
                logger.log(msg)
                await asyncio.sleep(SLEEP_DURATION)
        logger.enter(is_last)
        # Pass down the parent_names chain for folder structure
        subtree = await build_category_tree(session, child["url"], child["name"], depth+1, max_depth, logger, main_category=main_category)
        tree_children.append(subtree)
        logger.exit()
        # Save checkpoint after each successful subcategory (date-based)
        completed_subcats.add(subcat_name)
        try:
            with open(checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(list(completed_subcats), f)
        except Exception as e:
            safe_print(f"[ERROR] Could not save checkpoint for {name}: {e}")
    logger.current_names.pop()
    return {"name": name, "url": url, "children": tree_children}



async def main_category_tree_scrape():
    # Setup comprehensive logging
    setup_comprehensive_logging()
    
    # Log process start
    start_time = time.time()
    log_process_start("category_tree_scraping")
    
    # Checkpoint setup
    CHECKPOINTS_DIR = os.path.join(os.path.dirname(__file__), "checkpoints")
    os.makedirs(CHECKPOINTS_DIR, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, f"category_tree_checkpoint_{today_str}.json")
    # Remove old checkpoint files
    old_checkpoints = [os.path.join(CHECKPOINTS_DIR, f) for f in os.listdir(CHECKPOINTS_DIR)
                      if f.startswith("category_tree_checkpoint_") and not f.startswith(f"category_tree_checkpoint_{today_str}") and f.endswith(".json")]
    for cp in old_checkpoints:
        try:
            os.remove(cp)
        except Exception as e:
            print(f"Could not delete old checkpoint {cp}: {e}")
    # Load today's checkpoint
    completed = set()
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                completed = set(json.load(f))
        except Exception:
            completed = set()
    all_trees = {}
    for cat in CATEGORIES:
        if cat in completed:
            safe_print(f"[CHECKPOINT] Skipping already completed category: {cat}")
            continue
        url = f"https://www.njuskalo.hr/{cat}"
        html_file = os.path.join(CATEGORIES_HTMLS_DIR, f"{cat}_{today_str}.html")
        tree_file = os.path.join(CATEGORIES_TREE_DIR, f"{cat}_tree_{today_str}.json")
        log_file = os.path.join(CATEGORIES_LOGS_DIR, f"{cat}_{today_str}.log")
        logger = CategoryLogger(log_file_path=log_file)
        if not os.path.exists(html_file):
            safe_print(f"Fetching first page for {cat} and saving as {html_file}...")
            await fetch_and_save_html(url, html_file, CATEGORIES_LOGS_DIR)
        if not os.path.exists(tree_file):
            with open(html_file, "r", encoding="utf-8") as f:
                html = f.read()
            root_links = extract_category_links_from_html(html)
            # Debug and error handling for root_links
            if not isinstance(root_links, list) or (root_links and not isinstance(root_links[0], dict)):
                safe_print(f"[ERROR] Unexpected root_links structure for category '{cat}': {root_links}")
                logger.log(f"[ERROR] Unexpected root_links structure for category '{cat}': {root_links}")
                logger.close()
                continue
            tree = []
            async with AsyncSession() as session:
                for idx, root_cat in enumerate(root_links):
                    is_last = (idx == len(root_links) - 1)
                    logger.log(root_cat["name"], is_last=is_last)
                    logger.enter(is_last)
                    subtree = await build_category_tree(session, root_cat["url"], root_cat["name"], logger=logger, main_category=cat)
                    tree.append(subtree)
                    logger.exit()
            with open(tree_file, "w", encoding="utf-8") as f:
                json.dump(tree, f, ensure_ascii=False, indent=2)
            safe_print(f"Tree for {cat} saved to {tree_file}")
        else:
            safe_print(f"Tree for {cat} already exists, skipping.")
        with open(tree_file, "r", encoding="utf-8") as f:
            all_trees[cat] = json.load(f)
        logger.close()
        # Save checkpoint after each category
        completed.add(cat)
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(list(completed), f)
    merged_tree_file = os.path.join(CATEGORIES_TREE_DIR, f"category_tree_{today_str}.json")
    with open(merged_tree_file, "w", encoding="utf-8") as f:
        json.dump(all_trees, f, ensure_ascii=False, indent=2)
    safe_print(f"\nMerged category tree saved to {merged_tree_file}")
    
    # Log process end
    log_process_end("category_tree_scraping", start_time)
    
    safe_print("\nCategory Tree Structure:\n")
    # Optionally print the last logger's log (for the last category)
    if 'logger' in locals():
        logger.print_log()

if __name__ == "__main__":
    import aiofiles
    safe_print(f"[INFO] Using concurrency: {get_concurrency()}")
    try:
        asyncio.run(main_category_tree_scrape())
        sys.exit(0)  # Normal completion
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        safe_print(f"[ERROR] Unhandled exception: {e}")
        sys.exit(1)