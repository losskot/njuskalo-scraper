#!/usr/bin/env python3
"""
scrape_and_parse.py — Unified scraper: download HTML → parse in-memory → fetch phone → insert into DB.

Replaces: scrape_leaf_entries.py + parser_ultrafast.py + fetch_phones_from_api.py

Usage:
    python scrape_and_parse.py [--restart]
"""
import asyncio
import importlib.util
import json
import logging
import os
import random
import re
import sys
import threading
import time
from datetime import datetime

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from db import (
    get_db, get_archive_db, upsert_listing, store_html, listing_exists,
    get_checkpoint, set_checkpoint, log_event, SCALAR_FIELDS, LIST_FIELDS,
)

# ─── Config ──────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONCURRENT_ENTRIES = 6

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scrape_and_parse")

# ─── Import Playwright token/cookie fetcher ──────────────────────────────────

spec = importlib.util.spec_from_file_location(
    "bearer_token_finder", os.path.join(SCRIPT_DIR, "bearer_token_finder.py")
)
bearer_token_finder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bearer_token_finder)

# ─── Import Azure proxy client ───────────────────────────────────────────────

from azure_proxy_client import AzureProxyClient
azure_proxy_client = AzureProxyClient()

# ─── Proxy loading and rotation ──────────────────────────────────────────────

def load_proxies_from_file():
    proxies = []
    proxy_file = os.path.join(SCRIPT_DIR, "proxies.txt")
    try:
        with open(proxy_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split(":")
                    if len(parts) == 4:
                        ip, port, username, password = parts
                        proxy_dict = {
                            "http": f"http://{username}:{password}@{ip}:{port}",
                            "https": f"http://{username}:{password}@{ip}:{port}",
                        }
                        proxies.append(proxy_dict)
        log.info(f"Loaded {len(proxies)} proxies")
    except Exception as e:
        log.warning(f"Could not load proxies: {e}")
    return proxies


LOADED_PROXIES = load_proxies_from_file()
_proxy_index = 0
_proxy_lock = threading.Lock()

LOCAL_DURATION = 10 * 60
PROXY_DURATION = 5 * 60
AZURE_DURATION = 5 * 60
_cycle_start = time.time()
_cycle_mode = "local"


def get_next_proxy():
    global _proxy_index
    if not LOADED_PROXIES:
        return None
    with _proxy_lock:
        p = LOADED_PROXIES[_proxy_index]
        _proxy_index = (_proxy_index + 1) % len(LOADED_PROXIES)
        return p


def get_connection_mode():
    global _cycle_start, _cycle_mode
    elapsed = time.time() - _cycle_start
    if _cycle_mode == "local" and elapsed >= LOCAL_DURATION:
        _cycle_mode = "proxy"
        _cycle_start = time.time()
        log.info("Switching to PROXY mode")
    elif _cycle_mode == "proxy" and elapsed >= PROXY_DURATION:
        if azure_proxy_client.available:
            _cycle_mode = "azure"
            log.info("Switching to AZURE mode")
        else:
            _cycle_mode = "local"
            log.info("Switching to LOCAL mode")
        _cycle_start = time.time()
    elif _cycle_mode == "azure" and elapsed >= AZURE_DURATION:
        _cycle_mode = "local"
        _cycle_start = time.time()
        log.info("Switching to LOCAL mode")
    return _cycle_mode


# ─── Headers ──────────────────────────────────────────────────────────────────

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-IN,en;q=0.9,hi-IN;q=0.8,hi;q=0.7,en-GB;q=0.6,en-US;q=0.5",
    "cache-control": "no-cache",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    ),
}
COOKIES = {}


# ─── Anti-bot / Fetch ─────────────────────────────────────────────────────────

class AntibotExit(Exception):
    pass


def is_blocked(text):
    if not text:
        return False
    if re.search(r"<title>\s*ShieldSquare Captcha\s*</title>", text, re.IGNORECASE):
        return True
    forbidden = ["forbidden", "insufficient flow", "errormsg"]
    return any(s in text.lower() for s in forbidden)


def extract_ad_id_from_url(url):
    m = re.search(r"-(\d+)$", url)
    return m.group(1) if m else None


async def refresh_headers_and_cookies():
    log.info("Refreshing bearer token and cookies via Playwright...")
    token, cookies = await bearer_token_finder.get_bearer_token_and_cookies(headless=True)
    if token:
        HEADERS["authorization"] = f"Bearer {token}"
    if cookies:
        COOKIES.clear()
        COOKIES.update(cookies)
    log.info("Token refreshed.")
    return token, cookies


async def fetch_html(session, url):
    """Fetch HTML with connection mode cycling and anti-bot handling."""
    timeout = 10
    mode = get_connection_mode()

    try:
        if mode == "azure" and azure_proxy_client.available:
            status, body, region = await azure_proxy_client.fetch(url, headers=HEADERS, cookies=COOKIES)
            text = body
        elif mode == "proxy":
            proxy = get_next_proxy()
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110", proxies=proxy),
                timeout=timeout,
            )
            response.raise_for_status()
            text = getattr(response, "text", "")
        else:
            response = await asyncio.wait_for(
                session.get(url, headers=HEADERS, cookies=COOKIES, impersonate="chrome110"),
                timeout=timeout,
            )
            response.raise_for_status()
            text = getattr(response, "text", "")

        # Block detection with fallbacks
        if is_blocked(text):
            if mode != "azure" and azure_proxy_client.available:
                _, text, _ = await azure_proxy_client.fetch(url, headers=HEADERS, cookies=COOKIES)
            if is_blocked(text):
                log.warning(f"Blocked on {extract_ad_id_from_url(url)} — pausing 60s")
                await asyncio.sleep(60)
                raise AntibotExit()

        return text

    except AntibotExit:
        raise
    except Exception as e:
        ad_id = extract_ad_id_from_url(url) or url[-20:]
        log.error(f"Fetch error {ad_id}: {e}")
        # Azure fallback on error
        if mode != "azure" and azure_proxy_client.available:
            try:
                _, text, _ = await azure_proxy_client.fetch(url, headers=HEADERS, cookies=COOKIES)
                if not is_blocked(text):
                    return text
            except Exception:
                pass
        return None


# ─── Phone API ────────────────────────────────────────────────────────────────

async def fetch_phone(session, ad_id, bearer_token, cookies):
    """Fetch phone number for a listing via Njuskalo API."""
    url = f"https://www.njuskalo.hr/ccapi/v4/phone-numbers/ad/{ad_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": HEADERS["user-agent"],
        "Referer": f"https://www.njuskalo.hr/nekretnine/*-oglas-{ad_id}",
    }
    try:
        resp = await session.get(url, headers=headers, cookies=cookies, timeout=15)
        if resp.status_code == 401:
            return "REFRESH_TOKEN"
        resp.raise_for_status()
        data = resp.json()
        numbers = [
            n["formattedNumber"]
            for n in data.get("data", {}).get("attributes", {}).get("numbers", [])
            if n.get("formattedNumber")
        ]
        return numbers[0].strip() if numbers else None
    except Exception as e:
        if "401" in str(e):
            return "REFRESH_TOKEN"
        return None


# ─── HTML Parsing (from parser_ultrafast.py) ──────────────────────────────────

LAT_LNG_PATTERN = re.compile(r'"lat":([\d\.-]+),"lng":([\d\.-]+),"approximate":(true|false)')


def parse_listing_html(html, ad_id):
    """
    Parse a single listing HTML page into a structured dict.
    Returns dict compatible with upsert_listing().
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    podaci = {"id": ad_id}

    # Canonical link
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        podaci["link"] = canonical["href"]

    # Location from script tags
    for script in soup.find_all("script"):
        if script.string:
            match = LAT_LNG_PATTERN.search(script.string)
            if match:
                podaci["lokacija"] = {
                    "lat": float(match.group(1)),
                    "lng": float(match.group(2)),
                    "approximate": match.group(3) == "true",
                }
                break

    # Title
    title_tag = soup.find("title")
    podaci["naslov"] = title_tag.get_text(strip=True) if title_tag else None

    # Price
    price_tag = soup.select_one("dl.ClassifiedDetailSummary-priceRow dd.ClassifiedDetailSummary-priceDomestic")
    podaci["cijena"] = price_tag.get_text(strip=True) if price_tag else None

    # Basic details
    info_section = soup.select_one("div.ClassifiedDetailBasicDetails dl.ClassifiedDetailBasicDetails-list")
    if info_section:
        dt_tags = info_section.find_all("dt")
        dd_tags = info_section.find_all("dd")
        for dt, dd in zip(dt_tags, dd_tags):
            key_span = dt.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
            val_span = dd.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
            if key_span and val_span:
                k = key_span.get_text(strip=True)
                v = val_span.get_text(strip=True)
                if k and v:
                    podaci[k] = v

    # Description
    desc_tag = soup.find("div", class_="ClassifiedDetailDescription-text")
    podaci["opis"] = desc_tag.get_text(" ", strip=True).replace("\n", " ") if desc_tag else None

    # Additional property groups (list fields)
    for sekcija in soup.select("section.ClassifiedDetailPropertyGroups-group"):
        naslov = sekcija.find("h3", class_="ClassifiedDetailPropertyGroups-groupTitle")
        if not naslov:
            continue
        ime = naslov.get_text(strip=True)
        stavke = [
            li.get_text(strip=True)
            for li in sekcija.select("li.ClassifiedDetailPropertyGroups-groupListItem")
            if li.get_text(strip=True)
        ]
        if stavke:
            podaci[ime] = stavke

    # Owner details
    owner = soup.select_one("div.ClassifiedDetailOwnerDetails")
    if owner:
        ag = owner.select_one("h2.ClassifiedDetailOwnerDetails-title a")
        if ag:
            podaci["naziv_agencije"] = ag.get_text(strip=True)
        web = owner.select_one("a[href^='http']:not([href^='mailto'])")
        if web:
            podaci["profil_agencije"] = web.get("href")
        email = owner.select_one("a[href^='mailto']")
        if email:
            podaci["email_agencije"] = email.get_text(strip=True)
        addr = owner.select_one("li.ClassifiedDetailOwnerDetails-contactEntry i[aria-label='Adresa']")
        if addr and addr.parent:
            podaci["adresa_agencije"] = addr.parent.get_text(strip=True).replace("Adresa: ", "")

    # System details
    sys_details = soup.select_one("dl.ClassifiedDetailSystemDetails-list")
    if sys_details:
        for dt, dd in zip(sys_details.find_all("dt"), sys_details.find_all("dd")):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key == "Oglas objavljen":
                podaci["oglas_objavljen"] = val
            elif key == "Do isteka još":
                podaci["do_isteka"] = val
            elif key == "Oglas prikazan":
                podaci["oglas_prikazan"] = val

    # Images
    image_tags = soup.select("li[data-media-type='image']")
    podaci["slike"] = [t.get("data-large-image-url") for t in image_tags if t.get("data-large-image-url")]

    return podaci


# ─── Leaf URL pagination (extract entry URLs from category pages) ─────────────

def extract_entry_urls(html):
    """Extract individual listing URLs from a leaf category page."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        urls = []
        valid_titles = {"Njuškalo oglasi", "Sniff ads"}
        for section in soup.find_all("section", class_=lambda c: c and "EntityList" in c):
            h2 = section.find("h2", class_=lambda c: c and "EntityList-groupTitle" in c)
            if not h2:
                continue
            if h2.get_text(strip=True) not in valid_titles:
                continue
            ul = section.find("ul", class_=lambda c: c and "EntityList-items" in c)
            if not ul:
                continue
            for li in ul.find_all("li", class_=lambda c: c and "EntityList-item" in c):
                a = li.find("a", class_="link")
                if a and a.get("href"):
                    href = a["href"]
                    if href.startswith("/"):
                        href = "https://www.njuskalo.hr" + href
                    urls.append(href)
        return list(set(urls))
    except Exception:
        return []


# ─── Main scraping logic ─────────────────────────────────────────────────────

async def process_entry(session, entry_url, bearer_token, db_conn):
    """Download one listing, parse it, fetch phone, insert into DB."""
    ad_id = extract_ad_id_from_url(entry_url)
    if not ad_id:
        return False

    # Skip if already in DB
    if listing_exists(db_conn, ad_id):
        return False

    # 1. Download HTML
    html = await fetch_html(session, entry_url)
    if not html:
        return False

    # 2. Parse in-memory
    parsed = parse_listing_html(html, ad_id)

    # 3. Fetch phone inline
    token = HEADERS.get("authorization", "").replace("Bearer ", "")
    if token:
        phone = await fetch_phone(session, ad_id, token, COOKIES)
        if phone == "REFRESH_TOKEN":
            new_token, new_cookies = await refresh_headers_and_cookies()
            if new_token:
                phone = await fetch_phone(session, ad_id, new_token, new_cookies)
            else:
                phone = None
        if phone and phone != "REFRESH_TOKEN":
            parsed["telefon"] = phone

    # 4. Insert into DB
    upsert_listing(db_conn, parsed)

    # 5. Store raw HTML for history
    store_html(ad_id, html)

    return True


async def process_leaf_url(session, leaf_url, bearer_token, db_conn):
    """Process one leaf URL: paginate, discover entries, scrape each."""
    entry_urls = []
    page = 1
    first_page_urls = None
    prev_page_urls = None

    while True:
        url = leaf_url if page == 1 else f"{leaf_url}?page={page}"
        html = await fetch_html(session, url)
        if not html:
            break

        # Detect redirect back to page 1
        if page > 1:
            m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE)
            if m and m.group(1).rstrip("/") == leaf_url.rstrip("/"):
                break

        page_urls = extract_entry_urls(html)
        if not page_urls:
            break

        if page == 1:
            first_page_urls = set(page_urls)
        elif set(page_urls) == first_page_urls or (prev_page_urls and set(page_urls) == prev_page_urls):
            break

        entry_urls.extend(page_urls)
        prev_page_urls = set(page_urls)

        if len(page_urls) < 25:
            break
        page += 1
        await asyncio.sleep(random.uniform(0.5, 1.0))

    entry_urls = list(set(entry_urls))

    # Process entries concurrently
    sem = asyncio.Semaphore(CONCURRENT_ENTRIES)
    saved = 0

    async def scrape_one(entry_url):
        nonlocal saved
        async with sem:
            ok = await process_entry(session, entry_url, bearer_token, db_conn)
            if ok:
                saved += 1
            await asyncio.sleep(random.uniform(0.3, 0.7))

    tasks = [scrape_one(u) for u in entry_urls]
    await asyncio.gather(*tasks)

    return saved, len(entry_urls)


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unified scraper: download + parse + phone → DB")
    parser.add_argument("--restart", action="store_true", help="Ignore checkpoints, start from scratch")
    args = parser.parse_args()

    log.info("Starting unified scrape_and_parse...")

    # Get bearer token
    token, cookies = await bearer_token_finder.get_bearer_token_and_cookies(headless=True)
    if token:
        HEADERS["authorization"] = f"Bearer {token}"
    if cookies:
        COOKIES.update(cookies)

    if not token:
        log.error("Could not get bearer token. Exiting.")
        sys.exit(1)

    # Load leaf URLs from DB
    db_conn = get_db()
    rows = db_conn.execute("SELECT url FROM leaf_urls ORDER BY id").fetchall()
    if not rows:
        log.error("No leaf URLs in database. Run category scraper first (step 1).")
        sys.exit(1)

    leaf_urls = [r[0] for r in rows]
    log.info(f"Found {len(leaf_urls)} leaf URLs to process")

    # Resume from checkpoint
    checkpoint_key = f"scrape_and_parse_{datetime.now().strftime('%Y-%m-%d')}"
    last_idx = 0
    if not args.restart:
        cp = get_checkpoint(db_conn, checkpoint_key)
        if cp:
            last_idx = cp.get("last_index", 0)
            log.info(f"Resuming from leaf URL #{last_idx + 1}")

    total_saved = 0
    start_time = time.time()

    async with AsyncSession() as session:
        for idx, leaf_url in enumerate(leaf_urls[last_idx:], start=last_idx):
            # Refresh token every 50 leaf URLs
            if idx > 0 and idx % 50 == 0:
                await refresh_headers_and_cookies()

            saved, total = await process_leaf_url(session, leaf_url, token, db_conn)
            total_saved += saved
            db_conn.commit()

            set_checkpoint(db_conn, checkpoint_key, {"last_index": idx + 1})

            elapsed = time.time() - start_time
            log.info(
                f"Leaf {idx + 1}/{len(leaf_urls)}: saved {saved}/{total} entries "
                f"(total: {total_saved}, elapsed: {elapsed:.0f}s)"
            )

    log_event(db_conn, "INFO", "scrape_and_parse", f"Completed. Saved {total_saved} listings in {time.time()-start_time:.0f}s")
    db_conn.close()
    log.info(f"Done. Total new listings: {total_saved}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AntibotExit:
        log.error("Anti-bot detected. Exiting with code 99.")
        sys.exit(99)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        log.error(f"Unhandled error: {e}", exc_info=True)
        sys.exit(1)
