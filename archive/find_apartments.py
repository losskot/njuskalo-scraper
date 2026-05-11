#!/usr/bin/env python3
"""
find_apartments.py — Filter Zagreb apartment rentals and produce a Leaflet map.

Criteria:
  1. Located in Zagreb
  2. Monthly price < 555 EUR
  3. Available for move-in by 17 May 2026 (or earlier / unspecified)
  4. No agency fee / commission (checked via AI)
  5. Has a bathtub (checked via AI vision — images streamed in-memory, no disk)

Usage:
    python find_apartments.py [--json-dir DIR] [--out results.html]
                              [--model MODEL] [--skip-scrape]
                              [--max-images N] [--dry-run]

Requires:  azure-identity, openai, curl_cffi
Auth:      Azure CLI credentials (az login)
"""

import argparse
import asyncio
import base64
import glob
import itertools
import json
import logging
import os
import re
import sys
import time
import threading
from datetime import datetime, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.identity import AzureCliCredential
from openai import AzureOpenAI, RateLimitError, APIStatusError
from curl_cffi.requests import AsyncSession

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AZURE_ENDPOINTS = [
    "https://admin-ml8gx7ra-eastus2.cognitiveservices.azure.com/",
    "https://gpt52-eastus.cognitiveservices.azure.com/",
    "https://gpt52-westus.cognitiveservices.azure.com/",
    "https://gpt52-swedencentral.cognitiveservices.azure.com/",
    "https://gpt52-francecentral.cognitiveservices.azure.com/",
]
API_VERSION    = "2025-01-01-preview"
DEFAULT_MODEL  = "gpt-5.2-chat"

MAX_PRICE_EUR       = 555
AVAILABLE_BY        = date(2026, 5, 17)
ZAGREB_LAT_RANGE    = (45.70, 45.92)
ZAGREB_LNG_RANGE    = (15.82, 16.15)

MAX_RETRIES         = 6
IMAGE_DOWNLOAD_SEM  = 10
AI_TEXT_WORKERS      = 5
AI_VISION_WORKERS    = 3
MAX_IMAGES_PER_LISTING = 15  # skip the rest
MAX_LISTING_AGE_DAYS   = 30  # skip listings published more than this many days ago (0 = no limit)

SCRIPT_DIR      = Path(__file__).parent
JSON_DIR        = SCRIPT_DIR / "backend" / "json"
CHECKPOINT_DIR  = SCRIPT_DIR / "checkpoints"

IMAGE_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.njuskalo.hr/",
    "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("find_apartments")

# ---------------------------------------------------------------------------
# Bathtub classifier prompt (reused from bathtub_classifier.py)
# ---------------------------------------------------------------------------

BATHTUB_PROMPT = """\
You are analyzing a single image from a real estate rental listing.

Your task: determine whether the image shows a bathtub that a person can LIE DOWN IN (recline horizontally with legs extended). A standing-only shower does NOT count.

Classify the image into exactly ONE of these four categories:

1. "not_bathroom"
   The image does not show a bathroom, or the bathroom is not the primary subject.

2. "bathroom_with_bathtub"
   The image shows a bathroom that contains a bathtub a person can lie down in. This INCLUDES:
   - Standard built-in rectangular bathtubs
   - Freestanding bathtubs (clawfoot, modern oval, stone)
   - Corner bathtubs
   - Combination units: rectangular bathtub WITH an overhead shower / shower screen / shower curtain
   - Whirlpool / jacuzzi tubs

3. "bathroom_with_shower_only"
   The image shows a bathroom with no bathtub, only a shower.

4. "bathroom_unclear"
   The image shows a bathroom but you cannot determine whether a bathtub is present.

Output STRICTLY as a single JSON object, no markdown fences:
{
  "category": "not_bathroom" | "bathroom_with_bathtub" | "bathroom_with_shower_only" | "bathroom_unclear",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one short sentence>"
}"""

# ---------------------------------------------------------------------------
# Text analysis prompt (agency fee + address extraction)
# ---------------------------------------------------------------------------

TEXT_ANALYSIS_PROMPT = """\
You are analyzing a Croatian real estate rental listing. Your task is to determine two things:

1. **Agency fee**: Does the listing explicitly state there is NO agency fee / commission?
   Look for Croatian phrases like: "bez provizije", "bez agencijske provizije",
   "bez posredničke naknade", "bez naknade", "provizija 0%", "nema provizije",
   "bez posredovanja", "bez agencije", "direktno od vlasnika", "vlasnik izdaje".
   If ANY of these are present, set no_agency_fee=true.
   If the listing is from an agency (naziv_agencije is present) and does NOT mention
   "bez provizije" or similar, set no_agency_fee=false.
   If there is no agency and no mention either way, set no_agency_fee=true (private owner).

2. **Address**: Extract the most specific street address or location from the text.
   Look for street names (ulica, cesta, avenija, trg), neighborhood names, landmarks.
   If no specific address, use the neighborhood/district from the location field.

IMPORTANT: Respond ONLY with a JSON object, no markdown fences:
{
  "no_agency_fee": true | false,
  "address": "<extracted address string>",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one short sentence>"
}"""

# ---------------------------------------------------------------------------
# Multi-endpoint Azure OpenAI client with round-robin & per-endpoint rate limiters
# ---------------------------------------------------------------------------

class MultiClientHolder:
    """Manages multiple AzureOpenAI clients across regions with round-robin
    distribution and per-endpoint rate limiting / cooldown."""

    def __init__(self, endpoints: list[str] = AZURE_ENDPOINTS,
                 base_delay: float = 0.15):
        self._lock = threading.Lock()
        self._credential = AzureCliCredential()
        self._endpoints = endpoints
        self._base_delay = base_delay
        # Per-endpoint state
        self._clients: list[AzureOpenAI] = []
        self._limiters: list[RateLimiter] = []
        self._cooldowns: list[float] = [0.0] * len(endpoints)  # timestamp when usable again
        self._names: list[str] = []
        self._token: str = ""
        self._expiry: float = 0
        self._refresh_token()
        self._build_clients()
        # Atomic round-robin counter
        self._counter = itertools.count()

    def _refresh_token(self):
        tok = self._credential.get_token(
            "https://cognitiveservices.azure.com/.default"
        )
        self._token = tok.token
        self._expiry = tok.expires_on
        log.info("Azure AD token refreshed (shared across all endpoints)")

    def _build_clients(self):
        self._clients.clear()
        self._limiters.clear()
        self._names.clear()
        for ep in self._endpoints:
            self._clients.append(AzureOpenAI(
                azure_endpoint=ep,
                api_version=API_VERSION,
                azure_ad_token=self._token,
            ))
            self._limiters.append(RateLimiter(base_delay=self._base_delay))
            # Extract short name from URL for logging
            name = ep.split("//")[1].split(".")[0]
            self._names.append(name)
        log.info(f"Initialized {len(self._clients)} Azure OpenAI endpoints: "
                 f"{', '.join(self._names)}")

    def _ensure_token(self):
        if time.time() > self._expiry - 60:
            self._refresh_token()
            self._build_clients()

    def get(self) -> tuple["AzureOpenAI", "RateLimiter", str]:
        """Return (client, limiter, endpoint_name) via round-robin,
        skipping endpoints in cooldown."""
        with self._lock:
            self._ensure_token()
            n = len(self._endpoints)
            now = time.time()
            # Try up to N endpoints to find one not in cooldown
            for _ in range(n):
                idx = next(self._counter) % n
                if now >= self._cooldowns[idx]:
                    return self._clients[idx], self._limiters[idx], self._names[idx]
            # All in cooldown — pick the one that recovers soonest
            idx = min(range(n), key=lambda i: self._cooldowns[i])
            wait = self._cooldowns[idx] - now
            if wait > 0:
                log.warning(f"All endpoints in cooldown, waiting {wait:.1f}s "
                            f"for {self._names[idx]}")
                time.sleep(wait)
            return self._clients[idx], self._limiters[idx], self._names[idx]

    def cooldown(self, endpoint_name: str, seconds: float = 30.0):
        """Temporarily remove an endpoint from rotation after persistent 429s."""
        with self._lock:
            for i, name in enumerate(self._names):
                if name == endpoint_name:
                    self._cooldowns[i] = time.time() + seconds
                    log.warning(f"Endpoint {name} in cooldown for {seconds:.0f}s")
                    break

# ---------------------------------------------------------------------------
# Adaptive rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, base_delay=0.3, backoff_factor=2.0, max_delay=120.0,
                 reduce_factor=0.9, success_streak_threshold=10):
        self.delay = base_delay
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.reduce_factor = reduce_factor
        self.success_streak_threshold = success_streak_threshold
        self._success_streak = 0
        self._total_429s = 0

    def wait(self):
        if self.delay > 0:
            time.sleep(self.delay)

    def on_success(self):
        self._success_streak += 1
        if self._success_streak >= self.success_streak_threshold:
            self.delay = max(self.base_delay, self.delay * self.reduce_factor)
            self._success_streak = 0

    def on_rate_limit(self, retry_after=None):
        self._total_429s += 1
        self._success_streak = 0
        if retry_after and retry_after > self.delay:
            self.delay = min(retry_after + 1.0, self.max_delay)
        else:
            self.delay = min(self.delay * self.backoff_factor, self.max_delay)
        log.warning(f"429 hit (total={self._total_429s}). Next delay: {self.delay:.1f}s")


def _parse_retry_after(exc):
    try:
        headers = exc.response.headers
        val = headers.get("retry-after") or headers.get("x-ratelimit-reset-requests")
        if val:
            return float(val)
    except Exception:
        pass
    try:
        m = re.search(r'retry after (\d+)', str(exc), re.IGNORECASE)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None

# ---------------------------------------------------------------------------
# Phase 1: Load & pre-filter (no AI, instant)
# ---------------------------------------------------------------------------

def parse_price(cijena: str) -> float | None:
    """Extract numeric EUR price from strings like '500 €', '1.200 €'."""
    if not cijena:
        return None
    # Remove dots used as thousands separators, replace comma with dot
    cleaned = cijena.replace(".", "").replace(",", ".")
    m = re.search(r'([\d.]+)', cleaned)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def parse_date(date_str: str) -> date | None:
    """Parse Croatian date formats: DD.MM.YYYY. or D.M.YYYY. (trailing dot)."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip(".")
    # Try DD.MM.YYYY
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def is_in_zagreb(data: dict) -> bool:
    """Check if listing is in Zagreb by location text or coordinates."""
    loc_text = data.get("Lokacija", "")
    if "Zagreb" in loc_text or "zagreb" in loc_text.lower():
        return True
    loc = data.get("lokacija", {})
    if loc:
        lat = loc.get("lat", 0)
        lng = loc.get("lng", 0)
        if (ZAGREB_LAT_RANGE[0] <= lat <= ZAGREB_LAT_RANGE[1] and
                ZAGREB_LNG_RANGE[0] <= lng <= ZAGREB_LNG_RANGE[1]):
            return True
    return False


def check_availability(data: dict) -> bool | None:
    """Check availability. Returns True/False, or None if unknown (needs AI)."""
    dostupno = data.get("Dostupno od", "")
    if not dostupno:
        # Field missing — mark for AI check
        return None
    dostupno_lower = dostupno.lower().strip()
    if dostupno_lower == "odmah":
        return True
    d = parse_date(dostupno)
    if d is None:
        # Can't parse — mark for AI check
        return None
    return d <= AVAILABLE_BY


# ---------------------------------------------------------------------------
# AI availability date check (for listings with missing "Dostupno od" field)
# ---------------------------------------------------------------------------

AVAILABILITY_PROMPT = f"""\
You are analyzing a Croatian real estate rental listing description.
Today's date is {date.today().strftime('%d.%m.%Y')}.
The tenant needs to move in by 17.05.2026. (17 May 2026).

Your task: determine from the description text whether the apartment is available
for move-in on or before 17 May 2026.

Look for:
- Explicit dates like "dostupno od 01.05.2026", "useljivo odmah", "slobodno od ..."
- Phrases like "odmah dostupno", "slobodno", "useljivo", "od odmah"
- "prvi najam" often means available now
- If no date info at all, assume it IS available (owner posting = likely available)

Respond ONLY with a JSON object, no markdown fences:
{{{{
  "available_by_may_17": true | false,
  "detected_date": "<date if found, or 'not_mentioned'>",
  "reasoning": "<one short sentence>"
}}}}"""


def check_availability_ai(client_holder: MultiClientHolder, model: str,
                           data: dict) -> bool:
    """Use AI to determine availability from description text."""
    listing_id = data.get("id", "?")
    description = data.get("opis", "")
    title = data.get("naslov", "")
    dostupnost = data.get("Dostupnost kroz godinu", "")

    user_text = f"Title: {title}\nDostupnost kroz godinu: {dostupnost}\nDescription: {description}"

    kwargs = dict(
        model=model,
        messages=[
            {"role": "system", "content": AVAILABILITY_PROMPT},
            {"role": "user", "content": user_text},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=200,
    )

    consecutive_429s = 0
    for attempt in range(1, MAX_RETRIES + 1):
        client, limiter, ep_name = client_holder.get()
        limiter.wait()
        try:
            resp = client.chat.completions.create(**kwargs)
            raw = resp.choices[0].message.content
            limiter.on_success()
            consecutive_429s = 0
        except RateLimitError as e:
            retry_after = _parse_retry_after(e)
            limiter.on_rate_limit(retry_after)
            consecutive_429s += 1
            if consecutive_429s >= 3:
                client_holder.cooldown(ep_name)
                consecutive_429s = 0
            if attempt == MAX_RETRIES:
                log.warning(f"  [{listing_id}] Availability AI failed: rate limit")
                return True
            time.sleep(limiter.delay)
            continue
        except APIStatusError as e:
            if e.status_code == 503 and attempt < MAX_RETRIES:
                time.sleep(min(10 * attempt, 60))
                continue
            log.warning(f"  [{listing_id}] Availability AI error: {e}")
            return True  # assume available on failure
        except Exception as e:
            log.warning(f"  [{listing_id}] Availability AI error: {e}")
            return True

        text = (raw or "").strip()
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*```$', '', text)
        try:
            result = json.loads(text.strip())
            available = result.get("available_by_may_17", True)
            log.info(
                f"  [{listing_id}] Availability AI: {available} "
                f"(date={result.get('detected_date', '?')}, "
                f"{result.get('reasoning', '')})"
            )
            return bool(available)
        except Exception:
            if attempt == MAX_RETRIES:
                return True  # assume available on parse failure
            continue

    return True


def load_and_prefilter(json_dir: Path, max_age_days: int = MAX_LISTING_AGE_DAYS) -> tuple[list[dict], dict]:
    """Load all JSONs and apply fast pre-filters. Returns (survivors, stats)."""
    files = sorted(glob.glob(str(json_dir / "*.json")))
    stats = {
        "total": len(files),
        "loaded": 0,
        "price_ok": 0,
        "zagreb_ok": 0,
        "age_ok": 0,
        "avail_ok": 0,
        "avail_needs_ai": 0,
    }

    today = date.today()
    survivors = []
    for fpath in files:
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log.warning(f"Failed to load {fpath}: {e}")
            continue

        stats["loaded"] += 1

        # Price filter
        price = parse_price(data.get("cijena", ""))
        if price is None or price >= MAX_PRICE_EUR:
            continue
        stats["price_ok"] += 1
        data["_price_eur"] = price

        # Zagreb filter
        if not is_in_zagreb(data):
            continue
        stats["zagreb_ok"] += 1

        # Age filter (oglas_objavljen)
        published_raw = data.get("oglas_objavljen", "")
        pub_date_str = published_raw.split(" u ")[0] if published_raw else ""
        data["_published"] = pub_date_str
        if max_age_days > 0 and pub_date_str:
            pub_date = parse_date(pub_date_str)
            if pub_date and (today - pub_date).days > max_age_days:
                continue
        stats["age_ok"] += 1

        # Availability filter
        avail = check_availability(data)
        if avail is False:
            continue
        if avail is None:
            # Date unknown — needs AI check on description
            data["_avail_needs_ai"] = True
            stats["avail_needs_ai"] += 1
        stats["avail_ok"] += 1

        survivors.append(data)

    return survivors, stats

# ---------------------------------------------------------------------------
# Phase 2: AI text analysis (agency fee + address)
# ---------------------------------------------------------------------------

def analyze_text(client_holder: MultiClientHolder, model: str,
                 data: dict) -> dict:
    """Call Azure OpenAI to check agency fee and extract address."""
    listing_id = data.get("id", "?")

    # Build context for AI
    parts = []
    parts.append(f"Title: {data.get('naslov', '')}")
    parts.append(f"Location: {data.get('Lokacija', '')}")
    street = data.get("Ulica", "")
    if street:
        parts.append(f"Street: {street}")
    parts.append(f"Agency: {data.get('naziv_agencije', 'none (private owner)')}")
    costs = data.get("Troškovi", [])
    if costs:
        parts.append(f"Costs: {'; '.join(costs)}")
    parts.append(f"Description: {data.get('opis', '')}")

    user_text = "\n".join(parts)

    kwargs = dict(
        model=model,
        messages=[
            {"role": "system", "content": TEXT_ANALYSIS_PROMPT},
            {"role": "user", "content": user_text},
        ],
        response_format={"type": "json_object"},
    )
    if model.startswith("gpt-4"):
        kwargs["max_tokens"] = 256
        kwargs["temperature"] = 0
    else:
        kwargs["max_completion_tokens"] = 256

    consecutive_429s = 0
    for attempt in range(1, MAX_RETRIES + 1):
        client, limiter, ep_name = client_holder.get()
        limiter.wait()
        try:
            resp = client.chat.completions.create(**kwargs)
            raw = resp.choices[0].message.content
            limiter.on_success()
            consecutive_429s = 0
        except RateLimitError as e:
            retry_after = _parse_retry_after(e)
            limiter.on_rate_limit(retry_after)
            consecutive_429s += 1
            if consecutive_429s >= 3:
                client_holder.cooldown(ep_name)
                consecutive_429s = 0
            if attempt == MAX_RETRIES:
                log.error(f"  [{listing_id}] Text analysis failed: rate limit")
                return {"no_agency_fee": None, "address": "", "error": str(e)}
            time.sleep(limiter.delay)
            continue
        except APIStatusError as e:
            if e.status_code == 503 and attempt < MAX_RETRIES:
                time.sleep(min(10 * attempt, 60))
                continue
            log.error(f"  [{listing_id}] Text analysis API error: {e}")
            return {"no_agency_fee": None, "address": "", "error": str(e)}
        except Exception as e:
            log.error(f"  [{listing_id}] Text analysis error: {e}")
            return {"no_agency_fee": None, "address": "", "error": str(e)}

        # Parse response
        text = (raw or "").strip()
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*```$', '', text)
        try:
            result = json.loads(text.strip())
            return {
                "no_agency_fee": result.get("no_agency_fee"),
                "address": result.get("address", ""),
                "confidence": result.get("confidence", 0),
                "reasoning": result.get("reasoning", ""),
                "error": None,
            }
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.warning(f"  [{listing_id}] JSON parse failed: {e}, raw={text[:100]}")
                return {"no_agency_fee": None, "address": "", "error": str(e)}
            continue

    return {"no_agency_fee": None, "address": "", "error": "exhausted retries"}

# ---------------------------------------------------------------------------
# Phase 3: Download images to memory & classify bathtub
# ---------------------------------------------------------------------------

async def download_image_bytes(session, sem, url: str,
                               max_retries: int = 3) -> bytes | None:
    """Download a single image to memory with retry. Returns bytes or None."""
    for attempt in range(1, max_retries + 1):
        async with sem:
            try:
                resp = await asyncio.wait_for(
                    session.get(url, headers=IMAGE_HEADERS, impersonate="chrome110"),
                    timeout=20,
                )
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                if attempt == max_retries:
                    log.debug(f"  Image download failed after {max_retries} tries: {url}: {e}")
                    return None
                await asyncio.sleep(1.5 * attempt)
    return None


def classify_image_bytes(client_holder: MultiClientHolder, model: str,
                         image_bytes: bytes) -> dict:
    """Send image bytes to Azure OpenAI vision for bathtub classification."""
    b64 = base64.b64encode(image_bytes).decode("utf-8")

    kwargs = dict(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{b64}",
                            "detail": "high",
                        },
                    },
                    {"type": "text", "text": BATHTUB_PROMPT},
                ],
            }
        ],
        response_format={"type": "json_object"},
    )
    if model.startswith("gpt-4"):
        kwargs["max_tokens"] = 256
        kwargs["temperature"] = 0
    else:
        kwargs["max_completion_tokens"] = 256

    consecutive_429s = 0
    for attempt in range(1, MAX_RETRIES + 1):
        client, limiter, ep_name = client_holder.get()
        limiter.wait()
        try:
            resp = client.chat.completions.create(**kwargs)
            raw = resp.choices[0].message.content
            limiter.on_success()
            consecutive_429s = 0
        except RateLimitError as e:
            retry_after = _parse_retry_after(e)
            limiter.on_rate_limit(retry_after)
            consecutive_429s += 1
            if consecutive_429s >= 3:
                client_holder.cooldown(ep_name)
                consecutive_429s = 0
            if attempt == MAX_RETRIES:
                return {"category": None, "error": str(e)}
            time.sleep(limiter.delay)
            continue
        except APIStatusError as e:
            if e.status_code == 503 and attempt < MAX_RETRIES:
                time.sleep(min(10 * attempt, 60))
                continue
            return {"category": None, "error": str(e)}
        except Exception as e:
            return {"category": None, "error": str(e)}

        text = (raw or "").strip()
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*```$', '', text)
        try:
            result = json.loads(text.strip())
            return {
                "category": result.get("category"),
                "confidence": result.get("confidence", 0),
                "reasoning": result.get("reasoning", ""),
                "error": None,
            }
        except Exception as e:
            if attempt == MAX_RETRIES:
                return {"category": None, "error": f"JSON parse: {e}"}
            continue

    return {"category": None, "error": "exhausted retries"}


async def check_bathtub_for_listing(
    listing: dict,
    client_holder: MultiClientHolder,
    model: str,
    max_images: int,
    session: AsyncSession,
    download_sem: asyncio.Semaphore,
) -> bool:
    """Download images in memory and classify. Returns True if bathtub found."""
    listing_id = listing.get("id", "?")
    urls = listing.get("slike", [])[:max_images]
    if not urls:
        log.info(f"  [{listing_id}] No images — skipping bathtub check")
        return False

    # Download all images concurrently
    download_tasks = [
        download_image_bytes(session, download_sem, url) for url in urls
    ]
    image_bytes_list = await asyncio.gather(*download_tasks)

    # Classify images — early exit on bathtub found
    loop = asyncio.get_event_loop()
    for i, img_bytes in enumerate(image_bytes_list):
        if img_bytes is None:
            continue
        # Run blocking AI call in thread pool to not block event loop
        result = await loop.run_in_executor(
            None, classify_image_bytes, client_holder, model, img_bytes
        )
        cat = result.get("category")
        log.debug(
            f"  [{listing_id}] img {i+1}/{len(urls)}: {cat} "
            f"({result.get('reasoning', '')[:60]})"
        )
        if cat == "bathroom_with_bathtub":
            conf = result.get("confidence", 0)
            if conf >= 0.7:
                log.info(
                    f"  [{listing_id}] Bathtub found in image {i+1} "
                    f"(confidence={conf:.0%})"
                )
                return True  # early exit

    return False

# ---------------------------------------------------------------------------
# Phase 4: Generate HTML map
# ---------------------------------------------------------------------------

def generate_html(results: list[dict], stats: dict, out_path: Path):
    """Generate a standalone HTML file with a Leaflet map."""
    markers_json = json.dumps([
        {
            "lat": r["lat"],
            "lng": r["lng"],
            "price": r["price"],
            "title": r["title"],
            "url": r["url"],
            "address": r["address"],
            "rooms": r.get("rooms", ""),
            "area": r.get("area", ""),
            "id": r["id"],
            "published": r.get("published", ""),
        }
        for r in results
    ], ensure_ascii=False)

    stats_json = json.dumps(stats, ensure_ascii=False)

    html = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zagreb Apartments — Filtered Results</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  #header {{
    background: #1a1a2e; color: #fff; padding: 12px 20px;
    display: flex; align-items: center; gap: 16px; flex-wrap: wrap; z-index: 1000;
  }}
  #header h1 {{ font-size: 1.2rem; margin-right: 4px; }}
  .stat {{ background: rgba(255,255,255,0.1); padding: 5px 12px; border-radius: 6px; font-size: 0.82rem; }}
  .stat b {{ color: #4fc3f7; }}
  #map {{ height: calc(100vh - 54px); width: 100%; }}

  /* ── Floating filter panel ── */
  #filter-panel {{
    position: absolute;
    top: 70px; right: 12px;
    z-index: 1000;
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.25);
    padding: 14px 16px;
    min-width: 220px;
    font-size: 0.88rem;
    color: #222;
  }}
  #filter-panel h3 {{
    font-size: 0.9rem; font-weight: 700; margin-bottom: 12px;
    color: #1565c0; border-bottom: 1px solid #e3eaf3; padding-bottom: 6px;
  }}
  .fp-row {{ margin-bottom: 10px; }}
  .fp-row label {{ display: block; font-weight: 600; margin-bottom: 4px; color: #444; }}
  .fp-row select, .fp-row input[type=range] {{ width: 100%; }}
  .fp-row select {{
    border: 1px solid #cdd5e0; border-radius: 5px; padding: 5px 8px;
    font-size: 0.85rem; background: #f7f9fc; cursor: pointer; color: #222;
  }}
  .fp-row input[type=range] {{ accent-color: #1565c0; cursor: pointer; }}
  .fp-row .range-val {{ text-align: right; color: #1565c0; font-weight: 700; font-size: 0.82rem; }}
  #fp-count {{
    margin-top: 10px; padding-top: 8px; border-top: 1px solid #e3eaf3;
    text-align: center; font-size: 0.85rem; color: #555;
  }}
  #fp-count b {{ color: #1565c0; font-size: 1rem; }}

  .price-icon {{
    background: #1565c0; color: #fff; font-weight: 700; font-size: 12px;
    padding: 3px 7px; border-radius: 4px; white-space: nowrap;
    box-shadow: 0 1px 4px rgba(0,0,0,0.3); border: 1px solid #0d47a1;
  }}
  .leaflet-popup-content {{ min-width: 230px; }}
  .popup-title {{ font-weight: 600; font-size: 0.95rem; margin-bottom: 6px; }}
  .popup-row {{ font-size: 0.85rem; color: #444; margin: 3px 0; }}
  .popup-price {{ font-size: 1.1rem; font-weight: 700; color: #1565c0; }}
  .popup-link {{ display: inline-block; margin-top: 8px; color: #1565c0; font-size: 0.85rem; }}
</style>
</head>
<body>
<div id="header">
  <h1>🏠 Zagreb Apartments</h1>
  <span class="stat">Total scraped: <b id="s-total">-</b></span>
  <span class="stat">Price &lt;{MAX_PRICE_EUR}€: <b id="s-price">-</b></span>
  <span class="stat">Available: <b id="s-avail">-</b></span>
  <span class="stat">No fee: <b id="s-fee">-</b></span>
  <span class="stat">Bathtub: <b id="s-bathtub">-</b></span>
</div>

<div id="map"></div>

<!-- Floating filter panel -->
<div id="filter-panel">
  <h3>🔍 Map Filters</h3>

  <div class="fp-row">
    <label for="age-filter">📅 Published within</label>
    <select id="age-filter">
      <option value="0">All time</option>
      <option value="7">Last 7 days</option>
      <option value="14">Last 14 days</option>
      <option value="30" selected>Last 30 days</option>
      <option value="60">Last 60 days</option>
      <option value="90">Last 90 days</option>
    </select>
  </div>

  <div class="fp-row">
    <label for="price-filter">💶 Max price: <span class="range-val" id="price-val">any</span></label>
    <input type="range" id="price-filter" min="100" max="{MAX_PRICE_EUR}" step="10" value="{MAX_PRICE_EUR}">
  </div>

  <div id="fp-count">Showing <b id="s-showing">-</b> listings</div>
</div>

<script>
const markersData = {markers_json};
const stats = {stats_json};

document.getElementById('s-total').textContent   = stats.total || '-';
document.getElementById('s-price').textContent    = stats.price_ok || '-';
document.getElementById('s-avail').textContent    = stats.avail_ok || '-';
document.getElementById('s-fee').textContent      = stats.no_fee_ok || '-';
document.getElementById('s-bathtub').textContent  = stats.bathtub_ok || '-';

const map = L.map('map').setView([45.815, 15.982], 12);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '&copy; OpenStreetMap contributors',
  maxZoom: 19,
}}).addTo(map);

function parseCroDate(s) {{
  if (!s) return null;
  const p = s.split('.');
  if (p.length < 3) return null;
  return new Date(parseInt(p[2]), parseInt(p[1]) - 1, parseInt(p[0]));
}}

markersData.forEach(m => {{ m._date = parseCroDate(m.published); }});

const leafletMarkers = markersData.map(m => {{
  const icon = L.divIcon({{
    className: '',
    html: '<div class="price-icon">' + m.price + '€</div>',
    iconSize: [60, 22], iconAnchor: [30, 11],
  }});
  const lm = L.marker([m.lat, m.lng], {{ icon }});
  lm.bindPopup(
    '<div class="popup-title">' + m.title.substring(0, 80) + '</div>' +
    '<div class="popup-price">' + m.price + ' \u20ac/month</div>' +
    '<div class="popup-row">📍 ' + (m.address || 'N/A') + '</div>' +
    (m.rooms ? '<div class="popup-row">🛏 ' + m.rooms + '</div>' : '') +
    (m.area  ? '<div class="popup-row">📐 ' + m.area  + '</div>' : '') +
    (m.published ? '<div class="popup-row">📅 ' + m.published + '</div>' : '') +
    '<a class="popup-link" href="' + m.url + '" target="_blank">↗ Open on Nju\u0161kalo</a>'
  );
  return lm;
}});

const ageSelect   = document.getElementById('age-filter');
const priceSlider = document.getElementById('price-filter');
const priceVal    = document.getElementById('price-val');
const maxPriceCap = {MAX_PRICE_EUR};

priceVal.textContent = maxPriceCap + '\u20ac';

function applyFilters() {{
  const maxDays  = parseInt(ageSelect.value);
  const maxPrice = parseInt(priceSlider.value);
priceVal.textContent = maxPrice >= maxPriceCap ? 'any' : maxPrice + '€';

  const now = new Date(); now.setHours(0,0,0,0);
  let visible = 0;
  markersData.forEach((m, i) => {{
    const lm = leafletMarkers[i];
    let show = true;
    if (maxDays > 0 && m._date) {{
      show = Math.floor((now - m._date) / 86400000) <= maxDays;
    }}
    if (show && maxPrice < maxPriceCap) {{
      show = m.price <= maxPrice;
    }}
    if (show) {{ if (!map.hasLayer(lm)) lm.addTo(map); visible++; }}
    else       {{ if (map.hasLayer(lm)) map.removeLayer(lm); }}
  }});
  document.getElementById('s-showing').textContent = visible;
}}

ageSelect.addEventListener('change', applyFilters);
priceSlider.addEventListener('input', applyFilters);

applyFilters();

if (markersData.length > 0) {{
  const group = L.featureGroup(markersData.map(m => L.marker([m.lat, m.lng])));
  map.fitBounds(group.getBounds().pad(0.1));
}}
</script>
</body>
</html>"""
    out_path.write_text(html, encoding="utf-8")
    log.info(f"Map written to {out_path} ({len(results)} markers)")

# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _checkpoint_path(run_id: str) -> Path:
    return CHECKPOINT_DIR / f"find_{run_id}.json"


def _save_checkpoint(run_id: str, phase: int, **data):
    """Save pipeline state after a phase completes."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp = {"run_id": run_id, "phase": phase, **data}
    path = _checkpoint_path(run_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cp, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)  # atomic on POSIX
    log.info(f"  Checkpoint saved: phase {phase} → {path.name}")


def _load_checkpoint(run_id: str | None) -> dict | None:
    """Load a checkpoint. If run_id is None, find the latest one."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    if run_id:
        path = _checkpoint_path(run_id)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return None
    # Find latest
    cps = sorted(CHECKPOINT_DIR.glob("find_*.json"))
    if not cps:
        return None
    return json.loads(cps[-1].read_text(encoding="utf-8"))


def _reload_listings(json_dir: Path, ids: set) -> dict[str, dict]:
    """Reload full listing data from JSON files for given IDs."""
    result = {}
    for fpath in glob.glob(str(json_dir / "*.json")):
        fname = Path(fpath).stem
        if fname in ids:
            try:
                with open(fpath, encoding="utf-8") as f:
                    result[fname] = json.load(f)
            except Exception:
                pass
    return result


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(args):
    t_start = time.time()

    # Run ID and output path
    run_id = args.run_id
    out_path = Path(args.out)
    checkpoint = None

    # Resume from checkpoint?
    if args.resume:
        checkpoint = _load_checkpoint(args.resume_id)
        if checkpoint:
            run_id = checkpoint["run_id"]
            out_path = SCRIPT_DIR / f"results_{run_id}.html"
            log.info(f"Resuming run {run_id} from phase {checkpoint['phase'] + 1}")
        else:
            log.warning("No checkpoint found to resume. Starting fresh.")

    # ── 1. Pre-filter ────────────────────────────────────────────────
    if not checkpoint or checkpoint["phase"] < 1:
        log.info("═" * 60)
        log.info("Phase 1: Loading & pre-filtering JSONs")
        log.info("═" * 60)

        survivors, stats = load_and_prefilter(Path(args.json_dir), max_age_days=args.max_age)

        log.info(f"  Total JSON files : {stats['total']}")
        log.info(f"  Loaded           : {stats['loaded']}")
        log.info(f"  Price < {MAX_PRICE_EUR}€      : {stats['price_ok']}")
        log.info(f"  In Zagreb        : {stats['zagreb_ok']}")
        log.info(f"  Age <= {args.max_age}d       : {stats['age_ok']}")
        log.info(f"  Available by {AVAILABLE_BY} : {stats['avail_ok']} ({stats['avail_needs_ai']} need AI check)")
        log.info(f"  → Survivors      : {len(survivors)}")

        if not survivors:
            log.warning("No listings survived pre-filtering. Generating empty map.")
            stats["no_fee_ok"] = 0
            stats["bathtub_ok"] = 0
            generate_html([], stats, out_path)
            return

        # Save checkpoint: listing IDs + enriched fields
        survivor_data = []
        for d in survivors:
            survivor_data.append({
                "id": str(d.get("id", "")),
                "_price_eur": d.get("_price_eur"),
                "_avail_needs_ai": d.get("_avail_needs_ai", False),
            })
        _save_checkpoint(run_id, 1, stats=stats, survivor_ids=survivor_data)
    else:
        # Restore phase 1 from checkpoint
        stats = checkpoint["stats"]
        survivor_meta = checkpoint["survivor_ids"]
        id_set = {s["id"] for s in survivor_meta}
        meta_map = {s["id"]: s for s in survivor_meta}
        json_data = _reload_listings(Path(args.json_dir), id_set)
        survivors = []
        for sid, d in json_data.items():
            m = meta_map.get(sid, {})
            d["_price_eur"] = m.get("_price_eur")
            if m.get("_avail_needs_ai"):
                d["_avail_needs_ai"] = True
            survivors.append(d)
        log.info(f"  Restored {len(survivors)} survivors from checkpoint")

    if args.dry_run:
        log.info("DRY RUN: skipping AI analysis")
        results = []
        for d in survivors:
            loc = d.get("lokacija", {})
            results.append({
                "id": d.get("id", ""),
                "lat": loc.get("lat", 0),
                "lng": loc.get("lng", 0),
                "price": d.get("_price_eur", 0),
                "title": d.get("naslov", ""),
                "url": d.get("link", ""),
                "address": d.get("Lokacija", ""),
                "rooms": d.get("Broj soba", ""),
                "area": d.get("Stambena površina", ""),
                "published": d.get("_published", ""),
            })
        stats["no_fee_ok"] = "N/A"
        stats["bathtub_ok"] = "N/A"
        generate_html(results, stats, out_path)
        log.info(f"Done in {time.time()-t_start:.1f}s (dry run)")
        return

    # ── 1b. AI availability check for listings with missing date ─────
    client_holder = MultiClientHolder()

    if not checkpoint or checkpoint["phase"] < 2:
        needs_ai_avail = [d for d in survivors if d.get("_avail_needs_ai")]
        if needs_ai_avail:
            log.info("")
            log.info("═" * 60)
            log.info(f"Phase 1b: AI availability check ({len(needs_ai_avail)} listings)")
            log.info("═" * 60)

            with ThreadPoolExecutor(max_workers=AI_TEXT_WORKERS) as pool:
                futures = {
                    pool.submit(
                        check_availability_ai, client_holder, args.model, d
                    ): d
                    for d in needs_ai_avail
                }
                removed = 0
                for future in as_completed(futures):
                    d = futures[future]
                    lid = d.get("id", "?")
                    try:
                        available = future.result()
                    except Exception:
                        available = True  # assume available on error
                    if not available:
                        survivors.remove(d)
                        removed += 1
                        log.info(f"  [{lid}] Excluded: not available by {AVAILABLE_BY}")

            stats["avail_ok"] -= removed
            log.info(f"  → Removed {removed}, remaining: {len(survivors)}")

        if not survivors:
            log.warning("No listings survived availability AI check. Generating empty map.")
            stats["no_fee_ok"] = 0
            stats["bathtub_ok"] = 0
            generate_html([], stats, out_path)
            return

        # Save checkpoint after phase 1b
        survivor_ids = [{"id": str(d.get("id", "")), "_price_eur": d.get("_price_eur")}
                        for d in survivors]
        _save_checkpoint(run_id, 2, stats=stats, survivor_ids=survivor_ids)
    else:
        if checkpoint["phase"] < 2:
            # Already handled above
            pass
        else:
            # Restore from phase 2 checkpoint — survivors already loaded in phase 1 restore
            # but need to filter to only those that passed availability
            cp_ids = {s["id"] for s in checkpoint["survivor_ids"]}
            survivors = [d for d in survivors if str(d.get("id", "")) in cp_ids]
            log.info(f"  Restored {len(survivors)} survivors after availability check")

    # ── 2. AI text analysis (agency fee + address) ───────────────────
    if not checkpoint or checkpoint["phase"] < 3:
        log.info("")
        log.info("═" * 60)
        log.info("Phase 2: AI text analysis (agency fee + address extraction)")
        log.info("═" * 60)

        text_results = {}  # id -> result dict
        with ThreadPoolExecutor(max_workers=AI_TEXT_WORKERS) as pool:
            futures = {
                pool.submit(analyze_text, client_holder, args.model, d): d
                for d in survivors
            }
            done_count = 0
            for future in as_completed(futures):
                d = futures[future]
                listing_id = d.get("id", "?")
                try:
                    result = future.result()
                except Exception as e:
                    result = {"no_agency_fee": None, "address": "", "error": str(e)}
                text_results[listing_id] = result
                done_count += 1
                if done_count % 10 == 0 or done_count == len(survivors):
                    log.info(f"  Text analysis: {done_count}/{len(survivors)}")

        # Filter: keep only no_agency_fee == True
        no_fee_survivors = []
        for d in survivors:
            lid = d.get("id", "?")
            tr = text_results.get(lid, {})
            if tr.get("no_agency_fee") is True:
                d["_address"] = tr.get("address", "")
                d["_text_reasoning"] = tr.get("reasoning", "")
                no_fee_survivors.append(d)
            else:
                reason = tr.get("reasoning", tr.get("error", "unknown"))
                log.debug(f"  [{lid}] Excluded: agency fee ({reason})")

        stats["no_fee_ok"] = len(no_fee_survivors)
        log.info(f"  → No agency fee: {len(no_fee_survivors)} / {len(survivors)}")

        if not no_fee_survivors:
            log.warning("No listings passed agency fee filter. Generating empty map.")
            stats["bathtub_ok"] = 0
            generate_html([], stats, out_path)
            return

        # Save checkpoint after phase 2: include text results for resume
        no_fee_data = []
        for d in no_fee_survivors:
            no_fee_data.append({
                "id": str(d.get("id", "")),
                "_price_eur": d.get("_price_eur"),
                "_address": d.get("_address", ""),
                "_text_reasoning": d.get("_text_reasoning", ""),
            })
        _save_checkpoint(run_id, 3, stats=stats, no_fee_survivors=no_fee_data,
                         checked_bathtub_ids=[], bathtub_results=[])
    else:
        # Restore phase 2 results from checkpoint
        no_fee_meta = checkpoint.get("no_fee_survivors", [])
        nf_ids = {s["id"] for s in no_fee_meta}
        nf_meta_map = {s["id"]: s for s in no_fee_meta}

        # Need to reload JSONs if not already loaded
        if not survivors:
            json_data = _reload_listings(Path(args.json_dir), nf_ids)
            no_fee_survivors = []
            for sid, d in json_data.items():
                m = nf_meta_map.get(sid, {})
                d["_price_eur"] = m.get("_price_eur")
                d["_address"] = m.get("_address", "")
                d["_text_reasoning"] = m.get("_text_reasoning", "")
                no_fee_survivors.append(d)
        else:
            no_fee_survivors = []
            for d in survivors:
                sid = str(d.get("id", ""))
                if sid in nf_ids:
                    m = nf_meta_map.get(sid, {})
                    d["_address"] = m.get("_address", "")
                    d["_text_reasoning"] = m.get("_text_reasoning", "")
                    no_fee_survivors.append(d)

        log.info(f"  Restored {len(no_fee_survivors)} no-fee survivors from checkpoint")

    # ── 3. AI vision analysis (bathtub detection) ────────────────────
    log.info("")
    log.info("═" * 60)
    log.info("Phase 3: AI vision analysis (bathtub detection — in-memory)")
    log.info("═" * 60)

    # Restore partial Phase 3 progress from checkpoint
    already_checked = set()
    bathtub_results = []
    if checkpoint and checkpoint["phase"] >= 3:
        already_checked = set(checkpoint.get("checked_bathtub_ids", []))
        bathtub_results = list(checkpoint.get("bathtub_results", []))
        if already_checked:
            log.info(f"  Resuming: {len(already_checked)} already checked, "
                     f"{len(bathtub_results)} bathtub hits so far")

    remaining = [d for d in no_fee_survivors
                 if str(d.get("id", "")) not in already_checked]

    download_sem = asyncio.Semaphore(IMAGE_DOWNLOAD_SEM)

    async def _check_one(d):
        lid = d.get("id", "?")
        async with AsyncSession() as session:
            return lid, d, await check_bathtub_for_listing(
                d, client_holder, args.model,
                args.max_images, session, download_sem
            )

    # Process listings concurrently (AI_VISION_WORKERS at a time)
    vision_sem = asyncio.Semaphore(AI_VISION_WORKERS)

    async def _bounded_check(d):
        async with vision_sem:
            return await _check_one(d)

    total_to_check = len(no_fee_survivors)
    done_so_far = len(already_checked)
    tasks = [_bounded_check(d) for d in remaining]
    for i, coro in enumerate(asyncio.as_completed(tasks)):
        lid, d, has_bathtub = await coro
        already_checked.add(str(lid))
        done_so_far += 1
        if has_bathtub:
            loc = d.get("lokacija", {})
            bathtub_results.append({
                "id": lid,
                "lat": loc.get("lat", 0),
                "lng": loc.get("lng", 0),
                "price": d.get("_price_eur", 0),
                "title": d.get("naslov", ""),
                "url": d.get("link", ""),
                "address": d.get("_address", d.get("Lokacija", "")),
                "rooms": d.get("Broj soba", ""),
                "area": d.get("Stambena površina", ""),
                "published": d.get("_published", ""),
            })
            log.info(f"  [{lid}] ✓ PASS — bathtub confirmed ({done_so_far}/{total_to_check})")
        else:
            log.info(f"  [{lid}] ✗ No bathtub ({done_so_far}/{total_to_check})")

        # Save Phase 3 progress periodically (every listing)
        _save_checkpoint(run_id, 3, stats=stats,
                         no_fee_survivors=[
                             {"id": str(d2.get("id", "")),
                              "_price_eur": d2.get("_price_eur"),
                              "_address": d2.get("_address", ""),
                              "_text_reasoning": d2.get("_text_reasoning", "")}
                             for d2 in no_fee_survivors
                         ],
                         checked_bathtub_ids=list(already_checked),
                         bathtub_results=bathtub_results)

    stats["bathtub_ok"] = len(bathtub_results)

    # ── 4. Generate map ──────────────────────────────────────────────
    log.info("")
    log.info("═" * 60)
    log.info("Phase 4: Generating HTML map")
    log.info("═" * 60)

    generate_html(bathtub_results, stats, out_path)

    # Clean up checkpoint on success
    cp_path = _checkpoint_path(run_id)
    if cp_path.exists():
        cp_path.unlink()
        log.info(f"  Checkpoint removed (clean finish)")

    elapsed = time.time() - t_start
    log.info(f"\n{'═' * 60}")
    log.info(f"DONE in {elapsed:.1f}s")
    log.info(f"  Scraped    : {stats['total']}")
    log.info(f"  Price OK   : {stats['price_ok']}")
    log.info(f"  Age ok     : {stats['age_ok']}")
    log.info(f"  Available  : {stats['avail_ok']}")
    log.info(f"  No fee     : {stats['no_fee_ok']}")
    log.info(f"  Bathtub    : {stats['bathtub_ok']}")
    log.info(f"  Map file   : {out_path.resolve()}")


def main():
    # Generate timestamped run ID
    run_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    default_out = str(SCRIPT_DIR / f"results_{run_id}.html")

    parser = argparse.ArgumentParser(description="Filter Zagreb apartments → Leaflet map")
    parser.add_argument("--json-dir", default=str(JSON_DIR),
                        help="Directory with listing JSON files")
    parser.add_argument("--out", default=default_out,
                        help="Output HTML file path (default: timestamped)")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Azure OpenAI model (default: {DEFAULT_MODEL})")
    parser.add_argument("--max-images", type=int, default=MAX_IMAGES_PER_LISTING,
                        help="Max images to check per listing for bathtub")
    parser.add_argument("--max-age", type=int, default=MAX_LISTING_AGE_DAYS,
                        help=f"Max listing age in days (default: {MAX_LISTING_AGE_DAYS}, 0=no limit)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip AI, output all pre-filtered listings on map")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from the latest checkpoint")
    parser.add_argument("--resume-id", default=None,
                        help="Resume a specific run ID (e.g. 2026-05-11_143022)")
    args = parser.parse_args()
    args.run_id = run_id

    if args.resume or args.resume_id:
        args.resume = True

    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
