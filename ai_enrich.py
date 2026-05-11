#!/usr/bin/env python3
"""
ai_enrich.py — AI enrichment step for listings.

For each listing where ai_enriched_at IS NULL:
1. Text analysis: confirm/correct deterministic extractions (washing machine, dishwasher,
   AC, heating, agency fee) and extract address — single AI call per listing.
2. Vision analysis: stream listing images to detect bathtub.

Replaces: find_apartments.py + bathtub_classifier.py

Usage:
    python ai_enrich.py [--limit N] [--skip-vision] [--model MODEL]

Requires: azure-identity, openai
Auth: Azure CLI credentials (az login)
"""
import argparse
import asyncio
import base64
import itertools
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.identity import AzureCliCredential
from openai import AzureOpenAI, RateLimitError, APIStatusError
from curl_cffi.requests import AsyncSession

from db import get_db

# ─── Config ──────────────────────────────────────────────────────────────────

AZURE_ENDPOINTS = [
    "https://admin-ml8gx7ra-eastus2.cognitiveservices.azure.com/",
    "https://gpt52-eastus.cognitiveservices.azure.com/",
    "https://gpt52-westus.cognitiveservices.azure.com/",
    "https://gpt52-swedencentral.cognitiveservices.azure.com/",
    "https://gpt52-francecentral.cognitiveservices.azure.com/",
]
API_VERSION = "2025-01-01-preview"
DEFAULT_MODEL = "gpt-5.2-chat"
MAX_RETRIES = 6
MAX_IMAGES_PER_LISTING = 15
MAX_LISTING_AGE_DAYS = 30  # skip listings published more than this many days ago (0 = no limit)
AI_TEXT_WORKERS = 5
AI_VISION_WORKERS = 3

IMAGE_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/138.0.0.0 Safari/537.36",
    "referer": "https://www.njuskalo.hr/",
    "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ai_enrich")

# ─── Prompts ──────────────────────────────────────────────────────────────────

TEXT_ENRICHMENT_PROMPT = """\
You are analyzing a Croatian real estate rental listing.

You will receive:
- Listing metadata (title, location, price, poster name)
- Our automated boolean extractions (confirm or correct)
- **Structured tags from the website** — these are AUTHORITATIVE data entered by the poster via dropdowns/checkboxes. Always trust tags over free-text description when they conflict.
- Free-text description

Determine each field using the rules below:

1. **no_agency_fee** (boolean): Is this listing free of agency commission for the tenant?
   PRIORITY ORDER (use the FIRST match):
   a. If tags contain "Agencijska provizija: Najmoprimac ne plaća agencijsku proviziju" → true
   b. If description contains: "bez provizije", "bez agencijske provizije", "bez posredničke naknade", "provizija 0%", "nema provizije", "bez posredovanja", "direktno od vlasnika", "vlasnik izdaje", "agencije isključene" → true
   c. If tags explicitly state a commission amount or percentage → false
   d. If poster name looks like a real estate agency (contains "nekretnine", "d.o.o.", "j.d.o.o.", "obrt") → false
   e. Otherwise → true (assume private owner)
   NOTE: The "Agency" field below is the poster's username, NOT necessarily a real estate agency.

2. **has_washing_machine** (boolean or null):
   - If tags list "Perilica rublja" or "Perilica-sušilica" → true
   - If description mentions: "perilica rublja", "perilicom rublja", "perilica-sušilica", "washing machine" → true
   - If enough detail is given but no mention → false
   - If description is too short to tell → null

3. **has_dishwasher** (boolean or null):
   - If tags list "Perilica posuđa" → true
   - If description mentions: "perilica posuđa", "perilicom posuđa", "dishwasher", "sudomašina" → true
   - If enough detail is given but no mention → false
   - If description is too short to tell → null

4. **has_air_conditioning** (boolean or null):
   - If tags contain "Klima uređaj: Da" → true
   - If description mentions: "klima", "klima uređaj", "klimatiziran", "air conditioning" → true
   - If enough detail is given but no mention → false
   - If description is too short to tell → null

5. **heating_type** (string or null):
   - If tags contain "Sustav grijanja: ..." → use that value
   - Otherwise look in description for: "centralno", "plinsko", "toplana", "gradska toplana", "etažno", "električno", "radijatori", "podno grijanje"
   - Return the Croatian term as found, or null if unknown

6. **address** (string): Extract the most specific street address.
   - Look for actual street names (ulica, cesta, etc.) in description
   - If no street name found, use the neighborhood/district from location field

7. **description_uk** (string): Translate the listing description to Ukrainian.
   - Translate the full description naturally and fluently into Ukrainian
   - Keep proper nouns (street names, neighborhood names, brand names) as-is
   - Keep numbers, measurements, and prices as-is
   - If description is empty, return empty string

Respond ONLY with a JSON object, no markdown fences:
{
  "no_agency_fee": true | false,
  "has_washing_machine": true | false | null,
  "has_dishwasher": true | false | null,
  "has_air_conditioning": true | false | null,
  "heating_type": "<string or null>",
  "address": "<string>",
  "description_uk": "<string>"
}"""

BATHTUB_PROMPT = """\
You are analyzing a single image from a real estate rental listing.

Your task: determine whether the image shows a bathtub that a person can LIE DOWN IN.

Classify into exactly ONE category:

1. "not_bathroom" — Not a bathroom image.
2. "bathroom_with_bathtub" — Has a bathtub (includes combo units with overhead shower).
3. "bathroom_with_shower_only" — Shower only, no bathtub.
4. "bathroom_unclear" — Bathroom visible but can't determine.

Output STRICTLY as a single JSON object, no markdown fences:
{
  "category": "not_bathroom" | "bathroom_with_bathtub" | "bathroom_with_shower_only" | "bathroom_unclear",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one short sentence>"
}"""

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, base_delay=0.15, backoff_factor=2.0, max_delay=120.0):
        self.delay = base_delay
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self._success_streak = 0

    def wait(self):
        if self.delay > 0:
            time.sleep(self.delay)

    def on_success(self):
        self._success_streak += 1
        if self._success_streak >= 10:
            self.delay = max(self.base_delay, self.delay * 0.9)
            self._success_streak = 0

    def on_rate_limit(self, retry_after=None):
        self._success_streak = 0
        if retry_after and retry_after > self.delay:
            self.delay = min(retry_after + 1.0, self.max_delay)
        else:
            self.delay = min(self.delay * self.backoff_factor, self.max_delay)


# ─── Multi-endpoint client ────────────────────────────────────────────────────

class MultiClientHolder:
    def __init__(self, endpoints=AZURE_ENDPOINTS, base_delay=0.15):
        self._lock = threading.Lock()
        self._credential = AzureCliCredential()
        self._endpoints = endpoints
        self._clients = []
        self._limiters = []
        self._cooldowns = [0.0] * len(endpoints)
        self._names = []
        self._token = ""
        self._expiry = 0.0
        self._refresh_token()
        self._build_clients(base_delay)
        self._counter = itertools.count()

    def _refresh_token(self):
        tok = self._credential.get_token("https://cognitiveservices.azure.com/.default")
        self._token = tok.token
        self._expiry = tok.expires_on
        log.info("Azure AD token refreshed")

    def _build_clients(self, base_delay=0.15):
        self._clients.clear()
        self._limiters.clear()
        self._names.clear()
        for ep in self._endpoints:
            self._clients.append(AzureOpenAI(
                azure_endpoint=ep,
                api_version=API_VERSION,
                azure_ad_token=self._token,
            ))
            self._limiters.append(RateLimiter(base_delay=base_delay))
            self._names.append(ep.split("//")[1].split(".")[0])
        log.info(f"Initialized {len(self._clients)} endpoints: {', '.join(self._names)}")

    def _ensure_token(self):
        if time.time() > self._expiry - 60:
            self._refresh_token()
            self._build_clients()

    def get(self):
        with self._lock:
            self._ensure_token()
            n = len(self._endpoints)
            now = time.time()
            for _ in range(n):
                idx = next(self._counter) % n
                if now >= self._cooldowns[idx]:
                    return self._clients[idx], self._limiters[idx], self._names[idx]
            idx = min(range(n), key=lambda i: self._cooldowns[i])
            wait = self._cooldowns[idx] - now
            if wait > 0:
                time.sleep(wait)
            return self._clients[idx], self._limiters[idx], self._names[idx]

    def cooldown(self, name, seconds=30.0):
        with self._lock:
            for i, n in enumerate(self._names):
                if n == name:
                    self._cooldowns[i] = time.time() + seconds
                    break


def _parse_retry_after(exc):
    try:
        val = exc.response.headers.get("retry-after") or exc.response.headers.get("x-ratelimit-reset-requests")
        if val:
            return float(val)
    except Exception:
        pass
    m = re.search(r"retry after (\d+)", str(exc), re.IGNORECASE)
    return float(m.group(1)) if m else None


# ─── AI Call helpers ──────────────────────────────────────────────────────────

def ai_text_enrich(client_holder, model, listing):
    """Single AI text call to confirm/correct all parsed fields."""
    listing_id = listing["id"]

    # Build context for AI
    parts = []
    parts.append(f"Title: {listing.get('title', '')}")
    parts.append(f"Location: {listing.get('location', '')}")
    parts.append(f"Poster name: {listing.get('agency_name', 'none')}")
    parts.append(f"Price: {listing.get('price', '')}")

    # Show what we parsed deterministically
    parts.append(f"\n--- Our automated extraction (confirm/correct these) ---")
    parts.append(f"has_washing_machine: {listing.get('has_washing_machine')}")
    parts.append(f"has_dishwasher: {listing.get('has_dishwasher')}")
    parts.append(f"has_air_conditioning: {listing.get('has_air_conditioning')}")
    parts.append(f"heating_type: {listing.get('heating_type')}")

    # Tags — authoritative structured data from the website
    tags = listing.get("_tags", {})
    tag_lines = []
    for cat, vals in tags.items():
        for v in vals:
            tag_lines.append(f"  {cat}: {v}")
    if tag_lines:
        parts.append(f"\n--- Structured tags (AUTHORITATIVE — from website dropdowns/checkboxes) ---")
        parts.extend(tag_lines)

    parts.append(f"\n--- Description ---")
    parts.append(listing.get("description", "") or "(no description)")

    user_text = "\n".join(parts)

    kwargs = dict(
        model=model,
        messages=[
            {"role": "system", "content": TEXT_ENRICHMENT_PROMPT},
            {"role": "user", "content": user_text},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=2000,
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
            limiter.on_rate_limit(_parse_retry_after(e))
            consecutive_429s += 1
            if consecutive_429s >= 3:
                client_holder.cooldown(ep_name)
                consecutive_429s = 0
            if attempt == MAX_RETRIES:
                log.warning(f"[{listing_id}] Text AI failed: rate limit")
                return None
            continue
        except APIStatusError as e:
            if e.status_code == 503 and attempt < MAX_RETRIES:
                time.sleep(min(10 * attempt, 60))
                continue
            log.warning(f"[{listing_id}] Text AI error: {e}")
            return None
        except Exception as e:
            log.warning(f"[{listing_id}] Text AI error: {e}")
            return None

        text = (raw or "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
        try:
            return json.loads(text)
        except Exception:
            if attempt == MAX_RETRIES:
                log.warning(f"[{listing_id}] Text AI parse failed: {text[:100]}")
                return None
            continue

    return None


def ai_vision_bathtub(client_holder, model, image_urls):
    """Stream images and check for bathtub. Returns True if bathtub found."""
    loop = asyncio.new_event_loop()

    async def download_images():
        results = []
        async with AsyncSession() as session:
            for url in image_urls[:MAX_IMAGES_PER_LISTING]:
                try:
                    resp = await session.get(url, headers=IMAGE_HEADERS, timeout=15)
                    if resp.status_code == 200:
                        content_type = resp.headers.get("content-type", "image/jpeg")
                        b64 = base64.b64encode(resp.content).decode()
                        results.append((content_type, b64))
                except Exception:
                    continue
        return results

    images = loop.run_until_complete(download_images())
    loop.close()

    if not images:
        return None

    for content_type, b64_data in images:
        kwargs = dict(
            model=model,
            messages=[
                {"role": "system", "content": BATHTUB_PROMPT},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {
                        "url": f"data:{content_type};base64,{b64_data}",
                        "detail": "low",
                    }},
                ]},
            ],
            max_completion_tokens=150,
        )

        for attempt in range(1, MAX_RETRIES + 1):
            client, limiter, ep_name = client_holder.get()
            limiter.wait()
            try:
                resp = client.chat.completions.create(**kwargs)
                raw = resp.choices[0].message.content
                limiter.on_success()
            except RateLimitError as e:
                limiter.on_rate_limit(_parse_retry_after(e))
                if attempt == MAX_RETRIES:
                    break
                continue
            except Exception:
                if attempt == MAX_RETRIES:
                    break
                continue

            text = (raw or "").strip()
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s*```$", "", text)
            try:
                result = json.loads(text)
                if result.get("category") == "bathroom_with_bathtub":
                    return True
            except Exception:
                pass
            break  # Move to next image

    return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def enrich_listing(client_holder, model, listing_row, skip_vision=False):
    """Enrich a single listing: text AI + vision AI."""
    listing_id = listing_row["id"]

    # Build listing dict with tags for context
    listing = dict(listing_row)

    # Load tags from DB
    db_conn = get_db()
    tags_rows = db_conn.execute(
        "SELECT category, value FROM listing_tags WHERE listing_id=?", (listing_id,)
    ).fetchall()
    tags = {}
    for row in tags_rows:
        tags.setdefault(row[0], []).append(row[1])
    listing["_tags"] = tags

    # Load image URLs
    image_rows = db_conn.execute(
        "SELECT url FROM images WHERE listing_id=? ORDER BY position", (listing_id,)
    ).fetchall()
    image_urls = [r[0] for r in image_rows]
    db_conn.close()

    # 1. Text enrichment
    text_result = ai_text_enrich(client_holder, model, listing)

    updates = {}
    if text_result:
        if "no_agency_fee" in text_result:
            updates["no_agency_fee"] = 1 if text_result["no_agency_fee"] else 0
        if "has_washing_machine" in text_result and text_result["has_washing_machine"] is not None:
            updates["has_washing_machine"] = 1 if text_result["has_washing_machine"] else 0
        if "has_dishwasher" in text_result and text_result["has_dishwasher"] is not None:
            updates["has_dishwasher"] = 1 if text_result["has_dishwasher"] else 0
        if "has_air_conditioning" in text_result and text_result["has_air_conditioning"] is not None:
            updates["has_air_conditioning"] = 1 if text_result["has_air_conditioning"] else 0
        if "heating_type" in text_result and text_result["heating_type"]:
            updates["heating_type"] = text_result["heating_type"]
        if "address" in text_result and text_result["address"]:
            updates["street"] = text_result["address"]
        if "description_uk" in text_result and text_result["description_uk"]:
            updates["description_uk"] = text_result["description_uk"]

    # 2. Vision (bathtub) — skip if tags already provide definitive answer
    bathtub_from_tags = None
    feature_tags = tags.get("Funkcionalnosti i ostale karakteristike stana", [])
    if "Kada" in feature_tags:
        bathtub_from_tags = True
    elif any("Tuš" in v for v in feature_tags):
        bathtub_from_tags = False

    if bathtub_from_tags is not None:
        updates["has_bathtub"] = 1 if bathtub_from_tags else 0
        updates["bathtub_analyzed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    elif not skip_vision and image_urls:
        has_bathtub = ai_vision_bathtub(client_holder, model, image_urls)
        if has_bathtub is not None:
            updates["has_bathtub"] = 1 if has_bathtub else 0
            updates["bathtub_analyzed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    # Write updates
    if updates:
        updates["ai_enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        set_clause = ", ".join(f"{k}=?" for k in updates.keys())
        db_conn = get_db()
        db_conn.execute(
            f"UPDATE listings SET {set_clause} WHERE id=?",
            list(updates.values()) + [listing_id],
        )
        db_conn.commit()
        db_conn.close()

    return listing_id, updates


def main():
    parser = argparse.ArgumentParser(description="AI enrichment for listings")
    parser.add_argument("--limit", type=int, default=0, help="Max listings to process (0=all)")
    parser.add_argument("--skip-vision", action="store_true", help="Skip bathtub vision analysis")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Azure OpenAI model deployment name")
    parser.add_argument("--max-age", type=int, default=MAX_LISTING_AGE_DAYS,
                        help=f"Max listing age in days based on published_at (default: {MAX_LISTING_AGE_DAYS}, 0=no limit)")
    args = parser.parse_args()

    log.info("Starting AI enrichment...")

    # Load listings needing enrichment
    db_conn = get_db()
    query = "SELECT * FROM listings WHERE ai_enriched_at IS NULL"
    if args.limit:
        query += f" LIMIT {args.limit}"
    rows = db_conn.execute(query).fetchall()
    db_conn.close()

    # Age filter: drop listings published more than max_age days ago
    if args.max_age > 0:
        cutoff = datetime.now() - timedelta(days=args.max_age)
        filtered_rows = []
        for row in rows:
            pub_raw = row["published_at"] or ""
            pub_str = pub_raw.split(" u ")[0].strip().rstrip(".")
            if pub_str:
                try:
                    pub_dt = datetime.strptime(pub_str, "%d.%m.%Y")
                    if pub_dt < cutoff:
                        continue
                except ValueError:
                    pass
            filtered_rows.append(row)
        skipped = len(rows) - len(filtered_rows)
        if skipped:
            log.info(f"  Age filter (>{args.max_age}d): skipped {skipped} old listings")
        rows = filtered_rows

    if not rows:
        log.info("No listings need enrichment.")
        return

    log.info(f"Found {len(rows)} listings to enrich")

    # Initialize AI clients
    client_holder = MultiClientHolder()

    # Process with thread pool
    completed = 0
    failed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=AI_TEXT_WORKERS) as executor:
        futures = {
            executor.submit(enrich_listing, client_holder, args.model, row, args.skip_vision): row["id"]
            for row in rows
        }
        for future in as_completed(futures):
            listing_id = futures[future]
            try:
                lid, updates = future.result()
                completed += 1
                elapsed = time.time() - start_time
                log.info(
                    f"[{completed}/{len(rows)}] {lid}: "
                    f"{len(updates)} fields updated ({elapsed:.0f}s elapsed)"
                )
            except Exception as e:
                failed += 1
                log.error(f"[{listing_id}] Error: {e}")

    elapsed = time.time() - start_time
    log.info(f"Done. Enriched {completed}, failed {failed} in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
