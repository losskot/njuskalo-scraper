#!/usr/bin/env python3
"""
Bathtub classifier: sends every local JPEG to each deployed Azure OpenAI vision model
and stores results in SQLite. Then generates a per-model HTML report.

Usage:
    python bathtub_classifier.py [--images-dir PATH] [--db PATH] [--models m1 m2 ...]
                                  [--limit N] [--workers N] [--html-out PATH]

Auth: uses Azure CLI credentials (az login already done).
"""

import argparse
import base64
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone

from azure.identity import AzureCliCredential, DefaultAzureCredential, get_bearer_token_provider
from openai import AzureOpenAI, OpenAI, RateLimitError, APIStatusError
from PIL import Image

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AZURE_ENDPOINT = "https://admin-ml8gx7ra-eastus2.cognitiveservices.azure.com/"
API_VERSION    = "2025-01-01-preview"

VISION_MODELS = [
    "gpt-5-mini",
    "gpt-5.2-chat",
    "gpt-5.3-chat",
    "gpt-4o",
    "gpt-5.4",
    "gpt-5.5",
    "o1",
    "gpt-5.4-pro",
]

IMAGES_DIR = Path(__file__).parent / "backend" / "images"
DB_PATH    = Path(__file__).parent / "backend" / "bathtub_results.db"
HTML_OUT   = Path(__file__).parent / "backend" / "website" / "bathtub_report.html"
CHECKPOINTS_DIR = Path(__file__).parent / "checkpoints"

VALID_CATEGORIES = {
    "not_bathroom",
    "bathroom_with_bathtub",
    "bathroom_with_shower_only",
    "bathroom_unclear",
}

SYSTEM_PROMPT = """\
You are analyzing a single image from a real estate rental listing.

Your task: determine whether the image shows a bathtub that a person can LIE DOWN IN (recline horizontally with legs extended). A standing-only shower does NOT count.

Classify the image into exactly ONE of these four categories:

1. "not_bathroom"
   The image does not show a bathroom, or the bathroom is not the primary subject (e.g., bedroom, kitchen, living room, hallway, exterior, balcony, floor plan, empty room, decorative shot, etc.). Use this even if a tiny corner of a bathroom is visible in the background.

2. "bathroom_with_bathtub"
   The image shows a bathroom that contains a bathtub a person can lie down in. This INCLUDES:
   - Standard built-in rectangular bathtubs
   - Freestanding bathtubs (clawfoot, modern oval, stone)
   - Corner bathtubs
   - Combination units: rectangular bathtub WITH an overhead shower / shower screen / shower curtain (very common in older European apartments) — the tub portion still allows lying down, so this counts as bathtub
   - Japanese-style deep soaking tubs (ofuro) if long enough to sit with legs extended
   - Whirlpool / jacuzzi tubs

   EXCLUDES:
   - Walk-in tubs with a vertical door and built-in seat where you can only sit upright — these do NOT count as "lie down" tubs, classify as "bathroom_with_shower_only" if there is a shower, otherwise "bathroom_unclear".
   - Sitz baths, foot baths, small basins.

3. "bathroom_with_shower_only"
   The image shows a bathroom with no bathtub, only a shower:
   - Walk-in shower stall / shower cabin with glass doors
   - Wet room (tiled floor with drain, no tub, no enclosure)
   - Shower tray with curtain
   - Open rainfall shower over tiled floor
   - Walk-in seated tub (see exclusion above)

4. "bathroom_unclear"
   The image shows a bathroom but you cannot determine whether a bathtub is present (e.g., only sink/toilet/mirror visible, camera angle hides the wet area, image too dark or blurry to tell).

CRITICAL DECISION RULES:
- A bathtub WITH a shower above it = "bathroom_with_bathtub" (the lying-down surface still exists).
- A shower-only stall in a bathroom that also has a separate visible bathtub = "bathroom_with_bathtub".
- If you are less than 75% confident in a positive category, use "bathroom_unclear" instead of guessing.
- Do NOT infer the existence of a bathtub from text, captions, or floor plans embedded in the image — only from visible plumbing fixtures.

Output STRICTLY as a single JSON object, no markdown fences, no commentary before or after:

{
  "category": "not_bathroom" | "bathroom_with_bathtub" | "bathroom_with_shower_only" | "bathroom_unclear",
  "confidence": <float between 0.0 and 1.0>,
  "visible_fixtures": [<list of visible fixtures: "bathtub", "shower", "toilet", "sink", "bidet", "washing_machine", "radiator", "mirror", "other">],
  "reasoning": "<one short sentence describing what you see and why you chose the category>"
}"""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            model         TEXT    NOT NULL,
            listing_id    TEXT    NOT NULL,
            image_file    TEXT    NOT NULL,
            image_path    TEXT    NOT NULL,
            category      TEXT,
            confidence    REAL,
            fixtures      TEXT,   -- JSON array
            reasoning     TEXT,
            raw_response  TEXT,
            error         TEXT,
            latency_ms    INTEGER,
            created_at    TEXT    DEFAULT (datetime('now')),
            UNIQUE (model, image_path)
        );
        CREATE INDEX IF NOT EXISTS idx_results_model     ON results(model);
        CREATE INDEX IF NOT EXISTS idx_results_category  ON results(category);
        CREATE INDEX IF NOT EXISTS idx_results_listing   ON results(listing_id);
    """)
    conn.commit()
    return conn


def is_already_done(conn: sqlite3.Connection, model: str, image_path: str) -> bool:
    row = conn.execute(
        "SELECT id FROM results WHERE model=? AND image_path=?",
        (model, image_path)
    ).fetchone()
    return row is not None


def save_result(conn: sqlite3.Connection, **kwargs):
    conn.execute("""
        INSERT OR REPLACE INTO results
            (model, listing_id, image_file, image_path,
             category, confidence, fixtures, reasoning,
             raw_response, error, latency_ms)
        VALUES
            (:model, :listing_id, :image_file, :image_path,
             :category, :confidence, :fixtures, :reasoning,
             :raw_response, :error, :latency_ms)
    """, kwargs)
    conn.commit()

# ---------------------------------------------------------------------------
# Thread-safe Azure OpenAI client with auto token refresh
# ---------------------------------------------------------------------------

class ClientHolder:
    """Shared across threads. Refreshes the AD token before expiry."""
    def __init__(self):
        self._lock = threading.Lock()
        self._credential = AzureCliCredential()
        self._refresh()

    def _refresh(self):
        token = self._credential.get_token("https://cognitiveservices.azure.com/.default")
        self._client = AzureOpenAI(
            azure_endpoint=AZURE_ENDPOINT,
            api_version=API_VERSION,
            azure_ad_token=token.token,
        )
        self._expiry = token.expires_on
        log.info("AzureCliCredential.get_token succeeded")

    def get(self) -> AzureOpenAI:
        with self._lock:
            if time.time() > self._expiry - 60:
                log.info("Refreshing Azure AD token...")
                self._refresh()
            return self._client

# ---------------------------------------------------------------------------
# Batch API client  (uses OpenAI client with v1 URL for files/batches API)
# ---------------------------------------------------------------------------

_AZURE_RESOURCE_NAME = AZURE_ENDPOINT.rstrip("/").split("//")[1].split(".")[0]

class BatchClientHolder:
    """Client for Azure OpenAI Batch API (files.create, batches.create, etc.).
    Uses the OpenAI SDK with the v1 base URL pattern required by the batch API."""
    def __init__(self):
        self._token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )
        self._client = OpenAI(
            base_url=f"https://{_AZURE_RESOURCE_NAME}.openai.azure.com/openai/v1/",
            api_key=self._token_provider(),
        )
        log.info("BatchClientHolder initialised (v1 URL)")

    def get(self) -> OpenAI:
        # Refresh the api_key (bearer token) on each access
        self._client.api_key = self._token_provider()
        return self._client

# ---------------------------------------------------------------------------
# Image resizing  (max 2048px longest side, JPEG q85)
# ---------------------------------------------------------------------------

MAX_IMAGE_DIM = 2048

def resize_and_encode(path: Path, max_dim: int = MAX_IMAGE_DIM) -> str:
    """Open image, resize longest side to max_dim (if larger), return base64 JPEG."""
    img = Image.open(path)
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    w, h = img.size
    if max(w, h) > max_dim:
        scale = max_dim / max(w, h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.b64encode(buf.getvalue()).decode("utf-8")

# ---------------------------------------------------------------------------
# Adaptive rate-limit state (per model)
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Tracks 429 hits per model and adapts the inter-request delay.
    Strategy:
      - Start at base_delay seconds between requests.
      - On 429: back off exponentially (delay *= backoff_factor), cap at max_delay.
      - After a successful window (success_streak calls), gently reduce delay.
      - Respects Retry-After header when present.
    """
    def __init__(
        self,
        base_delay: float = 0.3,
        backoff_factor: float = 2.0,
        max_delay: float = 120.0,
        reduce_factor: float = 0.9,
        success_streak_threshold: int = 10,
    ):
        self.delay               = base_delay
        self.base_delay          = base_delay
        self.backoff_factor      = backoff_factor
        self.max_delay           = max_delay
        self.reduce_factor       = reduce_factor
        self.success_streak_threshold = success_streak_threshold
        self._success_streak     = 0
        self._total_429s         = 0

    def wait(self):
        if self.delay > 0:
            time.sleep(self.delay)

    def on_success(self):
        self._success_streak += 1
        if self._success_streak >= self.success_streak_threshold:
            old = self.delay
            self.delay = max(self.base_delay, self.delay * self.reduce_factor)
            self._success_streak = 0
            if self.delay < old - 0.01:
                log.debug(f"Rate limiter eased: {old:.1f}s → {self.delay:.1f}s")

    def on_rate_limit(self, retry_after: float | None = None):
        self._total_429s += 1
        self._success_streak = 0
        if retry_after and retry_after > self.delay:
            self.delay = min(retry_after + 1.0, self.max_delay)
        else:
            self.delay = min(self.delay * self.backoff_factor, self.max_delay)
        log.warning(f"429 hit (total={self._total_429s}). Next delay: {self.delay:.1f}s")


# Global per-model limiters
_rate_limiters: dict[str, RateLimiter] = {}

def get_limiter(model: str) -> RateLimiter:
    if model not in _rate_limiters:
        _rate_limiters[model] = RateLimiter()
    return _rate_limiters[model]


def _parse_retry_after(exc: Exception) -> float | None:
    """Extract Retry-After seconds from a 429 exception."""
    # openai SDK exposes response headers via exc.response
    try:
        headers = exc.response.headers  # type: ignore[attr-defined]
        val = headers.get("retry-after") or headers.get("x-ratelimit-reset-requests")
        if val:
            return float(val)
    except Exception:
        pass
    # Also try parsing from the message string
    try:
        m = re.search(r'retry after (\d+)', str(exc), re.IGNORECASE)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Image encoding
# ---------------------------------------------------------------------------

def encode_image(path: Path) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

# ---------------------------------------------------------------------------
# Classify one image  (with 429-aware retry)
# ---------------------------------------------------------------------------

MAX_RETRIES = 8

def classify_image(
    client: AzureOpenAI,
    model: str,
    image_path: Path,
    limiter: RateLimiter,
    b64: str | None = None,         # pass pre-encoded to avoid re-reading disk
) -> dict:
    if b64 is None:
        b64 = encode_image(image_path)

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
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                    },
                ],
            }
        ],
        response_format={"type": "json_object"},
    )

    # GPT-4o uses max_tokens; GPT-5.x and o-series require max_completion_tokens
    if model.startswith("gpt-4"):
        kwargs["max_tokens"] = 512
    else:
        kwargs["max_completion_tokens"] = 512

    # Only gpt-4o supports temperature=0; o1 and gpt-5.x series only allow default (1)
    if model.startswith("gpt-4"):
        kwargs["temperature"] = 0

    raw        = None
    latency_ms = 0

    for attempt in range(1, MAX_RETRIES + 1):
        # Adaptive inter-request wait
        limiter.wait()
        t0 = time.monotonic()
        try:
            response   = client.chat.completions.create(**kwargs)
            latency_ms = int((time.monotonic() - t0) * 1000)
            raw        = response.choices[0].message.content
            limiter.on_success()

        except RateLimitError as e:
            latency_ms = int((time.monotonic() - t0) * 1000)
            retry_after = _parse_retry_after(e)
            limiter.on_rate_limit(retry_after)
            if attempt == MAX_RETRIES:
                return dict(
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=None,
                    error=f"RateLimitError after {MAX_RETRIES} attempts: {e}",
                    latency_ms=latency_ms,
                )
            wait = limiter.delay
            log.warning(f"  429 on attempt {attempt}/{MAX_RETRIES}, sleeping {wait:.1f}s then retrying...")
            time.sleep(wait)
            continue

        except APIStatusError as e:
            latency_ms = int((time.monotonic() - t0) * 1000)
            if e.status_code == 503 and attempt < MAX_RETRIES:
                wait = min(10 * attempt, 60)
                log.warning(f"  503 on attempt {attempt}/{MAX_RETRIES}, sleeping {wait}s...")
                time.sleep(wait)
                continue
            return dict(
                category=None, confidence=None, fixtures=None,
                reasoning=None, raw_response=None,
                error=f"APIStatusError {e.status_code}: {e}",
                latency_ms=latency_ms,
            )

        # --- Parse JSON (retry loop if malformed) ---
        text = (raw or "").strip()
        # Strip markdown fences: ```json ... ``` or ``` ... ```
        text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*```$', '', text)
        text = text.strip()
        try:
            data = json.loads(text)
            category   = data.get("category", "")
            confidence = float(data.get("confidence", 0.0))
            fixtures   = json.dumps(data.get("visible_fixtures", []))
            reasoning  = data.get("reasoning", "")

            if category not in VALID_CATEGORIES:
                raise ValueError(f"Unknown category: {category!r}")

            return dict(
                category=category,
                confidence=confidence,
                fixtures=fixtures,
                reasoning=reasoning,
                raw_response=raw,
                error=None,
                latency_ms=latency_ms,
            )
        except Exception as parse_err:
            log.warning(
                f"  JSON parse failed on attempt {attempt}/{MAX_RETRIES} "
                f"({type(parse_err).__name__}: {parse_err}) — "
                f"raw={repr((raw or '')[:120])} — retrying..."
            )
            if attempt == MAX_RETRIES:
                return dict(
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=raw,
                    error=f"JSONParseError after {MAX_RETRIES} attempts: {parse_err}",
                    latency_ms=latency_ms,
                )
            # No extra sleep — limiter.wait() at top of next iteration handles pacing
            continue

    # Should never reach here
    return dict(
        category=None, confidence=None, fixtures=None,
        reasoning=None, raw_response=raw,
        error="classify_image: exhausted retries unexpectedly",
        latency_ms=latency_ms,
    )

# ---------------------------------------------------------------------------
# Collect all images
# ---------------------------------------------------------------------------

def collect_images(images_dir: Path) -> list[dict]:
    images = []
    for listing_dir in sorted(images_dir.iterdir()):
        if not listing_dir.is_dir():
            continue
        listing_id = listing_dir.name
        for img in sorted(listing_dir.glob("*.jpg")):
            images.append({
                "listing_id": listing_id,
                "image_file": img.name,
                "image_path": str(img),
                "path_obj":   img,
            })
    return images

# ---------------------------------------------------------------------------
# Run classification — one thread per model, images processed in parallel
# ---------------------------------------------------------------------------

def _build_b64_cache(images: list[dict]) -> dict[str, str]:
    """Encode all images to base64 once, shared across all model threads."""
    log.info(f"Pre-encoding {len(images)} images to base64...")
    cache = {}
    for img in images:
        cache[img["image_path"]] = base64.b64encode(
            img["path_obj"].read_bytes()
        ).decode("utf-8")
    log.info("Pre-encoding done.")
    return cache


def run_all(models: list[str], images: list[dict], conn: sqlite3.Connection,
            limit: int | None = None):

    if limit:
        images = images[:limit]

    # Shared resources
    holder   = ClientHolder()
    db_lock  = threading.Lock()
    b64_cache = _build_b64_cache(images)

    def process_model(model: str):
        limiter    = get_limiter(model)
        model_done = 0
        log.info(f"=== [{model}] starting {len(images)} images ===")

        for img in images:
            # Skip if already done (thread-safe read)
            with db_lock:
                if is_already_done(conn, model, img["image_path"]):
                    model_done += 1
                    continue

            client = holder.get()
            try:
                result = classify_image(
                    client, model, img["path_obj"], limiter,
                    b64=b64_cache[img["image_path"]],
                )
            except Exception as e:
                result = dict(
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=None,
                    error=f"{type(e).__name__}: {e}",
                    latency_ms=0,
                )
                log.error(f"  [{model}] {img['image_file']}: {result['error']}")

            with db_lock:
                save_result(
                    conn,
                    model=model,
                    listing_id=img["listing_id"],
                    image_file=img["image_file"],
                    image_path=img["image_path"],
                    **result,
                )

            model_done += 1
            cat      = result.get("category") or "ERROR"
            conf     = result.get("confidence")
            conf_str = f"{conf:.2f}" if conf is not None else "n/a"
            log.info(
                f"  [{model}][{model_done}/{len(images)}] "
                f"{img['listing_id']}/{img['image_file'][:35]} "
                f"→ {cat} ({conf_str})  {result['latency_ms']}ms  [delay={limiter.delay:.1f}s]"
            )

        log.info(
            f"=== [{model}] done {model_done} | "
            f"429s={limiter._total_429s} delay={limiter.delay:.1f}s ==="
        )

    log.info(f"Launching {len(models)} model threads in parallel...")
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        futures = {executor.submit(process_model, m): m for m in models}
        for future in as_completed(futures):
            model = futures[future]
            try:
                future.result()
            except Exception as e:
                log.error(f"Thread for {model} crashed: {e}")

# ---------------------------------------------------------------------------
# Batch API — JSONL builder
# ---------------------------------------------------------------------------

MAX_JSONL_BYTES = 180 * 1024 * 1024  # 180 MB split threshold

def _build_chat_body(model: str, b64: str) -> dict:
    """Build the chat completions request body for a single image (same logic as classify_image)."""
    body: dict = dict(
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
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                    },
                ],
            }
        ],
        response_format={"type": "json_object"},
    )
    if model.startswith("gpt-4"):
        body["max_tokens"] = 512
        body["temperature"] = 0
    else:
        body["max_completion_tokens"] = 512
    return body


def build_batch_jsonl(
    model: str,
    images: list[dict],
    conn: sqlite3.Connection,
    db_lock: threading.Lock | None = None,
) -> list[Path]:
    """Build JSONL file(s) for the given model, skipping already-done images.
    Returns list of written file paths (empty if nothing to do)."""
    CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_model = model.replace(".", "_").replace("-", "_")

    paths: list[Path] = []
    chunk_idx = 0
    current_size = 0
    current_fh = None

    def _open_chunk():
        nonlocal chunk_idx, current_size, current_fh
        if current_fh:
            current_fh.close()
        suffix = f"_{chunk_idx}" if chunk_idx > 0 else ""
        p = CHECKPOINTS_DIR / f"batch_{safe_model}_{ts}{suffix}.jsonl"
        current_fh = open(p, "w", encoding="utf-8")
        paths.append(p)
        current_size = 0
        chunk_idx += 1

    written = 0
    skipped = 0
    for img in images:
        if db_lock:
            with db_lock:
                done = is_already_done(conn, model, img["image_path"])
        else:
            done = is_already_done(conn, model, img["image_path"])
        if done:
            skipped += 1
            continue

        b64 = resize_and_encode(img["path_obj"])
        body = _build_chat_body(model, b64)
        line = json.dumps({
            "custom_id": f"{img['listing_id']}::{img['image_path']}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        }, ensure_ascii=False)

        line_bytes = len(line.encode("utf-8")) + 1  # +1 for newline

        if current_fh is None or (current_size + line_bytes > MAX_JSONL_BYTES):
            _open_chunk()

        current_fh.write(line + "\n")
        current_size += line_bytes
        written += 1

    if current_fh:
        current_fh.close()

    # Remove empty files (shouldn't happen, but be safe)
    paths = [p for p in paths if p.stat().st_size > 0]

    log.info(f"[{model}] Built {len(paths)} JSONL file(s): {written} images, {skipped} skipped (already done)")
    for p in paths:
        log.info(f"  → {p}  ({p.stat().st_size / 1024 / 1024:.1f} MB)")
    return paths

# ---------------------------------------------------------------------------
# Batch API — submit
# ---------------------------------------------------------------------------

def submit_batches(
    models: list[str],
    images: list[dict],
    conn: sqlite3.Connection,
) -> Path:
    """Build JSONL files and submit batch jobs for all models.
    Returns path to the tracking JSON file."""
    holder = BatchClientHolder()
    client = holder.get()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    tracking_path = CHECKPOINTS_DIR / f"batch_jobs_{ts}.json"

    tracking: dict[str, dict] = {}

    for model in models:
        jsonl_paths = build_batch_jsonl(model, images, conn)
        if not jsonl_paths:
            log.info(f"[{model}] Nothing to submit (all images already done)")
            continue

        for jsonl_path in jsonl_paths:
            client = holder.get()  # refresh token
            log.info(f"[{model}] Uploading {jsonl_path.name} ...")
            with open(jsonl_path, "rb") as f:
                file_obj = client.files.create(
                    file=f,
                    purpose="batch",
                    extra_body={"expires_after": {"seconds": 1209600, "anchor": "created_at"}},
                )
            log.info(f"[{model}] File uploaded: {file_obj.id}")

            batch_resp = client.batches.create(
                input_file_id=file_obj.id,
                endpoint="/chat/completions",
                completion_window="24h",
            )
            log.info(f"[{model}] Batch submitted: {batch_resp.id} (status={batch_resp.status})")

            tracking[batch_resp.id] = {
                "model": model,
                "status": batch_resp.status,
                "input_file_id": file_obj.id,
                "jsonl_path": str(jsonl_path),
                "submitted_at": datetime.now(timezone.utc).isoformat(),
            }

    tracking_path.write_text(json.dumps(tracking, indent=2), encoding="utf-8")
    log.info(f"Tracking file: {tracking_path}  ({len(tracking)} batch job(s))")
    return tracking_path

# ---------------------------------------------------------------------------
# Batch API — poll
# ---------------------------------------------------------------------------

POLL_INTERVAL = 60  # seconds

def poll_batches(tracking_path: Path) -> dict:
    """Poll all batch jobs until they reach a terminal state. Updates tracking file in-place."""
    tracking = json.loads(tracking_path.read_text())
    if not tracking:
        log.info("No batch jobs to poll.")
        return tracking

    holder = BatchClientHolder()

    terminal = {"completed", "failed", "cancelled", "expired"}
    while True:
        pending = []
        for batch_id, info in tracking.items():
            if info["status"] in terminal:
                continue
            pending.append(batch_id)

        if not pending:
            log.info("All batch jobs have reached a terminal state.")
            break

        log.info(f"Polling {len(pending)} pending batch job(s)...")
        client = holder.get()
        for batch_id in pending:
            resp = client.batches.retrieve(batch_id)
            old_status = tracking[batch_id]["status"]
            tracking[batch_id]["status"] = resp.status
            if resp.output_file_id:
                tracking[batch_id]["output_file_id"] = resp.output_file_id
            if resp.error_file_id:
                tracking[batch_id]["error_file_id"] = resp.error_file_id
            if resp.request_counts:
                tracking[batch_id]["request_counts"] = {
                    "total": resp.request_counts.total,
                    "completed": resp.request_counts.completed,
                    "failed": resp.request_counts.failed,
                }
            model = tracking[batch_id]["model"]
            counts = tracking[batch_id].get("request_counts", {})
            log.info(
                f"  [{model}] {batch_id}: {old_status} → {resp.status}  "
                f"(completed={counts.get('completed', '?')}/{counts.get('total', '?')})"
            )

        # Save progress
        tracking_path.write_text(json.dumps(tracking, indent=2), encoding="utf-8")

        # Check again if all done
        if all(tracking[bid]["status"] in terminal for bid in tracking):
            log.info("All batch jobs have reached a terminal state.")
            break

        log.info(f"Waiting {POLL_INTERVAL}s before next poll...")
        time.sleep(POLL_INTERVAL)

    return tracking

# ---------------------------------------------------------------------------
# Batch API — collect results
# ---------------------------------------------------------------------------

def _parse_batch_response(raw_text: str) -> dict:
    """Parse the model output from a batch response (same logic as classify_image)."""
    text = (raw_text or "").strip()
    text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()
    try:
        data = json.loads(text)
        category = data.get("category", "")
        confidence = float(data.get("confidence", 0.0))
        fixtures = json.dumps(data.get("visible_fixtures", []))
        reasoning = data.get("reasoning", "")
        if category not in VALID_CATEGORIES:
            raise ValueError(f"Unknown category: {category!r}")
        return dict(
            category=category, confidence=confidence, fixtures=fixtures,
            reasoning=reasoning, raw_response=raw_text, error=None, latency_ms=0,
        )
    except Exception as e:
        return dict(
            category=None, confidence=None, fixtures=None,
            reasoning=None, raw_response=raw_text,
            error=f"JSONParseError: {e}", latency_ms=0,
        )


def collect_results(tracking_path: Path, conn: sqlite3.Connection):
    """Download results from completed batch jobs and insert into the database."""
    tracking = json.loads(tracking_path.read_text())
    holder = BatchClientHolder()
    client = holder.get()

    total_saved = 0
    total_errors = 0

    for batch_id, info in tracking.items():
        model = info["model"]
        status = info["status"]

        if status != "completed":
            counts = info.get("request_counts", {})
            log.warning(f"[{model}] Batch {batch_id} status={status}, skipping. "
                        f"(failed={counts.get('failed', '?')})")
            # Try to download error file
            error_file_id = info.get("error_file_id")
            if error_file_id:
                try:
                    client = holder.get()
                    err_content = client.files.content(error_file_id)
                    for line in err_content.text.strip().split("\n"):
                        if line.strip():
                            log.error(f"  [error] {line[:300]}")
                except Exception as e:
                    log.error(f"  Could not download error file: {e}")
            continue

        output_file_id = info.get("output_file_id")
        if not output_file_id:
            log.warning(f"[{model}] Batch {batch_id} completed but no output_file_id")
            continue

        log.info(f"[{model}] Downloading results from {batch_id} ...")
        client = holder.get()
        file_response = client.files.content(output_file_id)
        lines = file_response.text.strip().split("\n")
        batch_saved = 0
        batch_errors = 0

        for line in lines:
            if not line.strip():
                continue
            try:
                resp_obj = json.loads(line)
            except json.JSONDecodeError as e:
                log.error(f"  Failed to parse result line: {e}")
                batch_errors += 1
                continue

            custom_id = resp_obj.get("custom_id", "")
            # Parse custom_id: "{listing_id}::{image_path}"
            parts = custom_id.split("::", 1)
            if len(parts) != 2:
                log.error(f"  Invalid custom_id format: {custom_id[:100]}")
                batch_errors += 1
                continue
            listing_id, image_path = parts
            image_file = Path(image_path).name

            # Check for request-level error
            error = resp_obj.get("error")
            if error:
                save_result(
                    conn, model=model, listing_id=listing_id,
                    image_file=image_file, image_path=image_path,
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=json.dumps(error),
                    error=f"BatchError: {json.dumps(error)}", latency_ms=0,
                )
                batch_errors += 1
                continue

            # Extract the assistant message content
            response_body = resp_obj.get("response", {}).get("body", {})
            status_code = resp_obj.get("response", {}).get("status_code", 0)
            if status_code != 200:
                save_result(
                    conn, model=model, listing_id=listing_id,
                    image_file=image_file, image_path=image_path,
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=json.dumps(response_body),
                    error=f"HTTP {status_code}", latency_ms=0,
                )
                batch_errors += 1
                continue

            choices = response_body.get("choices", [])
            if not choices:
                save_result(
                    conn, model=model, listing_id=listing_id,
                    image_file=image_file, image_path=image_path,
                    category=None, confidence=None, fixtures=None,
                    reasoning=None, raw_response=json.dumps(response_body),
                    error="No choices in response", latency_ms=0,
                )
                batch_errors += 1
                continue

            raw_text = choices[0].get("message", {}).get("content", "")
            result = _parse_batch_response(raw_text)

            save_result(
                conn, model=model, listing_id=listing_id,
                image_file=image_file, image_path=image_path,
                **result,
            )
            if result["error"]:
                batch_errors += 1
            else:
                batch_saved += 1

        total_saved += batch_saved
        total_errors += batch_errors
        log.info(f"[{model}] Batch {batch_id}: {batch_saved} saved, {batch_errors} errors")

        # Clean up: delete the uploaded input file to stay under the 500-file limit
        input_file_id = info.get("input_file_id")
        if input_file_id:
            try:
                client = holder.get()
                client.files.delete(input_file_id)
                log.info(f"[{model}] Deleted input file {input_file_id}")
            except Exception as e:
                log.warning(f"[{model}] Could not delete input file {input_file_id}: {e}")

    log.info(f"Collection complete: {total_saved} results saved, {total_errors} errors across all batches")

# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def build_html(conn: sqlite3.Connection, models: list[str], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Stats per model
    stats = {}
    for model in models:
        rows = conn.execute("""
            SELECT category, COUNT(*) AS n FROM results
            WHERE model=? AND error IS NULL
            GROUP BY category
        """, (model,)).fetchall()
        stats[model] = {r["category"]: r["n"] for r in rows}
        stats[model]["_total"] = conn.execute(
            "SELECT COUNT(*) FROM results WHERE model=?", (model,)
        ).fetchone()[0]
        stats[model]["_errors"] = conn.execute(
            "SELECT COUNT(*) FROM results WHERE model=? AND error IS NOT NULL", (model,)
        ).fetchone()[0]

    # Bathtub images per model
    bathtub_per_model: dict[str, list[sqlite3.Row]] = {}
    for model in models:
        rows = conn.execute("""
            SELECT listing_id, image_file, image_path, confidence, reasoning, fixtures
            FROM results
            WHERE model=? AND category='bathroom_with_bathtub'
            ORDER BY listing_id, image_file
        """, (model,)).fetchall()
        bathtub_per_model[model] = rows

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Build tab HTML
    tabs_nav = []
    tabs_content = []

    for i, model in enumerate(models):
        active = "active" if i == 0 else ""
        show   = "show active" if i == 0 else ""
        tab_id = model.replace(".", "_").replace("-", "_")

        s = stats.get(model, {})
        total  = s.get("_total", 0)
        errors = s.get("_errors", 0)
        tub    = s.get("bathroom_with_bathtub", 0)
        shower = s.get("bathroom_with_shower_only", 0)
        no_br  = s.get("not_bathroom", 0)
        uncl   = s.get("bathroom_unclear", 0)

        tabs_nav.append(f"""
            <li class="nav-item" role="presentation">
              <button class="nav-link {active}" id="tab-{tab_id}-btn"
                      data-bs-toggle="tab" data-bs-target="#tab-{tab_id}"
                      type="button" role="tab">
                {model}
                <span class="badge bg-success ms-1">{tub}</span>
              </button>
            </li>""")

        rows = bathtub_per_model.get(model, [])
        cards_html = ""
        for row in rows:
            img_path  = row["image_path"]
            conf      = row["confidence"] or 0.0
            reasoning = row["reasoning"] or ""
            fixtures  = json.loads(row["fixtures"] or "[]")
            rel_path  = os.path.relpath(img_path, out_path.parent)

            cards_html += f"""
              <div class="col-6 col-md-4 col-lg-3 mb-3">
                <div class="card h-100 shadow-sm">
                  <img src="{rel_path}" class="card-img-top" style="height:180px;object-fit:cover;"
                       loading="lazy" alt="{row['image_file']}">
                  <div class="card-body p-2">
                    <p class="card-text small mb-1">
                      <strong>Listing:</strong> {row['listing_id']}<br>
                      <strong>Conf:</strong> {conf:.2f}<br>
                      <strong>Fixtures:</strong> {', '.join(fixtures) if fixtures else '—'}
                    </p>
                    <p class="card-text text-muted" style="font-size:.75rem;">{reasoning}</p>
                  </div>
                </div>
              </div>"""

        if not rows:
            cards_html = '<p class="text-muted">No bathtub images found.</p>'

        tabs_content.append(f"""
          <div class="tab-pane fade {show}" id="tab-{tab_id}" role="tabpanel">
            <div class="row mb-3 mt-2 g-2">
              <div class="col-auto">
                <span class="badge bg-secondary">Total: {total}</span>
                <span class="badge bg-success">Bathtub: {tub}</span>
                <span class="badge bg-primary">Shower only: {shower}</span>
                <span class="badge bg-light text-dark border">Not bathroom: {no_br}</span>
                <span class="badge bg-warning text-dark">Unclear: {uncl}</span>
                <span class="badge bg-danger">Errors: {errors}</span>
              </div>
            </div>
            <div class="row">
              {cards_html}
            </div>
          </div>""")

    tabs_nav_html     = "\n".join(tabs_nav)
    tabs_content_html = "\n".join(tabs_content)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bathtub Classifier Results</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <style>
    body {{ background: #f8f9fa; }}
    .nav-link {{ font-size: .85rem; }}
    .card-img-top {{ border-bottom: 1px solid #dee2e6; }}
  </style>
</head>
<body>
<div class="container-fluid py-3">
  <h4 class="mb-1">🛁 Bathtub Classifier — Model Comparison</h4>
  <p class="text-muted small mb-3">Generated: {now} &nbsp;|&nbsp;
     Images dir: <code>backend/images</code> &nbsp;|&nbsp;
     DB: <code>backend/bathtub_results.db</code>
  </p>

  <ul class="nav nav-tabs" id="modelTabs" role="tablist">
    {tabs_nav_html}
  </ul>

  <div class="tab-content border border-top-0 rounded-bottom bg-white p-3" id="modelTabsContent">
    {tabs_content_html}
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    log.info(f"HTML report written → {out_path}")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Classify bathroom images across Azure OpenAI models")
    p.add_argument("--images-dir", default=str(IMAGES_DIR), help="Root images directory")
    p.add_argument("--db",         default=str(DB_PATH),    help="SQLite database path")
    p.add_argument("--html-out",   default=str(HTML_OUT),   help="Output HTML report path")
    p.add_argument("--models",     nargs="+", default=VISION_MODELS, help="Models to test")
    p.add_argument("--limit",      type=int,  default=None, help="Limit number of images (for testing)")
    p.add_argument("--html-only",  action="store_true",     help="Skip classification, just regenerate HTML from DB")
    p.add_argument("--batch",      choices=["submit", "poll", "collect", "run"],
                   default=None,   help="Batch API mode: submit/poll/collect or run (all three)")
    p.add_argument("--tracking-file", default=None,
                   help="Path to batch tracking JSON (for poll/collect; defaults to most recent)")
    return p.parse_args()


def _find_latest_tracking_file() -> Path | None:
    """Find the most recent batch_jobs_*.json in checkpoints/."""
    candidates = sorted(CHECKPOINTS_DIR.glob("batch_jobs_*.json"), reverse=True)
    return candidates[0] if candidates else None


def main():
    args = parse_args()

    images_dir = Path(args.images_dir)
    db_path    = Path(args.db)
    html_out   = Path(args.html_out)
    models     = args.models

    conn = init_db(db_path)

    if args.batch:
        # --- Batch API mode ---
        images = collect_images(images_dir)
        if args.limit:
            images = images[:args.limit]
        log.info(f"Found {len(images)} images across {len(set(i['listing_id'] for i in images))} listings")
        log.info(f"Models: {models}")

        tracking_path = Path(args.tracking_file) if args.tracking_file else None

        if args.batch in ("submit", "run"):
            tracking_path = submit_batches(models, images, conn)

        if args.batch in ("poll", "run"):
            if tracking_path is None:
                tracking_path = _find_latest_tracking_file()
            if tracking_path is None or not tracking_path.exists():
                log.error("No tracking file found. Run --batch submit first.")
                sys.exit(1)
            log.info(f"Polling: {tracking_path}")
            poll_batches(tracking_path)

        if args.batch in ("collect", "run"):
            if tracking_path is None:
                tracking_path = _find_latest_tracking_file()
            if tracking_path is None or not tracking_path.exists():
                log.error("No tracking file found. Run --batch submit first.")
                sys.exit(1)
            log.info(f"Collecting results: {tracking_path}")
            collect_results(tracking_path, conn)

        log.info("Building HTML report...")
        build_html(conn, models, html_out)

    elif not args.html_only:
        # --- Real-time mode (existing behavior) ---
        images = collect_images(images_dir)
        log.info(f"Found {len(images)} images across {len(set(i['listing_id'] for i in images))} listings")
        log.info(f"Models: {models}")
        if args.limit:
            log.info(f"Limiting to first {args.limit} images")
        run_all(models, images, conn, limit=args.limit)
        log.info("Building HTML report...")
        build_html(conn, models, html_out)

    else:
        log.info("Building HTML report...")
        build_html(conn, models, html_out)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
