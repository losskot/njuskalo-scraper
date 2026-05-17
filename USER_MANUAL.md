# Njuskalo Scraper — User Manual

## Setup (one-time)

```bash
cd njuskalo-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
az login   # required for AI enrichment
```

---

## Full pipeline

```bash
.venv/bin/python3 pipeline.py
```

Runs all 3 steps in order, everything goes into `listings.db` (no intermediate files).

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `njuskalo_category_tree_scraper.py` | Scrapes category tree → `categories` + `leaf_urls` tables |
| 2 | `scrape_and_parse.py` | Downloads HTML → parses → fetches phone → `listings` table |
| 3 | `ai_enrich.py` | Text + vision AI → fills bathtub, appliances, address, agency fee |

### Pipeline options

```bash
.venv/bin/python3 pipeline.py --step 2              # run a single step only (1, 2 or 3)
.venv/bin/python3 pipeline.py --skip-existing       # skip steps whose output is already in DB
.venv/bin/python3 pipeline.py --price-min 300 --price-max 1200   # price filter for scraping
```

---

## Running steps individually

### Step 1 — category tree

```bash
.venv/bin/python3 njuskalo_category_tree_scraper.py
```

Exits with code **99** if anti-bot is detected (pipeline retries automatically).

### Step 2 — scrape & parse

```bash
.venv/bin/python3 scrape_and_parse.py                   # resumes from checkpoint
.venv/bin/python3 scrape_and_parse.py --restart         # ignore checkpoint, start fresh
.venv/bin/python3 scrape_and_parse.py --price-min 200 --price-max 800
```

### Step 3 — AI enrichment

```bash
.venv/bin/python3 ai_enrich.py                          # enrich all unenriched listings
.venv/bin/python3 ai_enrich.py --limit 50               # process at most 50 listings
.venv/bin/python3 ai_enrich.py --skip-vision            # text analysis only, no image calls
.venv/bin/python3 ai_enrich.py --model gpt-4o           # override model
```

Enriches: `has_bathtub`, `has_washing_machine`, `has_dishwasher`, `has_air_conditioning`,
`heating_type`, `no_agency_fee`, `address`, `description_uk`.

---

## Web UI

```bash
.venv/bin/python3 server.py                    # http://127.0.0.1:5000
.venv/bin/python3 server.py --port 8080
.venv/bin/python3 server.py --host 0.0.0.0    # expose on local network
```

Open **http://127.0.0.1:5000** — map with all listings, filter sidebar, detail panel with images and Ukrainian description.

---

## Bathtub photo classifier (PoC — standalone, not part of pipeline)

`bathtub_classifier.py` is a **proof-of-concept** deep-analysis tool that is **not integrated into the pipeline**. It was run manually on a small sample (46 listings) to evaluate per-photo multi-model classification quality.

### What it does
- Reads locally downloaded images from `backend/images/{listing_id}/`
- Sends **each photo individually** to **multiple Azure OpenAI vision models** (up to 8)
- Classifies each photo into one of: `bathroom_with_bathtub`, `bathroom_with_shower_only`, `bathroom_unclear`, `not_bathroom`
- Stores per-photo results in a **separate DB**: `backend/bathtub_results.db` (never touches `listings.db`)
- Uses majority vote across models to determine final per-photo verdict

### How it differs from `ai_enrich.py`
| | `ai_enrich.py` (pipeline step 3) | `bathtub_classifier.py` (PoC) |
|---|---|---|
| Integrated in pipeline | ✅ yes | ❌ no |
| Granularity | per-listing (yes/no) | per-photo + per-model |
| Models used | 1 | up to 8 |
| Output | `listings.db` → `has_bathtub` | `bathtub_results.db` |
| Coverage (current) | 2,163 listings | 46 listings |
| Early-exit | ✅ stops at first bathtub found | ❌ classifies all photos |

### Running it manually
```bash
# All 8 models on all downloaded images (slow — ~85 hrs total)
.venv/bin/python3 bathtub_classifier.py

# One model at a time (recommended — resumable)
.venv/bin/python3 bathtub_classifier.py --models gpt-5-mini
.venv/bin/python3 bathtub_classifier.py --models gpt-4o

# Limit to N images (for testing)
.venv/bin/python3 bathtub_classifier.py --limit 50 --models gpt-5-mini

# Regenerate HTML report without re-running inference
.venv/bin/python3 bathtub_classifier.py --html-only
```

The script skips already-classified images (`UNIQUE(model, image_path)` constraint), so it is safe to stop and resume at any time.

### Results used in the viewer
The web viewer's **🛁 Photos panel** reads from `bathtub_results.db` and shows per-photo confirmed bathtub images for listings visible on the map. Currently 21 listings have confirmed bathtub photos (30 photos total).

---

## Azure proxy deployment (one-time)

Deploy HTTP relay functions across 5 European regions to rotate IPs:

```bash
chmod +x deploy_azure_proxies.sh
./deploy_azure_proxies.sh
```

Saves endpoints to `azure_proxies.json` (used automatically by `scrape_and_parse.py`).
