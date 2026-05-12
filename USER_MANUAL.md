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

## Azure proxy deployment (one-time)

Deploy HTTP relay functions across 5 European regions to rotate IPs:

```bash
chmod +x deploy_azure_proxies.sh
./deploy_azure_proxies.sh
```

Saves endpoints to `azure_proxies.json` (used automatically by `scrape_and_parse.py`).
