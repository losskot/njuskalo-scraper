# Njuskalo Scraper — User Manual

## Setup (one-time)

```bash
cd njuskalo-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
az login   # required for AI features
```

---

## Full pipeline

```bash
./run_full.sh
```

Runs all 4 steps in order with automatic retry on anti-bot blocks.

| Step | Script | Output |
|------|--------|--------|
| 1 | `njuskalo_category_tree_scraper.py` | Leaf URLs → DB |
| 2 | `scrape_leaf_entries.py` | HTML pages → `backend/website/` |
| 3 | `parser_ultrafast.py` | JSON → `backend/json/` |
| 4 | `find_apartments.py` | Filtered map → `results_YYYY-MM-DD_HHMMSS.html` |

---

## Scrape listings (steps 1–3 unified)

Replaces old steps 1–3 in a single pass — fetches HTML, parses, writes to DB:

```bash
.venv/bin/python3 scrape_and_parse.py                   # normal run (resumes from checkpoint)
.venv/bin/python3 scrape_and_parse.py --restart         # ignore checkpoint, start over
.venv/bin/python3 scrape_and_parse.py --price-min 200   # filter by price while scraping
.venv/bin/python3 scrape_and_parse.py --price-max 600
```

---

## AI enrichment

Extracts `has_bathtub`, `has_washing_machine`, `has_dishwasher`, `has_air_conditioning`, `heating_type`, `no_agency_fee`, and `description_uk` for every unenriched listing in the DB:

```bash
.venv/bin/python3 ai_enrich.py
```

---

## Filter apartments & generate map

```bash
.venv/bin/python3 find_apartments.py                                 # full run with AI
.venv/bin/python3 find_apartments.py --dry-run                       # no AI, heuristic filters only
.venv/bin/python3 find_apartments.py --resume                        # resume from last checkpoint
.venv/bin/python3 find_apartments.py --resume-id 2026-05-11_143022  # resume specific run
.venv/bin/python3 find_apartments.py --out results.html              # custom output filename
.venv/bin/python3 find_apartments.py --max-age 0                     # include listings of any age
.venv/bin/python3 find_apartments.py --max-images 5                  # limit images checked per listing
.venv/bin/python3 find_apartments.py --model gpt-4o                  # override AI model
```

Output: `results_YYYY-MM-DD_HHMMSS.html` (standalone Leaflet map, never overwritten).  
Checkpoints: `checkpoints/find_*.json` — safe to interrupt and `--resume`.

---

## Web UI

```bash
.venv/bin/python3 server.py                    # default: http://127.0.0.1:5000
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
