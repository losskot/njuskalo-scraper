Njuskalo Scraping Pipeline - README
# 🏠 Njuskalo Scraping Pipeline

A complete automated pipeline for scraping and parsing real estate data from **[Njuskalo.hr](https://www.njuskalo.hr/)**.  
This project is designed for **data collection, research, and analysis** purposes, with full support for resuming, skipping, and modular execution of each pipeline step.

---

## 📦 Requirements

- **Python**: 3.8+  
- **OS**: Linux, macOS, Windows  
- **Dependencies**: listed in `requirements.txt`  

### Install
```bash
pip install -r requirements.txt
playwright install
```

---

## 🚀 Quick Start

### Run Complete Pipeline
```bash
python pipeline.py
```

### Run Specific Steps
```bash
python pipeline.py --step 1  # Category scraper only
python pipeline.py --step 2  # HTML scraper only  
python pipeline.py --step 3  # Phone fetcher only
python pipeline.py --step 4  # Parser only
```

### Skip Existing Data
```bash
python pipeline.py --skip-existing
```

### Windows Shortcut
```bash
run_pipeline.bat
```
*(Windows-only helper script; not required on Linux/macOS)*

---

## 📋 Pipeline Steps

### Step 1: Category Tree Scraper
- **Script**: `njuskalo_category_tree_scraper.py`
- **Purpose**: Scrape category tree and collect all property listing URLs
- **Output**: Category data and URL lists

### Step 2: HTML Page Scraper  
- **Script**: `scrape_leaf_entries.py`
- **Purpose**: Download individual property listing HTML pages  
- **Output**: `backend/website/` directory with HTML files  
- **Features**: checkpointing, proxy rotation, session management, retry logic

### Step 3: Phone Number Fetcher
- **Script**: `fetch_phones_from_api.py`  
- **Purpose**: Extract phone numbers via Njuskalo API  
- **Output**: `backend/phoneDB/phones.db` SQLite database  
- **Features**: async processing (50 req/sec), token auto-refresh, skip processed ads

### Step 4: Ultrafast Parser
- **Script**: `parser_ultrafast.py`  
- **Purpose**: Parse HTML files to structured JSON data  
- **Output**: `backend/json/` directory with parsed data  
- **Features**: memory-cached lookups, multi-core parsing, 3–5× faster than standard parser  

---

## 📊 Output Structure

```
backend/
├── website/           # HTML files from Step 2
├── phoneDB/           # Phone database from Step 3
│   ├── phones.db      # SQLite database
│   └── phones.log     # Phone fetcher logs
├── json/              # Parsed JSON from Step 4
└── logs/              # Parser logs
```

### Example JSON Output
```json
{
  "id": "123456",
  "title": "2-Bedroom Apartment in Zagreb",
  "price": "150000 EUR",
  "location": "Zagreb - Maksimir",
  "phone": "+385991234567",
  "url": "https://www.njuskalo.hr/nekretnine/ad-id-123456"
}
```

---

## 🔧 Configuration

### Phone Fetcher
- `RESCRAPE_NULL_PHONES = False` – re-scrape ads with no phone numbers if set to `True`
- `BATCH_SIZE = 50` – number of concurrent API requests  

### Parser
- `BATCH_SIZE = 200` – files per processing batch  
- `MAX_WORKERS = cpu_count()` – use all CPU cores  

---

## 📝 Logging

- **Pipeline log**: `pipeline.log` – overall pipeline execution  
- **Phone log**: `backend/phoneDB/phones.log` – phone fetching details  
- **Parser logs**: `backend/logs/` – individual parsing logs  

---

## ⚡ Performance

Expected throughput (based on typical tests):  
- **Phone Fetcher**: ~50 requests/second  
- **Parser**: 3–5× faster than standard parser  
- **Complete Pipeline**: performance depends on dataset size + network speed  

---

## 🛠️ Troubleshooting

### Resume from Failure
The pipeline supports resuming from any step. Each step checks for existing output and can skip completed work.

### Individual Step Execution
```bash
python pipeline.py --step 3  # Re-run phone fetcher only
```

### Skip Existing Data
```bash
python pipeline.py --skip-existing
```

---

## 📁 Required Files

Ensure these scripts exist in your project directory:
- `njuskalo_category_tree_scraper.py`
- `scrape_leaf_entries.py` 
- `fetch_phones_from_api.py`
- `parser_ultrafast.py`
- `bearer_token_finder.py` (required by phone fetcher)

---

## 🚨 Notes & Disclaimer

- The `backend/` folder is excluded from Git (see `.gitignore`)  
- Phone fetching requires valid bearer tokens (handled automatically)  
- Parser uses memory caching for maximum speed  
- All steps include comprehensive error handling and logging  

⚠️ **Disclaimer**:  
This project is provided **for educational and research purposes only**.  
Before running scrapers against Njuskalo.hr, please review and comply with their **Terms of Service**. The author(s) are not responsible for misuse.  

