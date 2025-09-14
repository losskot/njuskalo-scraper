# Njuskalo Scraping Pipeline

A complete automated pipeline for scraping real estate data from Njuskalo.hr.

### Install requirements

```bash
pip install -r requirements.txt
playwright install
```

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

### Windows Batch File
```bash
run_pipeline.bat
```

## 📋 Pipeline Steps

### Step 1: Category Tree Scraper
- **Script**: `njuskalo_category_tree_scraper.py`
- **Purpose**: Scrape category tree and collect all property listing URLs
- **Output**: Category data and URL lists

### Step 2: HTML Page Scraper  
- **Script**: `scrape_leaf_entries.py`
- **Purpose**: Download individual property listing HTML pages
- **Output**: `backend/website/` directory with HTML files
- **Features**: 
  - Checkpointing and resume capability
  - Proxy rotation and session management
  - Block detection and retry logic

### Step 3: Phone Number Fetcher
- **Script**: `fetch_phones_from_api.py` 
- **Purpose**: Extract phone numbers via Njuskalo API
- **Output**: `backend/phoneDB/phones.db` SQLite database
- **Features**:
  - Skip already-processed ads
  - Bearer token auto-refresh
  - High-throughput async processing (50 req/sec)

### Step 4: Ultrafast Parser
- **Script**: `parser_ultrafast.py`
- **Purpose**: Parse HTML files to structured JSON data
- **Output**: `backend/json/` directory with parsed data
- **Features**:
  - Memory-cached phone number lookups
  - Multi-core processing with all CPU cores
  - 3-5x faster than standard parser

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

## 🔧 Configuration

### Phone Fetcher Settings
- `RESCRAPE_NULL_PHONES = False` - Set to `True` to re-scrape ads with no phone numbers
- `BATCH_SIZE = 50` - Number of concurrent API requests

### Parser Settings  
- `BATCH_SIZE = 200` - Files per processing batch
- `MAX_WORKERS = cpu_count()` - Uses all CPU cores

## 📝 Logging

- **Pipeline log**: `pipeline.log` - Overall pipeline execution
- **Phone log**: `backend/phoneDB/phones.log` - Phone fetching details
- **Parser logs**: `backend/logs/` - Individual parsing logs

## ⚡ Performance

Expected throughput:
- **Phone Fetcher**: ~50 requests/second
- **Parser**: 3-5x faster than standard parser
- **Complete pipeline**: Depends on data size and network speed

## 🛠️ Troubleshooting

### Resume from Failure
The pipeline supports resuming from any step. Each step checks for existing output and can skip completed work.

### Individual Step Execution
If a step fails, you can re-run just that step:
```bash
python pipeline.py --step 3  # Re-run phone fetcher only
```

### Skip Existing Data
Use `--skip-existing` to avoid re-processing existing data:
```bash
python pipeline.py --skip-existing
```

## 📁 Required Files

Make sure these files exist in your project directory:
- `njuskalo_category_tree_scraper.py`
- `scrape_leaf_entries.py` 
- `fetch_phones_from_api.py`
- `parser_ultrafast.py`
- `bearer_token_finder.py` (required by phone fetcher)

## 🚨 Notes

- The `backend/` folder is excluded from Git (see `.gitignore`)
- Phone fetching requires valid bearer tokens (handled automatically)
- Parser uses memory caching for maximum speed
- All steps include comprehensive error handling and logging
