import os
import sys
import json
import time
import traceback
import re
import sqlite3
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil

# Comprehensive logging setup
def setup_comprehensive_logging():
    """Setup comprehensive logging with info and error loggers"""
    logs_dir = os.path.join(os.path.dirname(__file__), "backend", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    info_log_file = os.path.join(logs_dir, f"info_{date_str}.log")
    error_log_file = os.path.join(logs_dir, f"error_{date_str}.log")
    
    # Setup info logger
    global info_logger
    info_logger = logging.getLogger('parser_info')
    info_logger.setLevel(logging.INFO)
    if not info_logger.handlers:
        info_handler = logging.FileHandler(info_log_file, encoding='utf-8')
        info_formatter = logging.Formatter('%(asctime)s - %(message)s')
        info_handler.setFormatter(info_formatter)
        info_logger.addHandler(info_handler)
        info_logger.propagate = False
    
    # Setup error logger
    global error_logger
    error_logger = logging.getLogger('parser_error')
    error_logger.setLevel(logging.ERROR)
    if not error_logger.handlers:
        error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
        error_formatter = logging.Formatter('%(asctime)s - %(message)s')
        error_handler.setFormatter(error_formatter)
        error_logger.addHandler(error_handler)
        error_logger.propagate = False

# Global counters
http_success_count = 0
http_failure_count = 0
parsing_success_count = 0
parsing_failure_count = 0

def log_process_start(process_name):
    """Log process start"""
    info_logger.info(f"PROCESS_START: {process_name} started at {datetime.now().isoformat()}")

def log_process_end(process_name, start_time):
    """Log process end with duration and summary"""
    duration = time.time() - start_time
    summary = f"Duration: {duration:.2f}s, HTTP Success: {http_success_count}, HTTP Failures: {http_failure_count}, Parsing Success: {parsing_success_count}, Parsing Failures: {parsing_failure_count}"
    info_logger.info(f"PROCESS_END: {process_name} completed at {datetime.now().isoformat()} - {summary}")

def log_http_completion(url, status_code, response_size, method="GET"):
    """Log successful HTTP request"""
    global http_success_count
    http_success_count += 1
    info_logger.info(f"HTTP_SUCCESS: {method} {url} -> {status_code} ({response_size} bytes)")

def log_http_failure(url, error_msg, duration_ms, method="GET"):
    """Log failed HTTP request"""
    global http_failure_count
    http_failure_count += 1
    error_logger.error(f"HTTP_FAILURE: {method} {url} failed after {duration_ms}ms - {error_msg}")

def log_parsing_completion(operation, items_parsed, data_type="html"):
    """Log successful parsing operation"""
    try:
        global parsing_success_count
        parsing_success_count += 1
        
        # Reinitialize logger if not available (for multiprocessing)
        if 'info_logger' not in globals():
            setup_comprehensive_logging()
        
        info_logger.info(f"PARSING_SUCCESS: {operation} parsed {items_parsed} items of type {data_type}")
    except:
        # Fallback to print if logging fails
        print(f"PARSING_SUCCESS: {operation} parsed {items_parsed} items of type {data_type}")

def log_parsing_failure(operation, error_msg, html_snippet=""):
    """Log failed parsing operation with HTML snippet"""
    try:
        global parsing_failure_count
        parsing_failure_count += 1
        
        # Reinitialize logger if not available (for multiprocessing)
        if 'error_logger' not in globals():
            setup_comprehensive_logging()
        
        snippet = html_snippet[:500] + "..." if len(html_snippet) > 500 else html_snippet
        error_logger.error(f"PARSING_FAILURE: {operation} failed - {error_msg} | HTML: {snippet}")
    except:
        # Fallback to print if logging fails
        snippet = html_snippet[:500] + "..." if len(html_snippet) > 500 else html_snippet
        print(f"PARSING_FAILURE: {operation} failed - {error_msg} | HTML: {snippet}")

def log_exception(operation, exception):
    """Log exception with full traceback"""
    try:
        # Reinitialize logger if not available (for multiprocessing)
        if 'error_logger' not in globals():
            setup_comprehensive_logging()
        
        import traceback
        error_logger.error(f"EXCEPTION: {operation} - {str(exception)} | Traceback: {traceback.format_exc()}")
    except:
        # Fallback to print if logging fails
        import traceback
        print(f"EXCEPTION: {operation} - {str(exception)} | Traceback: {traceback.format_exc()}")

# Paths
INPUT_DIR = "backend/website"
OUTPUT_DIR = "backend/json"
LOG_DIR = "backend/logs"
DB_PATH = "backend/phoneDB/phones.db"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Exit codes
EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_NETWORK_ERROR = 2
EXIT_PARSING_ERROR = 3
EXIT_FS_ERROR = 4

# Optimized Configuration - Use all available cores
BATCH_SIZE = 200  # Much larger batches
MAX_WORKERS = cpu_count()  # Use all CPU cores
CHUNK_SIZE = 50  # Files per worker chunk

# Pre-compiled regex patterns for speed
LAT_LNG_PATTERN = re.compile(r'"lat":([\d\.-]+),"lng":([\d\.-]+),"approximate":(true|false)')
AD_ID_PATTERN = re.compile(r'^(\d+)\.html$')

# Global database connection cache to avoid repeated DB connections
_db_cache = {}

def load_phone_cache():
    """Load all phone data into memory cache for ultra-fast lookups"""
    global _db_cache
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT ad_id, phones FROM phones")
        _db_cache = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        print(f"[CACHE] Loaded {len(_db_cache)} phone records into memory")
    except Exception as e:
        print(f"[CACHE ERROR] Failed to load phone cache: {e}")
        _db_cache = {}

def get_phone_from_cache(ad_id):
    """Ultra-fast phone lookup from memory cache"""
    phones_json = _db_cache.get(ad_id)
    if phones_json:
        try:
            phones = json.loads(phones_json)
            return phones[0].strip() if phones and isinstance(phones[0], str) else None
        except:
            return None
    return None

def process_single_file_ultrafast(filename):
    """Ultra-optimized single file processing"""
    if not filename.endswith(".html"):
        return None
    
    file_start = time.time()
    filepath = os.path.join(INPUT_DIR, filename)
    base_filename = os.path.splitext(filename)[0]
    
    # Skip if JSON already exists (should be pre-filtered but double check)
    json_file = os.path.join(OUTPUT_DIR, base_filename + ".json")
    if os.path.exists(json_file):
        return {'filename': filename, 'status': 'skipped', 'duration_ms': 0}
    
    try:
        # Fast file read with minimal encoding detection
        with open(filepath, "r", encoding="utf-8", errors='ignore') as f:
            html = f.read()

        # Use lxml parser for speed (falls back to html.parser if not available)
        try:
            soup = BeautifulSoup(html, "lxml")
        except:
            soup = BeautifulSoup(html, "html.parser")

        # Extract ad_id from filename (now just the number before .html)
        oglas_id = base_filename
        podaci = {"id": oglas_id}

        # Canonical link - single selector
        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get("href"):
            podaci["link"] = canonical_tag["href"]

        # Location from script tags - optimized regex search
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string:
                match = LAT_LNG_PATTERN.search(script.string)
                if match:
                    podaci["lokacija"] = {
                        "lat": float(match.group(1)),
                        "lng": float(match.group(2)),
                        "approximate": match.group(3) == 'true'
                    }
                    break

        # Title - direct find
        title_tag = soup.find("title")
        podaci["naslov"] = title_tag.get_text(strip=True) if title_tag else None

        # Price - single CSS selector
        price_tag = soup.select_one("dl.ClassifiedDetailSummary-priceRow dd.ClassifiedDetailSummary-priceDomestic")
        podaci["cijena"] = price_tag.get_text(strip=True) if price_tag else None

        # Basic details - optimized processing
        info_section = soup.select_one("div.ClassifiedDetailBasicDetails dl.ClassifiedDetailBasicDetails-list")
        if info_section:
            dt_tags = info_section.find_all("dt")
            dd_tags = info_section.find_all("dd")
            for dt, dd in zip(dt_tags, dd_tags):
                key_span = dt.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
                val_span = dd.find("span", class_="ClassifiedDetailBasicDetails-textWrapContainer")
                if key_span and val_span:
                    kljuc = key_span.get_text(strip=True)
                    vrijednost = val_span.get_text(strip=True)
                    if kljuc and vrijednost:
                        podaci[kljuc] = vrijednost

        # Description - single operation
        desc_tag = soup.find("div", class_="ClassifiedDetailDescription-text")
        podaci["opis"] = desc_tag.get_text(" ", strip=True).replace("\n", " ") if desc_tag else None

        # Additional property groups - batch processing
        dodatne_sekcije = soup.select("section.ClassifiedDetailPropertyGroups-group")
        for sekcija in dodatne_sekcije:
            naslov_grupe = sekcija.find("h3", class_="ClassifiedDetailPropertyGroups-groupTitle")
            if not naslov_grupe:
                continue
            ime_grupe = naslov_grupe.get_text(strip=True)

            # Fast list comprehension instead of loop
            stavke = [li.get_text(strip=True) for li in sekcija.select("li.ClassifiedDetailPropertyGroups-groupListItem") if li.get_text(strip=True)]
            if stavke:
                podaci[ime_grupe] = stavke

        # Owner details - batch selectors
        owner_section = soup.select_one("div.ClassifiedDetailOwnerDetails")
        if owner_section:
            agencija_tag = owner_section.select_one("h2.ClassifiedDetailOwnerDetails-title a")
            if agencija_tag:
                podaci["naziv_agencije"] = agencija_tag.get_text(strip=True)

            web_tag = owner_section.select_one("a[href^='http']:not([href^='mailto'])")
            if web_tag:
                podaci["profil_agencije"] = web_tag.get("href")

            email_tag = owner_section.select_one("a[href^='mailto']")
            if email_tag:
                podaci["email_agencije"] = email_tag.get_text(strip=True)

            adresa_li = owner_section.select_one("li.ClassifiedDetailOwnerDetails-contactEntry i[aria-label='Adresa']")
            if adresa_li and adresa_li.parent:
                podaci["adresa_agencije"] = adresa_li.parent.get_text(strip=True).replace("Adresa: ", "")

        # Ultra-fast phone lookup from memory cache
        podaci["telefon"] = get_phone_from_cache(oglas_id)

        # System details - optimized processing
        system_details = soup.select_one("dl.ClassifiedDetailSystemDetails-list")
        if system_details:
            dt_tags = system_details.find_all("dt")
            dd_tags = system_details.find_all("dd")
            for dt, dd in zip(dt_tags, dd_tags):
                key = dt.get_text(strip=True)
                val = dd.get_text(strip=True)
                if key and val:
                    if key == "Oglas objavljen":
                        podaci["oglas_objavljen"] = val
                    elif key == "Do isteka još":
                        podaci["do_isteka"] = val
                    elif key == "Oglas prikazan":
                        podaci["oglas_prikazan"] = val

        # Images - list comprehension for speed
        image_tags = soup.select("li[data-media-type='image']")
        podaci["slike"] = [tag.get("data-large-image-url") for tag in image_tags if tag.get("data-large-image-url")]

        # Fast JSON write with minimal formatting
        json_putanja = os.path.join(OUTPUT_DIR, base_filename + ".json")
        with open(json_putanja, "w", encoding="utf-8") as jf:
            json.dump(podaci, jf, ensure_ascii=False, separators=(',', ':'))  # No indent for speed

        duration_ms = int((time.time() - file_start) * 1000)
        
        # Log successful parsing
        log_parsing_completion("html_to_json", 1, "ad_data")
        
        return {
            'filename': filename,
            'status': 'success',
            'duration_ms': duration_ms,
            'ad_id': oglas_id
        }

    except Exception as e:
        duration_ms = int((time.time() - file_start) * 1000)
        
        # Log parsing failure
        log_parsing_failure("html_to_json", str(e)[:200], html[:1000] if 'html' in locals() else "")
        
        return {
            'filename': filename,
            'status': 'error',
            'duration_ms': duration_ms,
            'error': str(e)[:200],  # Truncate error for speed
            'ad_id': oglas_id if 'oglas_id' in locals() else 'unknown'
        }

def process_batch_ultrafast(filenames):
    """Ultra-fast batch processing with ProcessPoolExecutor"""
    print(f"[BATCH] Processing {len(filenames)} files with {MAX_WORKERS} workers...")
    
    batch_start = time.time()
    results = []
    
    # Use ProcessPoolExecutor for better performance than Pool
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_filename = {executor.submit(process_single_file_ultrafast, filename): filename 
                             for filename in filenames}
        
        # Collect results as they complete
        for future in as_completed(future_to_filename):
            result = future.result()
            if result is not None:
                results.append(result)
    
    batch_duration = time.time() - batch_start
    
    success_count = len([r for r in results if r['status'] == 'success'])
    error_count = len([r for r in results if r['status'] == 'error'])
    skipped_count = len([r for r in results if r['status'] == 'skipped'])
    total_files = len(results)
    
    if total_files > 0:
        processed_results = [r for r in results if r['status'] in ['success', 'error']]
        if processed_results:
            avg_duration = sum(r['duration_ms'] for r in processed_results) / len(processed_results)
            files_per_second = len(processed_results) / batch_duration if batch_duration > 0 else 0
        else:
            avg_duration = 0
            files_per_second = 0
        
        print(f"[BATCH COMPLETE] {success_count}/{total_files} successful, {error_count} errors, {skipped_count} skipped")
        if processed_results:
            print(f"[BATCH STATS] Avg: {avg_duration:.1f}ms per file, Rate: {files_per_second:.1f} files/sec")
        
        # Only show first 3 errors to avoid spam
        error_results = [r for r in results if r['status'] == 'error']
        if error_results:
            print(f"[ERRORS] First few errors:")
            for result in error_results[:3]:
                print(f"  - {result['filename']}: {result['error']}")
            if len(error_results) > 3:
                print(f"  ... and {len(error_results) - 3} more errors")
    
    return results

def main():
    """Ultra-fast main function"""
    # Setup comprehensive logging
    setup_comprehensive_logging()
    
    # Log process start
    process_start_time = time.time()
    log_process_start("html_parsing")
    
    exit_code = EXIT_SUCCESS
    start_time = time.time()
    
    try:
        print(f"[INIT] System info: {cpu_count()} CPU cores, {psutil.virtual_memory().total // (1024**3)}GB RAM")
        
        # Load phone cache into memory for ultra-fast lookups
        load_phone_cache()
        
        # Pre-filter files to avoid redundant checks
        print("[INIT] Scanning for unparsed files...")
        all_html_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".html")]
        unparsed_files = []
        
        # Batch check for existing JSON files
        for f in all_html_files:
            base_filename = os.path.splitext(f)[0]
            json_file = os.path.join(OUTPUT_DIR, base_filename + ".json")
            if not os.path.exists(json_file):
                unparsed_files.append(f)
        
        total_files = len(unparsed_files)
        skipped_count = len(all_html_files) - total_files

        if total_files == 0:
            print(f"[COMPLETE] All {len(all_html_files)} HTML files already parsed!")
            return EXIT_SUCCESS

        print(f"[INIT] Found {total_files} unparsed files ({skipped_count} already parsed)")
        print(f"[INIT] Using {MAX_WORKERS} workers, batch size: {BATCH_SIZE}")

        # Process in large batches for maximum efficiency
        all_results = []
        processed_count = 0

        for i in range(0, total_files, BATCH_SIZE):
            batch_files = unparsed_files[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE

            print(f"\n[BATCH {batch_num}/{total_batches}] Processing files {i+1}-{min(i+BATCH_SIZE, total_files)} of {total_files}")

            batch_results = process_batch_ultrafast(batch_files)
            all_results.extend(batch_results)
            processed_count += len([r for r in batch_results if r['status'] != 'skipped'])

            # Quick progress update
            if total_files > 0:
                progress = ((i + len(batch_files)) / total_files) * 100
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                eta = (total_files - processed_count) / rate if rate > 0 else 0
                print(f"[PROGRESS] {progress:.1f}% complete, Rate: {rate:.1f} files/sec, ETA: {eta:.1f}s")
        
        # Lightning-fast final statistics
        total_elapsed = time.time() - start_time
        success_count = len([r for r in all_results if r['status'] == 'success'])
        error_count = len([r for r in all_results if r['status'] == 'error'])
        final_skipped = len([r for r in all_results if r['status'] == 'skipped'])
        actual_processed = success_count + error_count
        
        print(f"\n[ULTRAFAST RESULTS]")
        print(f"Total files: {len(all_html_files)}")
        print(f"Already parsed: {skipped_count}")
        print(f"Newly processed: {actual_processed}")
        print(f"  - Successful: {success_count}")
        print(f"  - Errors: {error_count}")
        print(f"Total time: {total_elapsed:.2f}s")
        if actual_processed > 0:
            print(f"Processing rate: {actual_processed / total_elapsed:.1f} files/sec")
            print(f"Avg per file: {(total_elapsed / actual_processed) * 1000:.1f}ms")
        
        if error_count > 0:
            exit_code = EXIT_PARSING_ERROR
            print(f"\n[ERRORS] {error_count} files failed to process")
        
        # Minimal logging for speed
        if actual_processed > 0:
            summary_log_path = os.path.join(LOG_DIR, f"ultrafast_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            with open(summary_log_path, "w", encoding="utf-8") as f:
                f.write(f"Ultrafast Processing - {datetime.now().isoformat()}\n")
                f.write(f"Rate: {actual_processed / total_elapsed:.1f} files/sec\n")
                f.write(f"Success: {success_count}, Errors: {error_count}\n")
                f.write(f"Workers: {MAX_WORKERS}, Batch: {BATCH_SIZE}\n")
            print(f"[LOG] Summary: {summary_log_path}")
        
    except Exception as e:
        exit_code = EXIT_FS_ERROR
        print(f"FATAL ERROR: {str(e)}")
        traceback.print_exc()
    
    # Log process end
    log_process_end("html_parsing", process_start_time)
    
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
