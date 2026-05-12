#!/usr/bin/env python3
"""
Njuskalo Scraping Pipeline
=========================

3-step pipeline — everything goes into listings.db (no intermediate files):

1. njuskalo_category_tree_scraper.py — Scrape category tree → DB (categories, leaf_urls)
2. scrape_and_parse.py — Download HTML → parse → insert into DB (phone optional, see --fetch-phones)
3. ai_enrich.py — AI enrichment (agency fee, appliances, bathtub vision)

Usage:
    python pipeline.py [--step STEP] [--skip-existing]

Options:
    --step STEP         Run specific step only (1-3)
    --skip-existing     Skip steps if output already exists in DB
    --fetch-phones      Fetch phone numbers via API (requires Playwright, off by default)
"""

import os
import sys
import time
import subprocess
import argparse
from datetime import datetime
import logging

from db import get_db, log_event

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PIPELINE] %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


# Default price filter for scraping (EUR)
DEFAULT_PRICE_MIN = 300
DEFAULT_PRICE_MAX = 1200


class PipelineRunner:
    def __init__(self, skip_existing=False, price_min=None, price_max=None, restart=False, fetch_phones=False):
        self.skip_existing = skip_existing
        self.price_min = price_min
        self.price_max = price_max
        self.restart = restart
        self.fetch_phones = fetch_phones
        self.start_time = time.time()
        self.step_times = {}

    def run_script(self, script_name, step_num, description, extra_args=None):
        """Run a Python script with live output streaming."""
        logging.info("=" * 60)
        logging.info(f"STEP {step_num}: {description}")
        logging.info(f"Running: {script_name}")
        logging.info("=" * 60)

        step_start = time.time()
        last_stderr = ""

        cmd = [sys.executable, "-u", script_name] + (extra_args or [])

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', errors='replace',
            )
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    logging.info(f"  [{step_num}] {line}")
                    last_stderr = line
            proc.wait()

            step_duration = time.time() - step_start
            self.step_times[f"Step {step_num}"] = step_duration

            if proc.returncode == 0:
                logging.info(f"✅ STEP {step_num} COMPLETED in {step_duration:.2f}s")
                return True
            elif proc.returncode == 99:
                logging.warning(f"⚠️ STEP {step_num} ANTI-BOT DETECTED in {step_duration:.2f}s")
                return False
            else:
                logging.error(f"❌ STEP {step_num} FAILED in {step_duration:.2f}s")
                return False

        except Exception as e:
            step_duration = time.time() - step_start
            self.step_times[f"Step {step_num}"] = step_duration
            logging.error(f"❌ STEP {step_num} CRASHED in {step_duration:.2f}s: {e}")
            return False

    def _db_has_rows(self, table, min_count=1):
        """Check if a DB table has at least min_count rows."""
        try:
            conn = get_db()
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.close()
            return count >= min_count
        except Exception:
            return False

    def step1_category_scraper(self):
        """Step 1: Scrape category tree and extract leaf URLs into DB."""
        if self.skip_existing and self._db_has_rows("leaf_urls"):
            logging.info("⏭️  STEP 1 SKIPPED: leaf_urls table already populated")
            return True
        return self.run_script(
            'njuskalo_category_tree_scraper.py', 1,
            'Scraping category tree → DB (categories + leaf_urls)'
        )

    def step2_scrape_and_parse(self):
        """Step 2: Download + parse → DB (phone optional, use --fetch-phones to enable)."""
        if self.skip_existing and self._db_has_rows("listings", 10):
            logging.info("⏭️  STEP 2 SKIPPED: listings table already has data")
            return True
        extra = []
        if self.price_min is not None:
            extra += ["--price-min", str(self.price_min)]
        if self.price_max is not None:
            extra += ["--price-max", str(self.price_max)]
        if self.restart:
            extra += ["--restart"]
        if self.fetch_phones:
            extra += ["--fetch-phones"]
        desc = 'Download HTML → parse → insert into DB'
        if self.fetch_phones:
            desc = 'Download HTML → parse → fetch phone → insert into DB'
        if extra:
            desc += f' (price: {self.price_min or "*"}–{self.price_max or "*"}€)'
        return self.run_script('scrape_and_parse.py', 2, desc, extra_args=extra)

    def step3_ai_enrich(self):
        """Step 3: AI enrichment (agency fee, appliances, bathtub)."""
        if self.skip_existing:
            conn = get_db()
            unenriched = conn.execute(
                "SELECT COUNT(*) FROM listings WHERE ai_enriched_at IS NULL"
            ).fetchone()[0]
            conn.close()
            if unenriched == 0:
                logging.info("⏭️  STEP 3 SKIPPED: all listings AI-enriched")
                return True
        return self.run_script(
            'ai_enrich.py', 3,
            'AI enrichment (agency fee, appliances, bathtub vision)'
        )

    def run_full_pipeline(self):
        """Run the complete 3-step pipeline."""
        logging.info("🚀 STARTING NJUSKALO PIPELINE (DB-only)")
        logging.info(f"Started at: {datetime.now().isoformat()}")

        steps = [
            (self.step1_category_scraper, "Category Scraper"),
            (self.step2_scrape_and_parse, "Scrape + Parse"),
            (self.step3_ai_enrich, "AI Enrichment"),
        ]

        failed_steps = []
        for i, (step_func, step_name) in enumerate(steps, 1):
            success = step_func()
            if not success:
                failed_steps.append(f"Step {i}: {step_name}")
                logging.error(f"Pipeline stopped at Step {i} due to failure")
                break

        total_time = time.time() - self.start_time
        logging.info("=" * 60)
        logging.info("🏁 PIPELINE SUMMARY")
        logging.info("=" * 60)

        if failed_steps:
            logging.error(f"❌ Pipeline FAILED at: {', '.join(failed_steps)}")
        else:
            logging.info("✅ Pipeline COMPLETED SUCCESSFULLY!")

        logging.info(f"Total execution time: {total_time:.2f}s")
        for step, duration in self.step_times.items():
            step_num = int(step.split()[1].rstrip(':'))
            if failed_steps:
                first_failed = int(failed_steps[0].split()[1].rstrip(':'))
                status = "❌" if step_num >= first_failed else "✅"
            else:
                status = "✅"
            logging.info(f"{status} {step}: {duration:.2f}s")

        # Log to DB
        try:
            conn = get_db()
            log_event(conn, "INFO", "pipeline",
                      f"Pipeline {'completed' if not failed_steps else 'failed'} in {total_time:.0f}s")
            conn.close()
        except Exception:
            pass

        return len(failed_steps) == 0

    def run_single_step(self, step_num):
        """Run a single step."""
        steps = {
            1: (self.step1_category_scraper, "Category Scraper"),
            2: (self.step2_scrape_and_parse, "Scrape + Parse"),
            3: (self.step3_ai_enrich, "AI Enrichment"),
        }
        if step_num not in steps:
            logging.error(f"Invalid step: {step_num}. Must be 1-3.")
            return False

        step_func, step_name = steps[step_num]
        logging.info(f"🎯 RUNNING SINGLE STEP {step_num}: {step_name}")
        success = step_func()
        total_time = time.time() - self.start_time

        if success:
            logging.info(f"✅ Step {step_num} completed in {total_time:.2f}s")
        else:
            logging.error(f"❌ Step {step_num} failed after {total_time:.2f}s")
        return success


def main():
    parser = argparse.ArgumentParser(
        description='Njuskalo Scraping Pipeline (DB-only)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--step', type=int, choices=[1, 2, 3],
                        help='Run specific step only (1-3)')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Skip steps if output already exists in DB')
    parser.add_argument('--price-min', type=int, default=DEFAULT_PRICE_MIN,
                        help=f'Min price filter in EUR (default: {DEFAULT_PRICE_MIN})')
    parser.add_argument('--price-max', type=int, default=DEFAULT_PRICE_MAX,
                        help=f'Max price filter in EUR (default: {DEFAULT_PRICE_MAX})')
    parser.add_argument('--restart', action='store_true',
                        help='Ignore checkpoints, re-scan all leaf URLs from scratch')
    parser.add_argument('--fetch-phones', action='store_true', default=False,
                        help='Fetch phone numbers via Njuskalo API (requires Playwright, off by default)')
    args = parser.parse_args()

    runner = PipelineRunner(
        skip_existing=args.skip_existing,
        price_min=args.price_min,
        price_max=args.price_max,
        restart=args.restart,
        fetch_phones=args.fetch_phones,
    )

    try:
        if args.step:
            success = runner.run_single_step(args.step)
        else:
            success = runner.run_full_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logging.warning("🛑 Pipeline interrupted")
        sys.exit(130)
    except Exception as e:
        logging.error(f"💥 Pipeline crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
