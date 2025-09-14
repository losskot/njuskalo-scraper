#!/usr/bin/env python3
"""
Njuskalo Scraping Pipeline
=========================

This pipeline runs the complete Njuskalo scraping process in the correct order:

1. njuskalo_category_tree_scraper.py - Scrape category tree and URLs
2. scrape_leaf_entries.py - Scrape individual ad HTML pages
3. fetch_phones_from_api.py - Fetch phone numbers via API
4. parser_ultrafast.py - Parse HTML to structured JSON

Usage:
    python pipeline.py [--step STEP] [--skip-existing]

Options:
    --step STEP         Run specific step only (1-4)
    --skip-existing     Skip steps if output already exists
"""

import os
import sys
import time
import subprocess
import argparse
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PIPELINE] %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class PipelineRunner:
    def __init__(self, skip_existing=False):
        self.skip_existing = skip_existing
        self.start_time = time.time()
        self.step_times = {}
        
    def run_script(self, script_name, step_num, description):
        """Run a Python script and track execution time"""
        logging.info(f"=" * 60)
        logging.info(f"STEP {step_num}: {description}")
        logging.info(f"Running: {script_name}")
        logging.info(f"=" * 60)
        
        step_start = time.time()
        
        try:
            # Run the script
            result = subprocess.run([
                sys.executable, script_name
            ], capture_output=True, text=True, encoding='utf-8')
            
            step_duration = time.time() - step_start
            self.step_times[f"Step {step_num}"] = step_duration
            
            if result.returncode == 0:
                logging.info(f"✅ STEP {step_num} COMPLETED in {step_duration:.2f}s")
                logging.info(f"Output:\n{result.stdout}")
                return True
            else:
                logging.error(f"❌ STEP {step_num} FAILED in {step_duration:.2f}s")
                logging.error(f"Error output:\n{result.stderr}")
                logging.error(f"Standard output:\n{result.stdout}")
                return False
                
        except Exception as e:
            step_duration = time.time() - step_start
            self.step_times[f"Step {step_num}"] = step_duration
            logging.error(f"❌ STEP {step_num} CRASHED in {step_duration:.2f}s: {e}")
            return False
    
    def check_output_exists(self, paths):
        """Check if output files/directories exist"""
        for path in paths:
            if os.path.exists(path):
                if os.path.isdir(path):
                    files = os.listdir(path)
                    if len(files) > 0:
                        return True
                else:
                    return True
        return False
    
    def step1_category_scraper(self):
        """Step 1: Scrape category tree and URLs"""
        if self.skip_existing:
            # Check if category URLs already exist
            if self.check_output_exists(['category_urls.json', 'categories.json']):
                logging.info("⏭️  STEP 1 SKIPPED: Category data already exists")
                return True
        
        return self.run_script(
            'njuskalo_category_tree_scraper.py',
            1,
            'Scraping category tree and collecting URLs'
        )
    
    def step2_scrape_entries(self):
        """Step 2: Scrape individual ad HTML pages"""
        if self.skip_existing:
            # Check if HTML files already exist
            if self.check_output_exists(['backend/website']):
                logging.info("⏭️  STEP 2 SKIPPED: HTML files already exist")
                return True
        
        return self.run_script(
            'scrape_leaf_entries.py',
            2,
            'Scraping individual ad HTML pages'
        )
    
    def step3_fetch_phones(self):
        """Step 3: Fetch phone numbers via API"""
        if self.skip_existing:
            # Check if phone database already exists
            if self.check_output_exists(['backend/phoneDB/phones.db']):
                logging.info("⏭️  STEP 3 SKIPPED: Phone database already exists")
                return True
        
        return self.run_script(
            'fetch_phones_from_api.py',
            3,
            'Fetching phone numbers via API'
        )
    
    def step4_parse_ultrafast(self):
        """Step 4: Parse HTML to structured JSON"""
        if self.skip_existing:
            # Check if JSON files already exist
            if self.check_output_exists(['backend/json']):
                logging.info("⏭️  STEP 4 SKIPPED: JSON files already exist")
                return True
        
        return self.run_script(
            'parser_ultrafast.py',
            4,
            'Parsing HTML to structured JSON (ultrafast)'
        )
    
    def run_full_pipeline(self):
        """Run the complete pipeline"""
        logging.info("🚀 STARTING NJUSKALO SCRAPING PIPELINE")
        logging.info(f"Started at: {datetime.now().isoformat()}")
        
        steps = [
            (self.step1_category_scraper, "Category Tree Scraper"),
            (self.step2_scrape_entries, "HTML Page Scraper"),
            (self.step3_fetch_phones, "Phone Number Fetcher"),
            (self.step4_parse_ultrafast, "Ultrafast Parser")
        ]
        
        failed_steps = []
        
        for i, (step_func, step_name) in enumerate(steps, 1):
            success = step_func()
            if not success:
                failed_steps.append(f"Step {i}: {step_name}")
                logging.error(f"Pipeline stopped at Step {i} due to failure")
                break
        
        # Final summary
        total_time = time.time() - self.start_time
        logging.info("=" * 60)
        logging.info("🏁 PIPELINE SUMMARY")
        logging.info("=" * 60)
        
        if failed_steps:
            logging.error(f"❌ Pipeline FAILED at: {', '.join(failed_steps)}")
        else:
            logging.info("✅ Pipeline COMPLETED SUCCESSFULLY!")
        
        logging.info(f"Total execution time: {total_time:.2f}s")
        
        # Step-by-step timing - fixed logic
        for step, duration in self.step_times.items():
            # Extract step number from step name (e.g., "Step 1" -> 1)
            step_num = int(step.split()[1])
            
            # Check if this step failed by looking if there are any failed steps 
            # and if this step number is at or after the first failed step
            if failed_steps:
                first_failed_step = int(failed_steps[0].split()[1])
                status = "❌" if step_num >= first_failed_step else "✅"
            else:
                status = "✅"
                
            logging.info(f"{status} {step}: {duration:.2f}s")
        
        return len(failed_steps) == 0
    
    def run_single_step(self, step_num):
        """Run a single step of the pipeline"""
        steps = {
            1: (self.step1_category_scraper, "Category Tree Scraper"),
            2: (self.step2_scrape_entries, "HTML Page Scraper"), 
            3: (self.step3_fetch_phones, "Phone Number Fetcher"),
            4: (self.step4_parse_ultrafast, "Ultrafast Parser")
        }
        
        if step_num not in steps:
            logging.error(f"Invalid step number: {step_num}. Must be 1-4.")
            return False
        
        step_func, step_name = steps[step_num]
        logging.info(f"🎯 RUNNING SINGLE STEP {step_num}: {step_name}")
        
        success = step_func()
        total_time = time.time() - self.start_time
        
        if success:
            logging.info(f"✅ Step {step_num} completed successfully in {total_time:.2f}s")
        else:
            logging.error(f"❌ Step {step_num} failed after {total_time:.2f}s")
        
        return success

def main():
    parser = argparse.ArgumentParser(
        description='Njuskalo Scraping Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--step', 
        type=int, 
        choices=[1, 2, 3, 4],
        help='Run specific step only (1-4)'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip steps if output already exists'
    )
    
    args = parser.parse_args()
    
    # Create pipeline runner
    runner = PipelineRunner(skip_existing=args.skip_existing)
    
    try:
        if args.step:
            # Run single step
            success = runner.run_single_step(args.step)
        else:
            # Run full pipeline
            success = runner.run_full_pipeline()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logging.warning("🛑 Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"💥 Pipeline crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
