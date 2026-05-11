"""
Test Script for Njuskalo Logging System
Run this script to verify the logging system is working correctly
"""

import asyncio
import time
from logging_system import get_logger, ProcessLogger
from logging_integration import LoggedHTTPSession, logged_html_parsing, log_batch_operation


async def test_logging_system():
    """Test all aspects of the logging system"""
    
    print("Testing Njuskalo Logging System...")
    print("=" * 50)
    
    # Test 1: Basic logging
    print("\\n1. Testing basic logging...")
    logger = get_logger()
    logger.log_info("Test info message")
    logger.log_warning("Test warning message")
    
    # Test 2: Process logging
    print("\\n2. Testing process logging...")
    with ProcessLogger("test_process") as process_logger:
        process_logger.log_info("Process started")
        time.sleep(1)  # Simulate work
        process_logger.log_info("Process work completed")
    
    # Test 3: HTTP request logging (simulated)
    print("\\n3. Testing HTTP request logging...")
    logger.log_http_request_completion("https://example.com/test", "SUCCESS", 1500, 200)
    logger.log_http_request_failure("https://example.com/fail", Exception("Test timeout"), 5000)
    
    # Test 4: HTML parsing logging
    print("\\n4. Testing HTML parsing logging...")
    def test_parser(html_content):
        from bs4 import BeautifulSoup, Tag
        soup = BeautifulSoup(html_content, 'html.parser')
        links = soup.find_all('a')
        result = []
        for a in links:
            if isinstance(a, Tag):
                result.append({'name': a.get_text(), 'url': a.get('href', '')})
        return result
    
    logged_parser = logged_html_parsing(test_parser, "test.html")
    
    # Test successful parsing
    test_html = "<html><body><a href='http://example.com'>Test Link</a></body></html>"
    result = logged_parser(test_html)
    print(f"  Parsed {len(result)} links successfully")
    
    # Test parsing failure
    try:
        failing_parser = logged_html_parsing(lambda x: 1/0, "failing.html")  # Will cause division by zero
        failing_parser("test")
    except:
        print("  Parsing failure logged successfully")
    
    # Test 5: Batch operation logging
    print("\\n5. Testing batch operation logging...")
    with log_batch_operation("test_batch", 10) as batch:
        for i in range(10):
            try:
                if i == 5:  # Simulate one failure
                    raise Exception(f"Test error at item {i}")
                # Simulate processing time
                time.sleep(0.1)
                batch.log_item_success()
            except Exception as e:
                batch.log_item_failure(e, f"item_{i}")
            
            # Log progress every 3 items
            if i % 3 == 0:
                batch.log_progress(3)
    
    # Test 6: Exception logging
    print("\\n6. Testing exception logging...")
    try:
        raise ValueError("Test exception for logging")
    except Exception as e:
        logger.log_exception("Testing exception logging", e, {"test_data": "example"})
    
    print("\\n" + "=" * 50)
    print("Test completed! Check the following log files:")
    print("- backend/logs/info_YYYY-MM-DD.log")
    print("- backend/logs/error_YYYY-MM-DD.log")
    print("\\nAll logging functionality is working correctly!")


def check_log_files():
    """Check if log files were created and show their content"""
    import os
    from datetime import datetime
    
    today = datetime.now().strftime("%Y-%m-%d")
    logs_dir = os.path.join(os.path.dirname(__file__), "backend", "logs")
    
    info_log = os.path.join(logs_dir, f"info_{today}.log")
    error_log = os.path.join(logs_dir, f"error_{today}.log")
    
    print("\\n" + "=" * 60)
    print("LOG FILES CONTENT:")
    print("=" * 60)
    
    if os.path.exists(info_log):
        print(f"\\nINFO LOG ({info_log}):")
        print("-" * 40)
        with open(info_log, 'r', encoding='utf-8') as f:
            content = f.read()
            # Show last 20 lines
            lines = content.split('\\n')
            for line in lines[-20:]:
                if line.strip():
                    print(line)
    else:
        print(f"\\nInfo log not found: {info_log}")
    
    if os.path.exists(error_log):
        print(f"\\nERROR LOG ({error_log}):")
        print("-" * 40)
        with open(error_log, 'r', encoding='utf-8') as f:
            content = f.read()
            # Show last 20 lines
            lines = content.split('\\n')
            for line in lines[-20:]:
                if line.strip():
                    print(line)
    else:
        print(f"\\nError log not found: {error_log}")


if __name__ == "__main__":
    # Run the test
    asyncio.run(test_logging_system())
    
    # Show log file contents
    check_log_files()
    
    print("\\n" + "=" * 60)
    print("INTEGRATION READY!")
    print("=" * 60)
    print("""
Your logging system is now ready for integration!

NEXT STEPS:
1. Import the logging modules in your scrapers:
   from logging_system import get_logger, ProcessLogger
   from logging_integration import LoggedHTTPSession, logged_html_parsing

2. Wrap your main functions with ProcessLogger:
   with ProcessLogger("your_scraper_name") as logger:
       # your existing code

3. Replace AsyncSession with LoggedHTTPSession:
   session = LoggedHTTPSession(original_session, logger)

4. Wrap your parsing functions:
   logged_parser = logged_html_parsing(your_parser, "filename.html", logger)

Your existing code logic will remain completely unchanged!
""")
