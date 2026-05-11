#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PYTHON=".venv/bin/python3"
MAX_RETRIES=10

echo "═══ Step 1/4: Scraping category tree ═══"
for i in $(seq 1 $MAX_RETRIES); do
  set +e
  $PYTHON njuskalo_category_tree_scraper.py
  rc=$?
  set -e
  if [ $rc -eq 0 ]; then
    break
  elif [ $rc -eq 99 ]; then
    echo "  [BLOCKED] Anti-bot detected (attempt $i/$MAX_RETRIES). Waiting 90s before retry..."
    sleep 90
  else
    echo "  [ERROR] njuskalo_category_tree_scraper.py exited with code $rc"
    exit $rc
  fi
done
if [ $rc -ne 0 ]; then
  echo "  [ERROR] njuskalo_category_tree_scraper.py failed after $MAX_RETRIES retries"
  exit 1
fi

echo "═══ Step 2/4: Scraping leaf entries ═══"
for i in $(seq 1 $MAX_RETRIES); do
  set +e
  $PYTHON scrape_leaf_entries.py
  rc=$?
  set -e
  if [ $rc -eq 0 ]; then
    break
  elif [ $rc -eq 99 ]; then
    echo "  [BLOCKED] Anti-bot detected (attempt $i/$MAX_RETRIES). Waiting 90s before retry..."
    sleep 90
  else
    echo "  [ERROR] scrape_leaf_entries.py exited with code $rc"
    exit $rc
  fi
done
if [ $rc -ne 0 ]; then
  echo "  [ERROR] scrape_leaf_entries.py failed after $MAX_RETRIES retries"
  exit 1
fi

echo "═══ Step 3/4: Parsing HTMLs → JSON ═══"
$PYTHON parser_ultrafast.py

echo "═══ Step 4/4: Filtering apartments → map ═══"
$PYTHON find_apartments.py "$@"

echo "═══ Done ═══"
