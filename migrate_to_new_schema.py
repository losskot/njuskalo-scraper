#!/usr/bin/env python3
"""
migrate_to_new_schema.py — One-time migration to import existing data into the new unified DB.

Imports:
  1. Existing backend/json/*.json → listings + images + listing_tags
  2. Existing backend/phoneDB/phones.db → updates phone column
  3. Existing backend/categories/leaf_urls/*.txt → leaf_urls table

Run once, then the old files are no longer needed.

Usage:
    python migrate_to_new_schema.py
"""
import glob
import json
import os
import sqlite3
import sys

from db import (
    get_db, init_db, upsert_listing, LIST_FIELDS, DB_PATH, ARCHIVE_DB_PATH,
    get_archive_db,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(SCRIPT_DIR, "backend", "json")
PHONE_DB = os.path.join(SCRIPT_DIR, "backend", "phoneDB", "phones.db")
LEAF_URLS_DIR = os.path.join(SCRIPT_DIR, "backend", "categories", "leaf_urls")
HTML_DIR = os.path.join(SCRIPT_DIR, "backend", "website")


def migrate_json_listings():
    """Import all JSON files into listings DB."""
    files = sorted(glob.glob(os.path.join(JSON_DIR, "*.json")))
    if not files:
        print(f"No JSON files found in {JSON_DIR}")
        return 0

    print(f"Found {len(files)} JSON files to import...")

    # Load phone cache
    phone_cache = {}
    if os.path.exists(PHONE_DB):
        pconn = sqlite3.connect(PHONE_DB)
        try:
            for row in pconn.execute("SELECT ad_id, phones FROM phones"):
                if row[1]:
                    try:
                        phones = json.loads(row[1])
                        if phones and isinstance(phones[0], str):
                            phone_cache[row[0]] = phones[0].strip()
                    except Exception:
                        pass
            print(f"Loaded {len(phone_cache)} phone records from phones.db")
        finally:
            pconn.close()

    conn = get_db()
    inserted = 0
    skipped = 0

    for fpath in files:
        try:
            data = json.load(open(fpath, encoding="utf-8"))
        except Exception as e:
            print(f"  [ERROR] {fpath}: {e}")
            continue

        ad_id = data.get("id")
        if not ad_id:
            continue

        # Add phone from cache if not in parsed data
        if not data.get("telefon") and ad_id in phone_cache:
            data["telefon"] = phone_cache[ad_id]

        try:
            upsert_listing(conn, data)
            inserted += 1
        except Exception as e:
            print(f"  [ERROR] {ad_id}: {e}")
            skipped += 1
            continue

        if inserted % 100 == 0:
            conn.commit()
            print(f"  Imported {inserted}...")

    conn.commit()
    conn.close()
    print(f"Imported {inserted} listings ({skipped} errors)")
    return inserted


def migrate_leaf_urls():
    """Import leaf URL text files into leaf_urls table."""
    if not os.path.exists(LEAF_URLS_DIR):
        print(f"No leaf_urls directory found at {LEAF_URLS_DIR}")
        return 0

    txt_files = glob.glob(os.path.join(LEAF_URLS_DIR, "*.txt"))
    if not txt_files:
        print("No .txt files found in leaf_urls directory")
        return 0

    conn = get_db()
    total = 0
    for txt_file in txt_files:
        with open(txt_file, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
        for url in urls:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO leaf_urls (url, scraped_at) VALUES (?, ?)",
                    (url, None),
                )
                total += 1
            except Exception:
                pass
    conn.commit()
    conn.close()
    print(f"Imported {total} leaf URLs from {len(txt_files)} files")
    return total


def migrate_html_archive():
    """Archive existing HTML files into html_archive.db."""
    if not os.path.exists(HTML_DIR):
        print(f"No HTML directory found at {HTML_DIR}")
        return 0

    html_files = glob.glob(os.path.join(HTML_DIR, "*.html"))
    if not html_files:
        print("No HTML files to archive")
        return 0

    print(f"Archiving {len(html_files)} HTML files...")
    aconn = get_archive_db()
    archived = 0

    for fpath in html_files:
        fname = os.path.basename(fpath)
        ad_id = os.path.splitext(fname)[0]
        if not ad_id.isdigit():
            continue
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            aconn.execute(
                "INSERT OR IGNORE INTO raw_html (ad_id, html, fetched_at) VALUES (?,?,?)",
                (ad_id, html, "migrated"),
            )
            archived += 1
            if archived % 500 == 0:
                aconn.commit()
                print(f"  Archived {archived}...")
        except Exception as e:
            print(f"  [ERROR] {fname}: {e}")

    aconn.commit()
    aconn.close()
    print(f"Archived {archived} HTML files into html_archive.db")
    return archived


def print_stats():
    """Print DB stats after migration."""
    conn = get_db()
    n_listings = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    n_images = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    n_tags = conn.execute("SELECT COUNT(*) FROM listing_tags").fetchone()[0]
    n_leafs = conn.execute("SELECT COUNT(*) FROM leaf_urls").fetchone()[0]
    n_with_phone = conn.execute("SELECT COUNT(*) FROM listings WHERE phone IS NOT NULL").fetchone()[0]
    n_with_coords = conn.execute("SELECT COUNT(*) FROM listings WHERE lat IS NOT NULL").fetchone()[0]
    n_washer = conn.execute("SELECT COUNT(*) FROM listings WHERE has_washing_machine=1").fetchone()[0]
    n_dishwasher = conn.execute("SELECT COUNT(*) FROM listings WHERE has_dishwasher=1").fetchone()[0]
    n_ac = conn.execute("SELECT COUNT(*) FROM listings WHERE has_air_conditioning=1").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  Migration Complete — {DB_PATH}")
    print(f"{'='*60}")
    print(f"  Listings     : {n_listings}")
    print(f"  With phone   : {n_with_phone}")
    print(f"  With coords  : {n_with_coords}")
    print(f"  Images (URLs): {n_images}")
    print(f"  Tags         : {n_tags}")
    print(f"  Leaf URLs    : {n_leafs}")
    print(f"  Washer det.  : {n_washer}")
    print(f"  Dishwasher   : {n_dishwasher}")
    print(f"  AC detected  : {n_ac}")
    print(f"{'='*60}\n")

    if n_listings > 0:
        row = conn.execute(
            "SELECT MIN(price_eur), MAX(price_eur), ROUND(AVG(price_eur),0) "
            "FROM listings WHERE price_eur IS NOT NULL"
        ).fetchone()
        if row[0]:
            print(f"  Price: {row[0]:.0f}€ – {row[1]:.0f}€ (avg {row[2]:.0f}€)")

    conn.close()


def main():
    print("Njuskalo DB Migration")
    print("=" * 60)
    print(f"Target DB: {DB_PATH}")
    print(f"Archive DB: {ARCHIVE_DB_PATH}")
    print()

    # Ensure schema exists
    init_db()

    # 1. Migrate leaf URLs
    migrate_leaf_urls()
    print()

    # 2. Migrate JSON listings (includes phone merge)
    migrate_json_listings()
    print()

    # 3. Archive HTML files
    migrate_html_archive()
    print()

    # Stats
    print_stats()


if __name__ == "__main__":
    main()
