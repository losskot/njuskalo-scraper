"""
db.py — Shared database module for Njuskalo scraper.

Two databases:
  listings.db      — all structured data (listings, images, tags, categories, checkpoints, logs)
  html_archive.db  — raw HTML pages for history/re-parsing

Usage:
    from db import get_db, get_archive_db, upsert_listing, ...
"""
import json
import os
import re
import sqlite3
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "backend", "listings.db")
ARCHIVE_DB_PATH = os.path.join(SCRIPT_DIR, "backend", "html_archive.db")

os.makedirs(os.path.join(SCRIPT_DIR, "backend"), exist_ok=True)

# ─── Schema ──────────────────────────────────────────────────────────────────

LISTINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id                  TEXT PRIMARY KEY,
    url                 TEXT,
    title               TEXT,
    price               TEXT,
    price_eur           REAL,
    location            TEXT,
    lat                 REAL,
    lng                 REAL,
    lat_approximate     INTEGER,
    description         TEXT,
    apartment_type      TEXT,
    floors_count        TEXT,
    rooms               TEXT,
    floor               TEXT,
    area_m2             REAL,
    net_area_m2         REAL,
    year_built          TEXT,
    year_renovated      TEXT,
    furnishing          TEXT,
    parking_spots       TEXT,
    total_floors        TEXT,
    energy_class        TEXT,
    property_code       TEXT,
    street              TEXT,
    available_from      TEXT,
    expires             TEXT,
    pets_allowed        TEXT,
    video_tour          TEXT,
    agency_name         TEXT,
    agency_profile_url  TEXT,
    agency_address      TEXT,
    agency_email        TEXT,
    phone               TEXT,
    published_at        TEXT,
    views               TEXT,
    -- Deterministic extraction (set during parse)
    has_washing_machine INTEGER,
    has_dishwasher      INTEGER,
    has_air_conditioning INTEGER,
    heating_type        TEXT,
    -- AI enrichment (set by ai_enrich.py)
    no_agency_fee       INTEGER,
    has_bathtub         INTEGER,
    description_uk      TEXT,
    ai_enriched_at      TEXT,
    -- Metadata
    scraped_at          TEXT
);

CREATE TABLE IF NOT EXISTS images (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id  TEXT NOT NULL REFERENCES listings(id),
    url         TEXT NOT NULL,
    position    INTEGER
);

CREATE TABLE IF NOT EXISTS listing_tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id  TEXT NOT NULL REFERENCES listings(id),
    category    TEXT NOT NULL,
    value       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id   INTEGER REFERENCES categories(id),
    name        TEXT NOT NULL,
    url         TEXT,
    depth       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS leaf_urls (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER REFERENCES categories(id),
    url         TEXT NOT NULL UNIQUE,
    scraped_at  TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT NOT NULL,
    level       TEXT NOT NULL,
    script      TEXT,
    message     TEXT NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_images_listing    ON images(listing_id);
CREATE INDEX IF NOT EXISTS idx_tags_listing      ON listing_tags(listing_id);
CREATE INDEX IF NOT EXISTS idx_tags_category     ON listing_tags(category);
CREATE INDEX IF NOT EXISTS idx_listings_location ON listings(location);
CREATE INDEX IF NOT EXISTS idx_listings_price    ON listings(price_eur);
CREATE INDEX IF NOT EXISTS idx_listings_rooms    ON listings(rooms);
CREATE INDEX IF NOT EXISTS idx_listings_lat_lng  ON listings(lat, lng);
CREATE INDEX IF NOT EXISTS idx_leaf_urls_url     ON leaf_urls(url);
"""

ARCHIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_html (
    ad_id       TEXT PRIMARY KEY,
    html        TEXT NOT NULL,
    fetched_at  TEXT NOT NULL
);
"""

# ─── Connection helpers ───────────────────────────────────────────────────────

def get_db(path=None):
    """Get a connection to listings.db with WAL mode and foreign keys."""
    conn = sqlite3.connect(path or DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def get_archive_db(path=None):
    """Get a connection to html_archive.db."""
    conn = sqlite3.connect(path or ARCHIVE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create all tables and indexes if they don't exist. Add new columns to existing tables."""
    conn = get_db()
    conn.executescript(LISTINGS_SCHEMA)
    conn.commit()

    # Add new columns to existing listings table (safe ALTERs — ignore if already exist)
    new_columns = [
        ("has_washing_machine", "INTEGER"),
        ("has_dishwasher", "INTEGER"),
        ("has_air_conditioning", "INTEGER"),
        ("heating_type", "TEXT"),
        ("no_agency_fee", "INTEGER"),
        ("has_bathtub", "INTEGER"),
        ("description_uk", "TEXT"),
        ("ai_enriched_at", "TEXT"),
        ("scraped_at", "TEXT"),
    ]
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()}
    for col_name, col_type in new_columns:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
    conn.commit()
    conn.close()

    aconn = get_archive_db()
    aconn.executescript(ARCHIVE_SCHEMA)
    aconn.commit()
    aconn.close()


# ─── Parsing helpers ──────────────────────────────────────────────────────────

def parse_area(val):
    """'1.200,00 m²' → 1200.0, '26,00 m²' → 26.0"""
    if not val:
        return None
    s = str(val).replace(".", "").replace(",", ".")
    m = re.search(r"[\d.]+", s)
    return float(m.group()) if m else None


def parse_price(val):
    """'800 €' → 800.0"""
    if not val:
        return None
    m = re.search(r"[\d\s\.]+", str(val).replace(".", "").replace("\xa0", ""))
    if m:
        try:
            return float(m.group().replace(" ", ""))
        except ValueError:
            pass
    return None


# ─── Deterministic feature extraction ────────────────────────────────────────

def extract_features(tags_dict, description=""):
    """
    Extract appliance/heating features from parsed tag groups and description.

    Args:
        tags_dict: dict of {category: [values]} e.g. {"Grijanje": ["Klima uređaj: Da", ...]}
        description: listing description text

    Returns:
        dict with has_washing_machine, has_dishwasher, has_air_conditioning, heating_type
    """
    features = {
        "has_washing_machine": None,
        "has_dishwasher": None,
        "has_air_conditioning": None,
        "heating_type": None,
    }

    # Collect all tag values
    funkc = tags_dict.get("Funkcionalnosti i ostale karakteristike stana", [])
    grijanje = tags_dict.get("Grijanje", [])
    all_tags_text = " ".join(funkc + grijanje).lower()
    desc_lower = (description or "").lower()

    # Washing machine
    if "perilica rublja" in all_tags_text or "perilica rublja" in desc_lower or "perilicom rublja" in desc_lower:
        features["has_washing_machine"] = 1
    elif "perilica-sušilica" in all_tags_text or "perilicom-sušilicom" in desc_lower:
        features["has_washing_machine"] = 1

    # Dishwasher
    if "perilica posuđa" in all_tags_text or "perilica posuđa" in desc_lower or "perilicom posuđa" in desc_lower:
        features["has_dishwasher"] = 1

    # Air conditioning
    for val in grijanje:
        if "klima uređaj" in val.lower() and "da" in val.lower():
            features["has_air_conditioning"] = 1
            break
    if features["has_air_conditioning"] is None:
        if "klima" in desc_lower and ("uređaj" in desc_lower or "klimatiziran" in desc_lower):
            features["has_air_conditioning"] = 1

    # Heating type — extract value after "Sustav grijanja:"
    for val in grijanje:
        if val.lower().startswith("sustav grijanja:"):
            heating = val.split(":", 1)[1].strip()
            if heating:
                features["heating_type"] = heating
                break

    return features


# ─── SCALAR_FIELDS mapping (JSON key → DB column) ────────────────────────────

SCALAR_FIELDS = {
    "id":                           "id",
    "link":                         "url",
    "naslov":                       "title",
    "cijena":                       "price",
    "Lokacija":                     "location",
    "opis":                         "description",
    "Tip stana":                    "apartment_type",
    "Broj etaža":                   "floors_count",
    "Broj soba":                    "rooms",
    "Kat":                          "floor",
    "Stambena površina":            "area_m2",
    "Netto površina":               "net_area_m2",
    "Godina izgradnje":             "year_built",
    "Godina zadnje renovacije":     "year_renovated",
    "Namještenost i stanje":        "furnishing",
    "Broj parkirnih mjesta":        "parking_spots",
    "Ukupni broj katova":           "total_floors",
    "Energetski razred":            "energy_class",
    "Šifra objekta":                "property_code",
    "Ulica":                        "street",
    "Dostupno od":                  "available_from",
    "do_isteka":                    "expires",
    "Kućni ljubimci dozvoljeni":    "pets_allowed",
    "Razgledavanje putem video poziva": "video_tour",
    "naziv_agencije":               "agency_name",
    "profil_agencije":              "agency_profile_url",
    "adresa_agencije":              "agency_address",
    "email_agencije":               "agency_email",
    "telefon":                      "phone",
    "oglas_objavljen":              "published_at",
    "oglas_prikazan":               "views",
}

LIST_FIELDS = [
    "Grijanje",
    "Podaci o objektu",
    "Troškovi",
    "Vrsta parkinga",
    "Funkcionalnosti i ostale karakteristike stana",
    "Orijentacija stana",
    "Kupaonica i WC",
    "Balkon/Lođa/Terasa",
    "Dozvole i potvrde",
    "Ostali objekti i površine",
    "Dostupnost kroz godinu",
]


# ─── Upsert helpers ──────────────────────────────────────────────────────────

def upsert_listing(conn, parsed_data):
    """
    Insert or update a listing from parsed data dict.
    Also inserts images and listing_tags.
    Returns the ad_id.
    """
    ad_id = parsed_data.get("id")
    if not ad_id:
        return None

    # Build scalar row
    row = {}
    for json_key, col in SCALAR_FIELDS.items():
        row[col] = parsed_data.get(json_key)

    # Parsed numeric price
    row["price_eur"] = parse_price(parsed_data.get("cijena"))

    # Parsed area
    area_raw = parsed_data.get("Stambena površina") or parsed_data.get("Netto površina")
    row["area_m2"] = parse_area(area_raw)
    row["net_area_m2"] = parse_area(parsed_data.get("Netto površina"))

    # GPS coords
    loc_obj = parsed_data.get("lokacija") or {}
    if isinstance(loc_obj, dict):
        row["lat"] = loc_obj.get("lat")
        row["lng"] = loc_obj.get("lng")
        row["lat_approximate"] = 1 if loc_obj.get("approximate") else 0
    else:
        row["lat"] = row["lng"] = row["lat_approximate"] = None

    # Deterministic feature extraction
    tags_dict = {}
    for field in LIST_FIELDS:
        val = parsed_data.get(field)
        if isinstance(val, list):
            tags_dict[field] = val
        elif isinstance(val, str) and val:
            tags_dict[field] = [val]
    features = extract_features(tags_dict, row.get("description", ""))
    row.update(features)

    # Metadata
    row["scraped_at"] = datetime.now().isoformat()

    # Upsert listing
    cols = ", ".join(row.keys())
    placeholders = ", ".join("?" * len(row))
    update_clause = ", ".join(f"{col}=excluded.{col}" for col in row.keys() if col != "id")
    conn.execute(
        f"INSERT INTO listings ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {update_clause}",
        list(row.values()),
    )

    # Replace images
    conn.execute("DELETE FROM images WHERE listing_id=?", (ad_id,))
    for pos, img_url in enumerate(parsed_data.get("slike", [])):
        conn.execute(
            "INSERT INTO images (listing_id, url, position) VALUES (?,?,?)",
            (ad_id, img_url, pos),
        )

    # Replace tags
    conn.execute("DELETE FROM listing_tags WHERE listing_id=?", (ad_id,))
    for field in LIST_FIELDS:
        values = parsed_data.get(field)
        if isinstance(values, list):
            for val in values:
                if val:
                    conn.execute(
                        "INSERT INTO listing_tags (listing_id, category, value) VALUES (?,?,?)",
                        (ad_id, field, str(val)),
                    )
        elif isinstance(values, str) and values:
            conn.execute(
                "INSERT INTO listing_tags (listing_id, category, value) VALUES (?,?,?)",
                (ad_id, field, values),
            )

    return ad_id


def store_html(ad_id, html):
    """Store raw HTML in the archive database."""
    aconn = get_archive_db()
    aconn.execute(
        "INSERT OR REPLACE INTO raw_html (ad_id, html, fetched_at) VALUES (?,?,?)",
        (ad_id, html, datetime.now().isoformat()),
    )
    aconn.commit()
    aconn.close()


def listing_exists(conn, ad_id):
    """Check if a listing is already in the database."""
    return conn.execute("SELECT 1 FROM listings WHERE id=?", (ad_id,)).fetchone() is not None


# ─── Checkpoint helpers ───────────────────────────────────────────────────────

def get_checkpoint(conn, key):
    """Get a checkpoint value by key."""
    row = conn.execute("SELECT value FROM checkpoints WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def set_checkpoint(conn, key, value):
    """Set a checkpoint value (stores as JSON)."""
    conn.execute(
        "INSERT OR REPLACE INTO checkpoints (key, value, updated_at) VALUES (?,?,?)",
        (key, json.dumps(value), datetime.now().isoformat()),
    )
    conn.commit()


# ─── Log helper ───────────────────────────────────────────────────────────────

def log_event(conn, level, script, message):
    """Insert a log entry into the logs table."""
    conn.execute(
        "INSERT INTO logs (timestamp, level, script, message) VALUES (?,?,?,?)",
        (datetime.now().isoformat(), level, script, message),
    )
    conn.commit()


# ─── Initialize on import ─────────────────────────────────────────────────────

init_db()
