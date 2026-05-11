"""
Import all scraped Njuskalo listings into SQLite.

Tables:
  listings      — one row per ad (all scalar fields)
  images        — one row per image URL (FK → listings.id)
  listing_tags  — multi-value list fields stored as key/value rows
"""
import json
import os
import re
import glob
import sqlite3

JSON_DIR = os.path.join(os.path.dirname(__file__), "backend", "json")
IMAGES_DIR = os.path.join(os.path.dirname(__file__), "backend", "images")
DB_PATH = os.path.join(os.path.dirname(__file__), "backend", "listings.db")

# ── Fields that become their own columns in `listings` ───────────────────────
SCALAR_FIELDS = {
    # json key                     → column name
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

# ── Fields that are lists → stored in listing_tags ───────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────

def parse_area(val):
    """'26,00 m²' → 26.0"""
    if not val:
        return None
    m = re.search(r"[\d,\.]+", str(val).replace(",", "."))
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


def create_schema(conn):
    conn.executescript("""
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
        views               TEXT
    );

    CREATE TABLE IF NOT EXISTS images (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id  TEXT NOT NULL REFERENCES listings(id),
        url         TEXT NOT NULL,
        local_path  TEXT,
        position    INTEGER
    );

    CREATE TABLE IF NOT EXISTS listing_tags (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id  TEXT NOT NULL REFERENCES listings(id),
        category    TEXT NOT NULL,
        value       TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_images_listing    ON images(listing_id);
    CREATE INDEX IF NOT EXISTS idx_tags_listing      ON listing_tags(listing_id);
    CREATE INDEX IF NOT EXISTS idx_tags_category     ON listing_tags(category);
    CREATE INDEX IF NOT EXISTS idx_listings_location ON listings(location);
    CREATE INDEX IF NOT EXISTS idx_listings_price    ON listings(price_eur);
    CREATE INDEX IF NOT EXISTS idx_listings_rooms    ON listings(rooms);
    """)
    conn.commit()


def import_listings(conn):
    files = sorted(glob.glob(os.path.join(JSON_DIR, "*.json")))
    inserted = 0
    skipped = 0

    for fpath in files:
        data = json.load(open(fpath, encoding="utf-8"))
        ad_id = data.get("id")
        if not ad_id:
            continue

        # Check if already imported
        if conn.execute("SELECT 1 FROM listings WHERE id=?", (ad_id,)).fetchone():
            skipped += 1
            continue

        # ── Scalar row ─────────────────────────────────────────────────────
        row = {}
        for json_key, col in SCALAR_FIELDS.items():
            row[col] = data.get(json_key)

        # Parsed numeric price
        row["price_eur"] = parse_price(data.get("cijena"))

        # Parsed numeric area (use Stambena površina)
        area_raw = data.get("Stambena površina") or data.get("Netto površina")
        row["area_m2"] = parse_area(area_raw)
        net_raw = data.get("Netto površina")
        row["net_area_m2"] = parse_area(net_raw)

        # GPS coords
        loc_obj = data.get("lokacija") or {}
        if isinstance(loc_obj, dict):
            row["lat"] = loc_obj.get("lat")
            row["lng"] = loc_obj.get("lng")
            row["lat_approximate"] = 1 if loc_obj.get("approximate") else 0
        else:
            row["lat"] = row["lng"] = row["lat_approximate"] = None

        cols = ", ".join(row.keys())
        placeholders = ", ".join("?" * len(row))
        conn.execute(
            f"INSERT OR IGNORE INTO listings ({cols}) VALUES ({placeholders})",
            list(row.values()),
        )

        # ── Images ─────────────────────────────────────────────────────────
        for pos, img_url in enumerate(data.get("slike", [])):
            fname = img_url.split("/")[-1]
            local = os.path.join(IMAGES_DIR, ad_id, fname)
            local_path = local if os.path.exists(local) else None
            conn.execute(
                "INSERT INTO images (listing_id, url, local_path, position) VALUES (?,?,?,?)",
                (ad_id, img_url, local_path, pos),
            )

        # ── List-valued tags ────────────────────────────────────────────────
        for field in LIST_FIELDS:
            values = data.get(field)
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

        inserted += 1

    conn.commit()
    return inserted, skipped


def print_stats(conn):
    n_listings = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    n_images   = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    n_local    = conn.execute("SELECT COUNT(*) FROM images WHERE local_path IS NOT NULL").fetchone()[0]
    n_tags     = conn.execute("SELECT COUNT(*) FROM listing_tags").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  listings.db — import complete")
    print(f"{'='*60}")
    print(f"  Listings : {n_listings}")
    print(f"  Images   : {n_images}  ({n_local} downloaded locally)")
    print(f"  Tags     : {n_tags}")
    print(f"{'='*60}")

    print("\nPrice range:")
    row = conn.execute("SELECT MIN(price_eur), MAX(price_eur), ROUND(AVG(price_eur),0) FROM listings WHERE price_eur IS NOT NULL").fetchone()
    print(f"  Min={row[0]:.0f}€  Max={row[1]:.0f}€  Avg={row[2]:.0f}€")

    print("\nTop sub-districts:")
    for loc, cnt in conn.execute(
        "SELECT location, COUNT(*) c FROM listings GROUP BY location ORDER BY c DESC LIMIT 10"
    ):
        print(f"  {cnt:4d}  {loc}")

    print("\nRooms breakdown:")
    for rooms, cnt in conn.execute(
        "SELECT rooms, COUNT(*) c FROM listings WHERE rooms IS NOT NULL GROUP BY rooms ORDER BY c DESC"
    ):
        print(f"  {cnt:4d}  {rooms}")

    print(f"\nDatabase: {DB_PATH}\n")


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    print("Creating schema…")
    create_schema(conn)

    print("Importing listings…")
    inserted, skipped = import_listings(conn)
    print(f"  Inserted: {inserted}   Already present: {skipped}")

    print_stats(conn)
    conn.close()
