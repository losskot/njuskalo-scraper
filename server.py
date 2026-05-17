#!/usr/bin/env python3
"""
server.py — Flask API + static viewer for listings database.

Endpoints:
    GET /                           — static HTML viewer
    GET /api/listings               — filtered listings (paginated)
    GET /api/listings/<id>          — single listing with images + tags
    GET /api/stats                  — aggregate stats
    GET /api/filters                — distinct filter values

Usage:
    python server.py [--port 5000] [--host 0.0.0.0]
"""
import hashlib
import json
import os
import sqlite3
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory

from db import DB_PATH, init_db

BATHTUB_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend", "bathtub_results.db")

app = Flask(__name__, static_folder="static")


@app.after_request
def set_no_cache(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def jitter_coords(listing_id, lat, lng):
    """Apply a deterministic small offset to approximate coordinates so stacked markers spread out."""
    h = int(hashlib.md5(listing_id.encode()).hexdigest(), 16)
    lat_offset = ((h & 0xFFFF) / 0xFFFF - 0.5) * 0.004
    lng_offset = (((h >> 16) & 0xFFFF) / 0xFFFF - 0.5) * 0.004
    return lat + lat_offset, lng + lng_offset


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_bathtub_conn():
    """Get a read-only connection to bathtub_results.db, or None if it doesn't exist."""
    if not os.path.exists(BATHTUB_DB_PATH):
        return None
    conn = sqlite3.connect(BATHTUB_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_bathtub_classifications(listing_id):
    """Return dict of image_file → bool (True = bathroom_with_bathtub by majority vote)."""
    bconn = get_bathtub_conn()
    if not bconn:
        return {}
    try:
        rows = bconn.execute(
            "SELECT image_file, "
            "SUM(category='bathroom_with_bathtub') bathtub_votes, "
            "COUNT(*) total_votes "
            "FROM results WHERE listing_id=? AND error IS NULL "
            "GROUP BY image_file",
            (listing_id,),
        ).fetchall()
        return {r["image_file"]: r["bathtub_votes"] > r["total_votes"] / 2 for r in rows}
    finally:
        bconn.close()


def parse_available_from(s):
    """Parse Croatian-format date strings like '01.06.2026.' or '1.5.2026.' into a date."""
    if not s:
        return None
    s = s.strip().rstrip('.')
    parts = s.split('.')
    if len(parts) >= 3:
        try:
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
            if year < 100:
                year += 2000
            return datetime(year, month, day)
        except (ValueError, TypeError):
            return None
    return None


# ─── Static viewer ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ─── API: Filtered listings ───────────────────────────────────────────────────

@app.route("/api/listings")
def api_listings():
    conn = get_conn()
    conditions = []
    params = []

    # Price filter
    price_min = request.args.get("price_min", type=float)
    price_max = request.args.get("price_max", type=float)
    if price_min is not None:
        conditions.append("price_eur >= ?")
        params.append(price_min)
    if price_max is not None:
        conditions.append("price_eur <= ?")
        params.append(price_max)

    # Rooms filter (comma-separated)
    rooms = request.args.get("rooms")
    if rooms:
        room_list = [r.strip() for r in rooms.split(",") if r.strip()]
        if room_list:
            placeholders = ",".join("?" * len(room_list))
            conditions.append(f"rooms IN ({placeholders})")
            params.extend(room_list)

    # Area filter
    area_min = request.args.get("area_min", type=float)
    area_max = request.args.get("area_max", type=float)
    if area_min is not None:
        conditions.append("area_m2 >= ?")
        params.append(area_min)
    if area_max is not None:
        conditions.append("area_m2 <= ?")
        params.append(area_max)

    # Map bounds filter
    lat_min = request.args.get("lat_min", type=float)
    lat_max = request.args.get("lat_max", type=float)
    lng_min = request.args.get("lng_min", type=float)
    lng_max = request.args.get("lng_max", type=float)
    if all(v is not None for v in [lat_min, lat_max, lng_min, lng_max]):
        conditions.append("lat >= ? AND lat <= ? AND lng >= ? AND lng <= ?")
        params.extend([lat_min, lat_max, lng_min, lng_max])

    # Boolean filters
    if request.args.get("has_bathtub") == "1":
        conditions.append("has_bathtub = 1")
    if request.args.get("no_agency_fee") == "1":
        conditions.append("no_agency_fee = 1")
    if request.args.get("has_washing_machine") == "1":
        conditions.append("has_washing_machine = 1")
    if request.args.get("has_dishwasher") == "1":
        conditions.append("has_dishwasher = 1")
    if request.args.get("has_air_conditioning") == "1":
        conditions.append("has_air_conditioning = 1")
    if request.args.get("starred") == "1":
        conditions.append("starred = 1")
    if request.args.get("no_label") == "1":
        conditions.append("no_label = 1")

    # Heating type filter
    heating = request.args.get("heating_type")
    if heating:
        conditions.append("heating_type LIKE ?")
        params.append(f"%{heating}%")

    # Text search
    search = request.args.get("q")
    if search:
        conditions.append("(title LIKE ? OR description LIKE ? OR location LIKE ? OR street LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])

    # Only listings with coordinates (for map)
    if request.args.get("has_coords") == "1":
        conditions.append("lat IS NOT NULL AND lng IS NOT NULL")

    # Build query
    where = " AND ".join(conditions) if conditions else "1=1"
    limit = request.args.get("limit", 500, type=int)
    offset = request.args.get("offset", 0, type=int)

    # Availability date filter (parsed in Python due to mixed text formats)
    avail_after_str = request.args.get("avail_after")   # ISO date: YYYY-MM-DD
    avail_before_str = request.args.get("avail_before")  # ISO date: YYYY-MM-DD
    avail_after = datetime.fromisoformat(avail_after_str) if avail_after_str else None
    avail_before = datetime.fromisoformat(avail_before_str) if avail_before_str else None

    # Published date filter (parsed in Python — stored as "DD.MM.YYYY." Croatian format)
    pub_after_str = request.args.get("published_after")   # ISO date: YYYY-MM-DD
    pub_before_str = request.args.get("published_before")  # ISO date: YYYY-MM-DD
    pub_after = datetime.fromisoformat(pub_after_str) if pub_after_str else None
    pub_before = datetime.fromisoformat(pub_before_str) if pub_before_str else None

    # Count total matching (without date filter — date filter is post-SQL)
    count_row = conn.execute(f"SELECT COUNT(*) FROM listings WHERE {where}", params).fetchone()
    total = count_row[0]

    # Fetch listings (use higher limit when date filter active to compensate for post-SQL filtering)
    has_date_filter = avail_after or avail_before or pub_after or pub_before
    fetch_limit = limit * 4 if has_date_filter else limit
    rows = conn.execute(
        f"SELECT id, title, price, price_eur, location, lat, lng, lat_approximate, rooms, area_m2, "
        f"street, phone, has_bathtub, no_agency_fee, has_washing_machine, "
        f"has_dishwasher, has_air_conditioning, heating_type, url, available_from, "
        f"published_at, agency_name, furnishing, starred, no_label, read_at "
        f"FROM listings WHERE {where} ORDER BY price_eur ASC NULLS LAST "
        f"LIMIT ? OFFSET ?",
        params + [fetch_limit, offset],
    ).fetchall()

    def parse_published_at(s):
        """Parse Croatian-format published_at like '24.04.2026.' or '24.04.2026. u 10:30'."""
        if not s:
            return None
        s = s.split(" u ")[0].strip().rstrip(".")
        try:
            return datetime.strptime(s, "%d.%m.%Y")
        except (ValueError, TypeError):
            return None

    listings = []
    for r in rows:
        # Apply availability date filter in Python
        if avail_after or avail_before:
            avail_dt = parse_available_from(r["available_from"])
            if avail_after and (avail_dt is None or avail_dt < avail_after):
                continue
            if avail_before and (avail_dt is None or avail_dt > avail_before):
                continue
        # Apply published date filter in Python
        if pub_after or pub_before:
            pub_dt = parse_published_at(r["published_at"])
            if pub_after and (pub_dt is None or pub_dt < pub_after):
                continue
            if pub_before and (pub_dt is None or pub_dt > pub_before):
                continue
        lat, lng = r["lat"], r["lng"]
        if r["lat_approximate"] and lat is not None and lng is not None:
            lat, lng = jitter_coords(r["id"], lat, lng)
        listings.append({
            "id": r["id"],
            "title": r["title"],
            "price": r["price"],
            "price_eur": r["price_eur"],
            "location": r["location"],
            "lat": lat,
            "lng": lng,
            "rooms": r["rooms"],
            "area_m2": r["area_m2"],
            "street": r["street"],
            "phone": r["phone"],
            "has_bathtub": r["has_bathtub"],
            "no_agency_fee": r["no_agency_fee"],
            "has_washing_machine": r["has_washing_machine"],
            "has_dishwasher": r["has_dishwasher"],
            "has_air_conditioning": r["has_air_conditioning"],
            "heating_type": r["heating_type"],
            "url": r["url"],
            "available_from": r["available_from"],
            "published_at": r["published_at"],
            "agency_name": r["agency_name"],
            "furnishing": r["furnishing"],
            "starred": r["starred"],
            "no_label": r["no_label"],
            "read_at": r["read_at"],
        })

    conn.close()
    return jsonify({"total": total, "listings": listings})


# ─── API: Single listing detail ───────────────────────────────────────────────

@app.route("/api/listings/<listing_id>")
def api_listing_detail(listing_id):
    conn = get_conn()
    row = conn.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404

    listing = dict(row)

    # Images with per-image bathtub flag (majority vote across models)
    images = conn.execute(
        "SELECT url, position FROM images WHERE listing_id=? ORDER BY position",
        (listing_id,),
    ).fetchall()
    bathtub_by_file = get_bathtub_classifications(listing_id)
    images_out = []
    for img in images:
        fname = img["url"].rsplit("/", 1)[-1]
        is_bath = bathtub_by_file.get(fname, False)
        images_out.append({"url": img["url"], "position": img["position"], "has_bathtub": is_bath})
    # Bathtub photos come first, then by original position
    images_out.sort(key=lambda x: (0 if x["has_bathtub"] else 1, x["position"]))
    listing["images"] = images_out

    # Tags
    tags = conn.execute(
        "SELECT category, value FROM listing_tags WHERE listing_id=?",
        (listing_id,),
    ).fetchall()
    tags_dict = {}
    for t in tags:
        tags_dict.setdefault(t["category"], []).append(t["value"])
    listing["tags"] = tags_dict

    conn.close()
    return jsonify(listing)


# ─── API: Bathtub images for a listing ───────────────────────────────────────

@app.route("/api/listings/<listing_id>/bathtub_images")
def api_bathtub_images(listing_id):
    """Return URLs of photos classified as bathroom_with_bathtub (majority vote)."""
    conn = get_conn()
    images = conn.execute(
        "SELECT url FROM images WHERE listing_id=? ORDER BY position",
        (listing_id,),
    ).fetchall()
    conn.close()
    bathtub_by_file = get_bathtub_classifications(listing_id)
    urls = [img["url"] for img in images if bathtub_by_file.get(img["url"].rsplit("/", 1)[-1], False)]
    return jsonify({"images": urls})


# ─── API: Bulk bathtub photos for map view ──────────────────────────────

@app.route("/api/bathtub_map_photos")
def api_bathtub_map_photos():
    """Bulk fetch bathtub photos for multiple listings (comma-separated ids param).
    Returns {listing_id: [url, ...]} only for listings that have classified bathtub photos."""
    ids_param = request.args.get("ids", "").strip()
    if not ids_param:
        return jsonify({})
    ids = [i.strip() for i in ids_param.split(",") if i.strip()][:400]
    if not ids:
        return jsonify({})

    # Fetch image URLs from listings.db
    conn = get_conn()
    placeholders = ",".join("?" * len(ids))
    images = conn.execute(
        f"SELECT listing_id, url FROM images WHERE listing_id IN ({placeholders}) ORDER BY listing_id, position",
        ids,
    ).fetchall()
    conn.close()

    urls_by_listing = {}
    for img in images:
        urls_by_listing.setdefault(img["listing_id"], []).append(img["url"])

    # Fetch bathtub classifications from bathtub_results.db
    bconn = get_bathtub_conn()
    if not bconn:
        return jsonify({})
    try:
        rows = bconn.execute(
            f"SELECT listing_id, image_file, "
            f"SUM(category='bathroom_with_bathtub') bathtub_votes, COUNT(*) total_votes "
            f"FROM results WHERE listing_id IN ({placeholders}) AND error IS NULL "
            f"GROUP BY listing_id, image_file",
            ids,
        ).fetchall()
    finally:
        bconn.close()

    # Build set of bathtub filenames per listing
    bath_files = {}
    for r in rows:
        if r["bathtub_votes"] > r["total_votes"] / 2:
            bath_files.setdefault(r["listing_id"], set()).add(r["image_file"])

    # Return only listings that have at least one bathtub photo
    result = {}
    for lid, files in bath_files.items():
        urls = [u for u in urls_by_listing.get(lid, []) if u.rsplit("/", 1)[-1] in files]
        if urls:
            result[lid] = urls

    return jsonify(result)


# ─── API: Stats ───────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    conn = get_conn()
    stats = {}
    stats["total"] = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    stats["with_coords"] = conn.execute("SELECT COUNT(*) FROM listings WHERE lat IS NOT NULL").fetchone()[0]
    stats["with_phone"] = conn.execute("SELECT COUNT(*) FROM listings WHERE phone IS NOT NULL").fetchone()[0]
    stats["ai_enriched"] = conn.execute("SELECT COUNT(*) FROM listings WHERE ai_enriched_at IS NOT NULL").fetchone()[0]
    stats["bathtub_analyzed"] = conn.execute("SELECT COUNT(*) FROM listings WHERE bathtub_analyzed_at IS NOT NULL").fetchone()[0]
    stats["has_bathtub"] = conn.execute("SELECT COUNT(*) FROM listings WHERE has_bathtub=1").fetchone()[0]
    stats["no_agency_fee"] = conn.execute("SELECT COUNT(*) FROM listings WHERE no_agency_fee=1").fetchone()[0]
    stats["has_washing_machine"] = conn.execute("SELECT COUNT(*) FROM listings WHERE has_washing_machine=1").fetchone()[0]
    stats["has_dishwasher"] = conn.execute("SELECT COUNT(*) FROM listings WHERE has_dishwasher=1").fetchone()[0]
    stats["has_air_conditioning"] = conn.execute("SELECT COUNT(*) FROM listings WHERE has_air_conditioning=1").fetchone()[0]

    price_row = conn.execute(
        "SELECT MIN(price_eur), MAX(price_eur), ROUND(AVG(price_eur),0) FROM listings WHERE price_eur IS NOT NULL"
    ).fetchone()
    stats["price_min"] = price_row[0]
    stats["price_max"] = price_row[1]
    stats["price_avg"] = price_row[2]

    conn.close()
    return jsonify(stats)


# ─── API: Filter values ──────────────────────────────────────────────────────

@app.route("/api/filters")
def api_filters():
    conn = get_conn()

    # Distinct room types
    rooms = [r[0] for r in conn.execute(
        "SELECT DISTINCT rooms FROM listings WHERE rooms IS NOT NULL ORDER BY rooms"
    ).fetchall()]

    # Distinct heating types
    heating = [r[0] for r in conn.execute(
        "SELECT DISTINCT heating_type FROM listings WHERE heating_type IS NOT NULL ORDER BY heating_type"
    ).fetchall()]

    # Distinct locations (top 50 by count)
    locations = [r[0] for r in conn.execute(
        "SELECT location FROM listings WHERE location IS NOT NULL "
        "GROUP BY location ORDER BY COUNT(*) DESC LIMIT 50"
    ).fetchall()]

    # Price range
    price_row = conn.execute(
        "SELECT MIN(price_eur), MAX(price_eur) FROM listings WHERE price_eur IS NOT NULL"
    ).fetchone()

    # Area range
    area_row = conn.execute(
        "SELECT MIN(area_m2), MAX(area_m2) FROM listings WHERE area_m2 IS NOT NULL"
    ).fetchone()

    conn.close()
    return jsonify({
        "rooms": rooms,
        "heating_types": heating,
        "locations": locations,
        "price_range": [price_row[0], price_row[1]] if price_row[0] else [0, 5000],
        "area_range": [area_row[0], area_row[1]] if area_row[0] else [0, 500],
    })


# ─── API: Toggle star ────────────────────────────────────────────────────────

@app.route("/api/listings/<listing_id>/star", methods=["POST"])
def api_toggle_star(listing_id):
    conn = get_conn()
    row = conn.execute("SELECT starred FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404
    new_starred = 0 if row["starred"] else 1
    # Mutual exclusion: starring clears no_label
    if new_starred:
        conn.execute("UPDATE listings SET starred=1, no_label=0 WHERE id=?", (listing_id,))
    else:
        conn.execute("UPDATE listings SET starred=0 WHERE id=?", (listing_id,))
    conn.commit()
    conn.close()
    return jsonify({"starred": new_starred, "no_label": 0 if new_starred else None})


# ─── API: Toggle no label ────────────────────────────────────────────────────

@app.route("/api/listings/<listing_id>/no", methods=["POST"])
def api_toggle_no(listing_id):
    conn = get_conn()
    row = conn.execute("SELECT no_label FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404
    new_no_label = 0 if row["no_label"] else 1
    # Mutual exclusion: marking no clears star
    if new_no_label:
        conn.execute("UPDATE listings SET no_label=1, starred=0 WHERE id=?", (listing_id,))
    else:
        conn.execute("UPDATE listings SET no_label=0 WHERE id=?", (listing_id,))
    conn.commit()
    conn.close()
    return jsonify({"no_label": new_no_label, "starred": 0 if new_no_label else None})




@app.route("/api/listings/<listing_id>/tags", methods=["PATCH"])
def api_update_tags(listing_id):
    conn = get_conn()
    row = conn.execute("SELECT id FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404

    data = request.get_json(force=True)
    allowed = {"has_bathtub", "has_air_conditioning", "has_dishwasher"}
    updates = {k: int(v) for k, v in data.items() if k in allowed and v in (0, 1, True, False)}

    if updates:
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(
            f"UPDATE listings SET {set_clause} WHERE id=?",
            list(updates.values()) + [listing_id],
        )
        conn.commit()

    conn.close()
    return jsonify({"updated": updates})


# ─── API: Mark as read ──────────────────────────────────────────────────────

@app.route("/api/listings/<listing_id>/read", methods=["POST"])
def api_mark_read(listing_id):
    conn = get_conn()
    row = conn.execute("SELECT read_at FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404
    # Toggle: if already read, clear; otherwise set
    if row["read_at"]:
        conn.execute("UPDATE listings SET read_at=NULL WHERE id=?", (listing_id,))
        new_read_at = None
    else:
        new_read_at = datetime.utcnow().isoformat()
        conn.execute("UPDATE listings SET read_at=? WHERE id=?", (new_read_at, listing_id))
    conn.commit()
    conn.close()
    return jsonify({"read_at": new_read_at})


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    init_db()
    app.run(host=args.host, port=args.port, debug=True)
