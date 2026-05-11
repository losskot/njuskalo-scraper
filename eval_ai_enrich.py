#!/usr/bin/env python3
"""
eval_ai_enrich.py — Evaluate AI enrichment quality against structured tags.

Compares AI-enriched fields to authoritative structured tags from the website.
Tags are ground truth: they come from the poster's dropdown/checkbox selections.

Usage:
    python eval_ai_enrich.py [--re-enrich N]  # re-enrich N listings to test new prompt
    python eval_ai_enrich.py                  # evaluate existing enriched data
"""
import argparse
import json
import sys
import time

from db import get_db


def evaluate():
    db = get_db()

    rows = db.execute("""
        SELECT l.id, l.no_agency_fee, l.has_bathtub, l.has_washing_machine,
               l.has_dishwasher, l.has_air_conditioning, l.heating_type, l.street
        FROM listings l
        WHERE l.ai_enriched_at IS NOT NULL
    """).fetchall()

    if not rows:
        print("No enriched listings to evaluate.")
        return

    results = {
        "no_agency_fee": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
        "has_bathtub": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
        "has_washing_machine": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
        "has_dishwasher": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
        "has_air_conditioning": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
        "heating_type": {"correct": 0, "wrong": 0, "no_tag": 0, "errors": []},
    }

    for r in rows:
        lid = r["id"]
        tags = db.execute(
            "SELECT category, value FROM listing_tags WHERE listing_id=?", (lid,)
        ).fetchall()
        tag_dict = {}
        for t in tags:
            tag_dict.setdefault(t[0], []).append(t[1])

        # --- no_agency_fee ---
        fee_tags = [v for v in tag_dict.get("Troškovi", []) if "provizij" in v.lower()]
        if fee_tags:
            tag_no_fee = any("ne plaća" in v.lower() for v in fee_tags)
            ai_no_fee = bool(r["no_agency_fee"])
            if ai_no_fee == tag_no_fee:
                results["no_agency_fee"]["correct"] += 1
            else:
                results["no_agency_fee"]["wrong"] += 1
                results["no_agency_fee"]["errors"].append(
                    f"  {lid}: AI={ai_no_fee}, tag={tag_no_fee} ({fee_tags[0]})"
                )
        else:
            results["no_agency_fee"]["no_tag"] += 1

        # --- has_bathtub ---
        feature_tags = tag_dict.get("Funkcionalnosti i ostale karakteristike stana", [])
        if "Kada" in feature_tags:
            tag_bathtub = True
        elif any("Tuš" in v for v in feature_tags):
            tag_bathtub = False
        else:
            tag_bathtub = None

        if tag_bathtub is not None:
            ai_bathtub = bool(r["has_bathtub"]) if r["has_bathtub"] is not None else None
            if ai_bathtub == tag_bathtub:
                results["has_bathtub"]["correct"] += 1
            else:
                results["has_bathtub"]["wrong"] += 1
                results["has_bathtub"]["errors"].append(
                    f"  {lid}: AI={ai_bathtub}, tag={'Kada' if tag_bathtub else 'Tuš'}"
                )
        else:
            results["has_bathtub"]["no_tag"] += 1

        # --- has_washing_machine ---
        if "Perilica rublja" in feature_tags or "Perilica-sušilica" in feature_tags:
            tag_val = True
        else:
            tag_val = None  # absence doesn't mean false (might not have been tagged)
        if tag_val is not None:
            ai_val = bool(r["has_washing_machine"]) if r["has_washing_machine"] is not None else None
            if ai_val == tag_val:
                results["has_washing_machine"]["correct"] += 1
            else:
                results["has_washing_machine"]["wrong"] += 1
                results["has_washing_machine"]["errors"].append(
                    f"  {lid}: AI={ai_val}, tag=True"
                )
        else:
            results["has_washing_machine"]["no_tag"] += 1

        # --- has_dishwasher ---
        if "Perilica posuđa" in feature_tags:
            tag_val = True
        else:
            tag_val = None
        if tag_val is not None:
            ai_val = bool(r["has_dishwasher"]) if r["has_dishwasher"] is not None else None
            if ai_val == tag_val:
                results["has_dishwasher"]["correct"] += 1
            else:
                results["has_dishwasher"]["wrong"] += 1
                results["has_dishwasher"]["errors"].append(
                    f"  {lid}: AI={ai_val}, tag=True"
                )
        else:
            results["has_dishwasher"]["no_tag"] += 1

        # --- has_air_conditioning ---
        heating_tags = tag_dict.get("Grijanje", [])
        ac_tags = [v for v in heating_tags if "Klima" in v]
        if ac_tags:
            tag_ac = "Da" in ac_tags[0]
            ai_ac = bool(r["has_air_conditioning"]) if r["has_air_conditioning"] is not None else None
            if ai_ac == tag_ac:
                results["has_air_conditioning"]["correct"] += 1
            else:
                results["has_air_conditioning"]["wrong"] += 1
                results["has_air_conditioning"]["errors"].append(
                    f"  {lid}: AI={ai_ac}, tag={tag_ac}"
                )
        else:
            results["has_air_conditioning"]["no_tag"] += 1

        # --- heating_type ---
        heat_sys = [v for v in heating_tags if "Sustav grijanja:" in v]
        if heat_sys:
            tag_heat = heat_sys[0].replace("Sustav grijanja: ", "").strip()
            ai_heat = (r["heating_type"] or "").strip()
            if ai_heat.lower() == tag_heat.lower():
                results["heating_type"]["correct"] += 1
            elif tag_heat.lower() in ai_heat.lower() or ai_heat.lower() in tag_heat.lower():
                results["heating_type"]["correct"] += 1  # partial match OK
            else:
                results["heating_type"]["wrong"] += 1
                results["heating_type"]["errors"].append(
                    f"  {lid}: AI=\"{ai_heat}\", tag=\"{tag_heat}\""
                )
        else:
            results["heating_type"]["no_tag"] += 1

    # Print report
    print(f"\n{'='*60}")
    print(f"AI ENRICHMENT QUALITY REPORT ({len(rows)} listings)")
    print(f"{'='*60}\n")

    for field, stats in results.items():
        total_testable = stats["correct"] + stats["wrong"]
        if total_testable == 0:
            pct = "N/A"
        else:
            pct = f"{stats['correct']/total_testable*100:.1f}%"
        print(f"{field}:")
        print(f"  Accuracy: {pct} ({stats['correct']}/{total_testable} correct, {stats['no_tag']} untestable)")
        if stats["errors"]:
            print(f"  Errors ({len(stats['errors'])}):")
            for e in stats["errors"][:5]:
                print(f"    {e}")
            if len(stats["errors"]) > 5:
                print(f"    ... and {len(stats['errors'])-5} more")
        print()

    db.close()


def re_enrich(limit):
    """Reset ai_enriched_at for N listings and re-run enrichment."""
    db = get_db()
    db.execute(
        "UPDATE listings SET ai_enriched_at = NULL WHERE id IN "
        "(SELECT id FROM listings WHERE ai_enriched_at IS NOT NULL LIMIT ?)",
        (limit,),
    )
    db.commit()
    count = db.execute(
        "SELECT COUNT(*) FROM listings WHERE ai_enriched_at IS NULL"
    ).fetchone()[0]
    db.close()
    print(f"Reset {limit} listings. Now {count} pending enrichment.")
    print("Run: python ai_enrich.py --limit", limit)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--re-enrich", type=int, default=0,
                        help="Reset N enriched listings for re-processing")
    args = parser.parse_args()

    if args.re_enrich:
        re_enrich(args.re_enrich)
    else:
        evaluate()
