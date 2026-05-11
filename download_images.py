"""
Download listing images from slike field in JSON files.
Saves to: backend/images/{ad_id}/{filename}.jpg
"""
import asyncio
import json
import os
import glob
from pathlib import Path
from curl_cffi.requests import AsyncSession

IMAGES_DIR = os.path.join(os.path.dirname(__file__), "backend", "images")
JSON_DIR   = os.path.join(os.path.dirname(__file__), "backend", "json")

CONCURRENT = 20   # parallel downloads

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.njuskalo.hr/",
    "accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}


async def download_image(session, sem, url, dest_path):
    if os.path.exists(dest_path):
        return True  # already downloaded
    async with sem:
        try:
            resp = await asyncio.wait_for(
                session.get(url, headers=HEADERS, impersonate="chrome110"),
                timeout=15,
            )
            resp.raise_for_status()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            with open(dest_path, "wb") as f:
                f.write(resp.content)
            return True
        except Exception as e:
            print(f"[FAIL] {url}: {e}")
            return False


async def main():
    os.makedirs(IMAGES_DIR, exist_ok=True)

    files = sorted(glob.glob(os.path.join(JSON_DIR, "*.json")))
    if not files:
        print("No JSON files found in backend/json/")
        return

    # Build task list
    tasks_meta = []  # (ad_id, url, dest_path)
    for fpath in files:
        data = json.load(open(fpath, encoding="utf-8"))
        ad_id = data.get("id") or Path(fpath).stem
        slike = data.get("slike", [])
        ad_dir = os.path.join(IMAGES_DIR, ad_id)
        for url in slike:
            fname = url.split("/")[-1]
            dest = os.path.join(ad_dir, fname)
            tasks_meta.append((ad_id, url, dest))

    total = len(tasks_meta)
    # Count already downloaded
    already = sum(1 for _, _, d in tasks_meta if os.path.exists(d))
    pending = total - already
    print(f"Total images : {total}")
    print(f"Already saved: {already}")
    print(f"To download  : {pending}")
    if pending == 0:
        print("Nothing to do.")
        return

    sem = asyncio.Semaphore(CONCURRENT)
    done = 0
    ok = 0
    fail = 0

    async with AsyncSession() as session:
        coros = [
            download_image(session, sem, url, dest)
            for _, url, dest in tasks_meta
            if not os.path.exists(dest)
        ]
        for coro in asyncio.as_completed(coros):
            result = await coro
            done += 1
            if result:
                ok += 1
            else:
                fail += 1
            if done % 100 == 0 or done == pending:
                pct = done / pending * 100
                print(f"  [{pct:5.1f}%] {done}/{pending}  ok={ok} fail={fail}", flush=True)

    print(f"\nDone. Downloaded {ok}/{pending} images  ({fail} failed)")
    print(f"Saved to: {IMAGES_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
