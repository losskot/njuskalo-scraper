
import os
import json

# Paths

from datetime import datetime
BASE_DIR = os.path.dirname(__file__)
today_str = datetime.now().strftime("%Y-%m-%d")
CATEGORIES_TREE_DIR = os.path.join(BASE_DIR, "backend", "categories", "tree_jsons")
LEAF_URLS_DIR = os.path.join(BASE_DIR, "backend", "categories", "leaf_urls")
os.makedirs(LEAF_URLS_DIR, exist_ok=True)

def collect_leaf_urls(node, leaf_urls, path=None):
    if not isinstance(node, dict):
        print(f"[WARN] Skipping non-dict node at {path or ''}: {repr(node)[:100]}")
        return
    if not node.get("children"):
        leaf_urls.append(node["url"])
    else:
        for idx, child in enumerate(node["children"]):
            collect_leaf_urls(child, leaf_urls, path=f"{path or node.get('name','root')}/{idx}")

def process_category_tree(tree_path, out_path):
    with open(tree_path, "r", encoding="utf-8") as f:
        tree = json.load(f)
    leaf_urls = []
    # The tree can be a list (multiple roots)
    for idx, node in enumerate(tree):
        collect_leaf_urls(node, leaf_urls, path=f"root/{idx}")
    with open(out_path, "w", encoding="utf-8") as f:
        for url in leaf_urls:
            f.write(url + "\n")
    print(f"Extracted {len(leaf_urls)} leaf URLs to {out_path}")

def main():
    for fname in os.listdir(CATEGORIES_TREE_DIR):
        # Only process today's tree files, but skip any starting with 'category_tree_'
        if fname.startswith("category_tree_"):
            continue
        if fname.endswith(f"_tree_{today_str}.json"):
            cat = fname.replace(f"_tree_{today_str}.json", "")
            tree_path = os.path.join(CATEGORIES_TREE_DIR, fname)
            out_path = os.path.join(LEAF_URLS_DIR, f"{cat}_leaf_urls_{today_str}.txt")
            process_category_tree(tree_path, out_path)

if __name__ == "__main__":
    main()
