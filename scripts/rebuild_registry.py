"""CLI Script: Rebuilds central knowledge registry by scanning all .meta.json descriptors."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))


def main() -> int:
    print("Rebuilding central registry from metadata descriptors...")
    knowledge_dir = root_dir / "knowledge"
    registry_path = root_dir / "registry" / "knowledge_registry.json"

    meta_files = sorted(knowledge_dir.rglob("*.meta.json"))
    datasets = []

    for mf in meta_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                meta = json.load(f)

            content_rel = mf.relative_to(root_dir).as_posix()
            json_rel = content_rel.replace(".meta.json", ".json")

            datasets.append({
                "dataset_id": meta.get("dataset_id"),
                "name": meta.get("name"),
                "category": meta.get("category"),
                "subcategory": meta.get("subcategory"),
                "descriptor_path": content_rel,
                "content_path": json_rel,
                "status": meta.get("status", "active"),
                "version": meta.get("version", "1.0.0"),
                "tags": meta.get("tags", [])
            })
        except Exception as e:
            print(f"Error parsing {mf}: {e}")

    # Load existing categories
    categories = []
    if registry_path.exists():
        with open(registry_path, "r", encoding="utf-8") as f:
            old_reg = json.load(f)
            categories = old_reg.get("categories", [])

    new_registry = {
        "registry_version": "1.0.0",
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "categories": categories,
        "datasets": datasets
    }

    with open(registry_path, "w", encoding="utf-8") as f:
        json.dump(new_registry, f, indent=2)

    print(f"Successfully rebuilt registry with {len(datasets)} datasets.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
