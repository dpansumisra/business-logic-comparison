"""CLI Script: Check registered external sources and standards for update policies and status."""

from __future__ import annotations

import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from services.source_collector import SourceCollector


def main() -> int:
    print("=" * 60)
    print("Checking Source Provenance & Status")
    print("=" * 60)

    collector = SourceCollector(base_dir=root_dir)
    status_list = collector.check_source_status()

    for s in status_list:
        print(f"[{s['status'].upper():8}] {s['source_id']:22} | {s['name'][:40]} (v: {s.get('version')})")

    print("\nAll sources monitored.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
