"""CLI Script: Trigger source refresh and validation snapshot."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from services.source_collector import SourceCollector
from services.schema_validator import RepositoryValidator


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect and snapshot knowledge.")
    parser.add_argument("--snapshot", action="store_true", help="Create rollback snapshot before updates")
    parser.add_argument("--label", default="manual_collection", help="Snapshot label")
    args = parser.parse_args()

    collector = SourceCollector(base_dir=root_dir)
    validator = RepositoryValidator(base_dir=root_dir)

    # 1. Validate before proceeding
    report = validator.validate_repository()
    if not report.is_valid:
        print("Cannot collect or update: repository currently in invalid state!")
        for err in report.errors:
            print(f"  ! {err}")
        return 1

    # 2. Snapshot if requested
    if args.snapshot:
        snap_path = collector.create_snapshot(args.label)
        print(f"Created rollback snapshot at: {snap_path}")

    print("Knowledge repository ready and validated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
