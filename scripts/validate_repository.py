"""CLI Script: Validate repository integrity, JSON schemas, descriptors, and cross-references."""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from services.schema_validator import RepositoryValidator


def main() -> int:
    print("=" * 70)
    print("Running Universal Data Knowledge Repository Validation...")
    print("=" * 70)

    validator = RepositoryValidator(base_dir=root_dir)
    report = validator.validate_repository()

    print(f"Validated Files   : {report.validated_files}")
    print(f"Validated Records : {report.validated_records}")
    print(f"Errors Found      : {len(report.errors)}")
    print(f"Warnings Found    : {len(report.warnings)}")

    if report.warnings:
        print("\n[WARNINGS]")
        for w in report.warnings:
            print(f"  * {w}")

    if report.errors:
        print("\n[ERRORS]")
        for err in report.errors:
            print(f"  ! {err}")
        print("\nValidation FAILED.")
        return 1

    print("\nValidation PASSED successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
