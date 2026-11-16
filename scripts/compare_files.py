"""CLI Script: Compare two code files (source and target) for semantic divergence and RCA."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from services.code_comparator import CodeComparator


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare source and target files for semantic divergence.")
    parser.add_argument("--source", "-s", required=True, help="Path to source code file (e.g., .sas, .sql, .cpp)")
    parser.add_argument("--target", "-t", required=True, help="Path to target code file (e.g., .py, .js)")
    parser.add_argument("--source-lang", help="Explicit source language override")
    parser.add_argument("--target-lang", help="Explicit target language override")
    parser.add_argument("--json", action="store_true", help="Output raw JSON report")

    args = parser.parse_args()

    src_path = Path(args.source)
    tgt_path = Path(args.target)

    if not src_path.exists():
        print(f"Error: Source file does not exist: {src_path}", file=sys.stderr)
        return 1
    if not tgt_path.exists():
        print(f"Error: Target file does not exist: {tgt_path}", file=sys.stderr)
        return 1

    source_code = src_path.read_text(encoding="utf-8", errors="replace")
    target_code = tgt_path.read_text(encoding="utf-8", errors="replace")

    comparator = CodeComparator()
    report = comparator.compare_files(
        source_code=source_code,
        target_code=target_code,
        source_filename=src_path.name,
        target_filename=tgt_path.name,
        source_language=args.source_lang,
        target_language=args.target_lang,
    )

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print("=" * 70)
    print("Semantic Code Comparison & Root Cause Analysis (RCA) Report")
    print("=" * 70)
    print(f"Source: {report['source_file']} ({report['source_language']})")
    print(f"Target: {report['target_file']} ({report['target_language']})")
    print(f"Status: {report['overall_status']}")
    print(f"Corrections Required: {report['corrections_needed_count']}")
    print("-" * 70)

    for idx, f in enumerate(report["findings"], 1):
        print(f"\n[Finding #{idx}] Operation: {f['operation']} ({f['category']})")
        print(f"  * Compatibility: {f['compatibility']}")
        print(f"  * Source (Line {f['source']['line']}): {f['source']['expression']}")
        if f['target']['expression']:
            print(f"  * Target (Line {f['target']['line']}): {f['target']['expression']}")
        if f['needs_correction']:
            print(f"  ! Correction Required: {f['correction_reason']}")
        if f['recommended_code']:
            print(f"  + Recommended Target Code: {f['recommended_code']}")
        if f['preservation_rules']:
            print("  * Preservation Rules:")
            for r in f['preservation_rules']:
                print(f"      - {r}")

    print("\n" + "=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
