"""Code Comparator Service for Universal Data Format & Semantics Knowledge Repository.

Analyzes two code files (source and target), detects semantic operations, compares
representations, and produces a structured semantic difference and RCA report.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from services.migration_engine_adapter import MigrationEngineAdapter


class CodeComparator:
    """Compares source and target code files and generates semantic difference reports."""

    def __init__(self, adapter: Optional[MigrationEngineAdapter] = None) -> None:
        self.adapter = adapter or MigrationEngineAdapter()

    def detect_language(self, filename_or_path: str) -> str:
        """Infers programming language from file extension."""
        suffix = Path(filename_or_path).suffix.lower()
        mapping = {
            ".sas": "SAS",
            ".py": "Python",
            ".sql": "SQL",
            ".js": "JavaScript",
            ".ts": "JavaScript",
            ".cpp": "C++",
            ".cc": "C++",
            ".h": "C++",
            ".java": "Java",
            ".cs": "C#",
        }
        return mapping.get(suffix, "Unknown")

    def _extract_operations(self, code_text: str, language: str) -> List[Dict[str, Any]]:
        """Scans code text for common data/semantic operations."""
        operations = []
        lines = code_text.splitlines()

        for line_idx, line in enumerate(lines, start=1):
            clean = line.strip()
            if not clean or clean.startswith(("*", "/*", "#", "//", "--")):
                continue

            lower = clean.lower()

            # 1. Date Arithmetic & Functions detection
            if any(k in lower for k in ["date", "datetime", "dt", "timestamp", "time"]) and "+" in clean:
                operations.append({
                    "line_number": line_idx,
                    "operation": "DATE_CONVERSION",
                    "expression": clean,
                    "type_hint": "numeric_date" if language == "SAS" else "datetime.date",
                    "category": "dates"
                })
            elif "datetime()" in lower or "e8601dt" in lower:
                operations.append({
                    "line_number": line_idx,
                    "operation": "DATETIME_CONVERSION",
                    "expression": clean,
                    "type_hint": "numeric_datetime" if language == "SAS" else "datetime.datetime",
                    "category": "dates"
                })
            elif "today()" in lower or "yymmdd" in lower or "date9" in lower:
                operations.append({
                    "line_number": line_idx,
                    "operation": "DATE_CONVERSION",
                    "expression": clean,
                    "type_hint": "numeric_date" if language == "SAS" else "datetime.date",
                    "category": "dates"
                })

            # 2. Null / Missing comparisons and functions
            if re.search(r"\bmissing\s*\(", lower):
                operations.append({
                    "line_number": line_idx,
                    "operation": "NULL_CHECK",
                    "expression": clean,
                    "type_hint": "numeric_missing" if language == "SAS" else "None",
                    "category": "nulls"
                })
            elif re.search(r"<\s*0|<\s*[0-9]+", clean) and ("if" in lower or "where" in lower):
                operations.append({
                    "line_number": line_idx,
                    "operation": "NULL_CHECK",
                    "expression": clean,
                    "type_hint": "numeric_missing" if language == "SAS" else "None",
                    "category": "nulls"
                })
            elif "is null" in lower or "is not null" in lower or "== none" in lower or ".isnull()" in lower:
                operations.append({
                    "line_number": line_idx,
                    "operation": "NULL_CHECK",
                    "expression": clean,
                    "type_hint": "NULL" if language == "SQL" else "None",
                    "category": "nulls"
                })

            # 3. Integer Division (strictly arithmetic, excluding path strings and imports)
            is_path_or_import = any(p in lower for p in ["%let", "libname", "infile", "path", "http", "import", "from "]) or ('"' in clean and "/" in clean)
            if not is_path_or_import and re.search(r"[a-zA-Z0-9_\)]\s*/\s*[a-zA-Z0-9_\(]", clean):
                operations.append({
                    "line_number": line_idx,
                    "operation": "INT_DIV",
                    "expression": clean,
                    "type_hint": "int" if language in ["C++", "Java", "SQL"] else "float",
                    "category": "numbers"
                })

            # 4. Rounding
            if re.search(r"\bround\s*\(", clean, re.IGNORECASE):
                operations.append({
                    "line_number": line_idx,
                    "operation": "DECIMAL_ROUND",
                    "expression": clean,
                    "type_hint": "ROUND(x, round_off_unit)" if language == "SAS" else "round(x, n)",
                    "category": "numbers"
                })

        return operations

    def compare_files(
        self,
        source_code: str,
        target_code: str,
        source_filename: str = "source.sas",
        target_filename: str = "target.py",
        source_language: Optional[str] = None,
        target_language: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compares source and target files, aligning detected operations and reporting semantic risks."""
        src_lang = source_language or self.detect_language(source_filename)
        tgt_lang = target_language or self.detect_language(target_filename)

        src_ops = self._extract_operations(source_code, src_lang)
        tgt_ops = self._extract_operations(target_code, tgt_lang)

        findings = []

        # Compare matching categories / operations
        for s_op in src_ops:
            # Look for corresponding operation in target
            matching_tgt = next((t for t in tgt_ops if t["operation"] == s_op["operation"]), None)
            tgt_expr = matching_tgt["expression"] if matching_tgt else ""
            tgt_type = matching_tgt["type_hint"] if matching_tgt else None
            tgt_line = matching_tgt["line_number"] if matching_tgt else None

            analysis = self.adapter.analyze_semantic_operation(
                source_language=src_lang,
                target_language=tgt_lang,
                operation=s_op["operation"],
                source_expression=s_op["expression"],
                target_expression=tgt_expr,
                source_type=s_op.get("type_hint"),
                target_type=tgt_type,
            )

            findings.append({
                "operation": s_op["operation"],
                "category": s_op["category"],
                "source": {
                    "line": s_op["line_number"],
                    "expression": s_op["expression"],
                    "type": s_op.get("type_hint"),
                },
                "target": {
                    "line": tgt_line,
                    "expression": tgt_expr,
                    "type": tgt_type,
                },
                "compatibility": analysis["semantic_decision"]["compatibility"],
                "needs_correction": analysis["needs_correction"],
                "correction_reason": analysis["correction_reason"],
                "recommended_code": analysis["recommended_target_code"],
                "preservation_rules": analysis["semantic_decision"].get("preservation_rules", []),
                "risks": analysis["semantic_decision"].get("identified_risks", []),
            })

        # Summary statistics
        corrections_needed = sum(1 for f in findings if f["needs_correction"])
        incompatibilities = sum(1 for f in findings if f["compatibility"] in ["INCOMPATIBLE", "CONDITIONALLY_EQUIVALENT"])

        return {
            "source_file": source_filename,
            "target_file": target_filename,
            "source_language": src_lang,
            "target_language": tgt_lang,
            "total_operations_detected": len(findings),
            "corrections_needed_count": corrections_needed,
            "potential_incompatibilities_count": incompatibilities,
            "findings": findings,
            "overall_status": "CORRECTIONS_REQUIRED" if corrections_needed > 0 else "COMPATIBLE_OR_REVIEW_NEEDED"
        }
