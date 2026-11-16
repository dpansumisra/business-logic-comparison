"""Schema Validator for Universal Data Format & Semantics Knowledge Repository.

Validates JSON syntax, schema conformance, metadata descriptors, and cross-record reference integrity.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import jsonschema


class ValidationReport:
    """Encapsulates repository validation findings."""

    def __init__(self) -> None:
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.validated_files: int = 0
        self.validated_records: int = 0

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "validated_files": self.validated_files,
            "validated_records": self.validated_records,
            "errors": self.errors,
            "warnings": self.warnings,
        }


class RepositoryValidator:
    """Validates structural and relational integrity of the knowledge repository."""

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parent.parent
        self.schemas_dir = self.base_dir / "schemas"
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        self._load_schemas()

    def _load_schemas(self) -> None:
        """Pre-loads local schemas for validation."""
        if not self.schemas_dir.exists():
            return
        for schema_file in self.schemas_dir.glob("*.json"):
            try:
                with open(schema_file, "r", encoding="utf-8") as f:
                    self._schema_cache[schema_file.name] = json.load(f)
            except Exception as e:
                # Malformed schema
                pass

    def validate_json_file(self, file_path: Path, schema_name: Optional[str] = None) -> List[str]:
        """Validates a single JSON file against a schema if provided."""
        errors: List[str] = []
        if not file_path.exists():
            return [f"File not found: {file_path}"]

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return [f"Invalid JSON syntax in {file_path}: {e}"]

        if schema_name and schema_name in self._schema_cache:
            schema = self._schema_cache[schema_name]
            validator = jsonschema.Draft202012Validator(schema)
            for err in validator.iter_errors(data):
                errors.append(f"[{file_path.name}] {err.message} at path: {'/'.join(str(p) for p in err.path)}")

        return errors

    def validate_repository(self) -> ValidationReport:
        """Comprehensive verification of registry, descriptors, datasets, and references."""
        report = ValidationReport()
        registry_path = self.base_dir / "registry" / "knowledge_registry.json"

        # 1. Validate Central Registry
        if not registry_path.exists():
            report.errors.append(f"Central registry missing at {registry_path}")
            return report

        reg_errors = self.validate_json_file(registry_path, "registry.schema.json")
        report.errors.extend(reg_errors)
        report.validated_files += 1

        with open(registry_path, "r", encoding="utf-8") as f:
            registry_data = json.load(f)

        all_record_ids: set[str] = set()
        referenced_record_ids: set[tuple[str, str]] = set()  # (referencing_file, id)

        # 2. Validate Every Dataset and Descriptor
        datasets = registry_data.get("datasets", [])
        for entry in datasets:
            dataset_id = entry.get("dataset_id")
            desc_rel = entry.get("descriptor_path")
            content_rel = entry.get("content_path")

            desc_path = self.base_dir / desc_rel
            content_path = self.base_dir / content_rel

            # Validate descriptor (.meta.json)
            desc_errors = self.validate_json_file(desc_path, "descriptor.schema.json")
            report.errors.extend(desc_errors)
            report.validated_files += 1

            if desc_path.exists():
                with open(desc_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                if meta.get("dataset_id") != dataset_id:
                    report.errors.append(f"Dataset ID mismatch: registry={dataset_id} vs meta={meta.get('dataset_id')}")

            # Validate content (.json)
            if not content_path.exists():
                report.errors.append(f"Content file missing: {content_path}")
                continue

            try:
                with open(content_path, "r", encoding="utf-8") as f:
                    content = json.load(f)
                report.validated_files += 1

                # Determine schema from meta
                schema_ref = meta.get("schema_ref") if desc_path.exists() else None
                schema_name = Path(schema_ref).name if schema_ref else None

                content_errors = self.validate_json_file(content_path, schema_name)
                report.errors.extend(content_errors)

                records = content.get("records", [])
                report.validated_records += len(records)

                # Validate record IDs uniqueness
                for rec in records:
                    rec_id = rec.get("record_id") or rec.get("mapping_id") or rec.get("format_id")
                    if rec_id:
                        if rec_id in all_record_ids:
                            report.errors.append(f"Duplicate stable ID detected: {rec_id} in {content_path.name}")
                        all_record_ids.add(rec_id)

                    # Collect references
                    for rel in rec.get("related_concepts", []) + rec.get("related_knowledge", []):
                        referenced_record_ids.add((content_path.name, rel))

            except Exception as e:
                report.errors.append(f"Error reading {content_path}: {e}")

        # 3. Validate Cross-References
        for source_file, target_id in referenced_record_ids:
            # Check if concept or record ID exists
            if target_id not in all_record_ids and not target_id.startswith("CONCEPT_"):
                report.warnings.append(f"Unresolved reference to ID '{target_id}' found in {source_file}")

        # 4. Validate Source Catalog
        catalog_path = self.base_dir / "sources" / "source_catalog.json"
        if catalog_path.exists():
            cat_errors = self.validate_json_file(catalog_path, "source.schema.json")
            report.errors.extend(cat_errors)
            report.validated_files += 1

        return report
