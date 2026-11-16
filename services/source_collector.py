"""Source Collector for Universal Data Format & Semantics Knowledge Repository.

Manages source catalog registration, version tracking, snapshots, and atomic updates.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


class SourceCollector:
    """Handles external source tracking, snapshotting, and atomic updates."""

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parent.parent
        self.catalog_path = self.base_dir / "sources" / "source_catalog.json"
        self.snapshots_dir = self.base_dir / "sources" / "source_snapshots"
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

    def load_catalog(self) -> Dict[str, Any]:
        """Loads registered sources catalog."""
        if not self.catalog_path.exists():
            return {"catalog_version": "1.0.0", "sources": []}
        with open(self.catalog_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_catalog(self, catalog: Dict[str, Any]) -> None:
        """Saves catalog atomically."""
        temp_path = self.catalog_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2)
        temp_path.replace(self.catalog_path)

    def register_source(
        self,
        source_id: str,
        name: str,
        publisher: str,
        source_url: str,
        source_type: str,
        categories: List[str],
        license_info: str,
        collection_method: str = "official_release_data",
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Registers a new external authoritative source."""
        catalog = self.load_catalog()
        for s in catalog.get("sources", []):
            if s.get("source_id") == source_id:
                raise ValueError(f"Source with ID {source_id} already exists.")

        entry = {
            "source_id": source_id,
            "name": name,
            "publisher": publisher,
            "source_url": source_url,
            "source_type": source_type,
            "categories": categories,
            "collection_method": collection_method,
            "license": license_info,
            "version": version,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "checksum": None,
            "status": "active"
        }
        catalog.setdefault("sources", []).append(entry)
        self.save_catalog(catalog)
        return entry

    def create_snapshot(self, label: str) -> Path:
        """Creates a timestamped snapshot of all knowledge and registry files for rollback."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        snapshot_dir = self.snapshots_dir / f"snapshot_{label}_{timestamp}"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        for folder in ["registry", "knowledge", "sources"]:
            src = self.base_dir / folder
            dst = snapshot_dir / folder
            if src.exists():
                shutil.copytree(src, dst)

        return snapshot_dir

    def calculate_file_checksum(self, file_path: Path) -> str:
        """Computes SHA-256 hash of a file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def check_source_status(self) -> List[Dict[str, Any]]:
        """Reports the health and update status of registered sources."""
        catalog = self.load_catalog()
        status_list: List[Dict[str, Any]] = []
        for s in catalog.get("sources", []):
            status_list.append({
                "source_id": s.get("source_id"),
                "name": s.get("name"),
                "status": s.get("status"),
                "version": s.get("version"),
                "retrieved_at": s.get("retrieved_at")
            })
        return status_list
