"""Knowledge Loader for Universal Data Format & Semantics Knowledge Repository.

Provides thread-safe, cached in-memory access to JSON knowledge files and metadata descriptors.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class KnowledgeLoader:
    """Manages loading and caching of knowledge datasets and metadata descriptors."""

    def __init__(self, base_dir: Optional[Path] = None, enable_cache: bool = True) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parent.parent
        self.enable_cache = enable_cache
        self._content_cache: Dict[str, Dict[str, Any]] = {}
        self._meta_cache: Dict[str, Dict[str, Any]] = {}
        self._record_index: Dict[str, Dict[str, Any]] = {}
        self._initialized: bool = False

    def load_json(self, rel_or_abs_path: str | Path) -> Dict[str, Any]:
        """Loads a JSON file, resolving relative paths against repository root."""
        p = Path(rel_or_abs_path)
        if not p.is_absolute():
            p = self.base_dir / p

        cache_key = str(p.resolve())
        if self.enable_cache and cache_key in self._content_cache:
            return self._content_cache[cache_key]

        if not p.exists():
            raise FileNotFoundError(f"Knowledge file not found at: {p}")

        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)

        if self.enable_cache:
            self._content_cache[cache_key] = data

        return data

    def load_dataset(self, content_path: str) -> Dict[str, Any]:
        """Loads dataset content file."""
        return self.load_json(content_path)

    def load_descriptor(self, descriptor_path: str) -> Dict[str, Any]:
        """Loads dataset metadata descriptor."""
        p = Path(descriptor_path)
        if not p.is_absolute():
            p = self.base_dir / p

        cache_key = str(p.resolve())
        if self.enable_cache and cache_key in self._meta_cache:
            return self._meta_cache[cache_key]

        data = self.load_json(p)
        if self.enable_cache:
            self._meta_cache[cache_key] = data
        return data

    def index_all_records(self, registry_datasets: List[Dict[str, Any]]) -> None:
        """Indexes all individual records by stable ID for fast direct lookup."""
        self._record_index.clear()
        for item in registry_datasets:
            content_path = item.get("content_path")
            if not content_path:
                continue
            try:
                data = self.load_dataset(content_path)
                for rec in data.get("records", []):
                    rec_id = rec.get("record_id") or rec.get("mapping_id") or rec.get("format_id")
                    if rec_id:
                        self._record_index[rec_id] = {
                            "record": rec,
                            "dataset_id": item.get("dataset_id"),
                            "category": item.get("category"),
                            "content_path": content_path,
                        }
            except Exception:
                pass
        self._initialized = True

    def get_record_by_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Direct O(1) lookup of any record by stable ID."""
        return self._record_index.get(record_id)

    def clear_cache(self) -> None:
        """Clears memory caches."""
        self._content_cache.clear()
        self._meta_cache.clear()
        self._record_index.clear()
        self._initialized = False
