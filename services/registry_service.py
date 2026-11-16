"""Registry Service for Universal Data Format & Semantics Knowledge Repository.

Provides discovery and navigation across categories, languages, and registered knowledge datasets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from services.knowledge_loader import KnowledgeLoader


class RegistryService:
    """Provides high-level discovery over the central registry and category/language catalogs."""

    def __init__(self, base_dir: Optional[Path] = None, loader: Optional[KnowledgeLoader] = None) -> None:
        self.base_dir = base_dir or Path(__file__).resolve().parent.parent
        self.loader = loader or KnowledgeLoader(base_dir=self.base_dir)
        self.registry_rel_path = "registry/knowledge_registry.json"
        self.categories_rel_path = "registry/category_registry.json"
        self.languages_rel_path = "registry/language_registry.json"
        self._registry_data: Optional[Dict[str, Any]] = None
        self._categories_data: Optional[Dict[str, Any]] = None
        self._languages_data: Optional[Dict[str, Any]] = None
        self.refresh()

    def refresh(self) -> None:
        """Reloads registries from disk and updates indexes."""
        self._registry_data = self.loader.load_json(self.registry_rel_path)
        try:
            self._categories_data = self.loader.load_json(self.categories_rel_path)
        except Exception:
            self._categories_data = {"categories": []}
        try:
            self._languages_data = self.loader.load_json(self.languages_rel_path)
        except Exception:
            self._languages_data = {"languages": []}

        # Index records in loader
        datasets = self._registry_data.get("datasets", [])
        self.loader.index_all_records(datasets)

    def get_registry_info(self) -> Dict[str, Any]:
        """Returns top-level registry metadata."""
        return {
            "registry_version": self._registry_data.get("registry_version"),
            "last_updated": self._registry_data.get("last_updated"),
            "category_count": len(self._registry_data.get("categories", [])),
            "dataset_count": len(self._registry_data.get("datasets", [])),
        }

    def list_categories(self) -> List[Dict[str, Any]]:
        """Returns all recognized knowledge categories."""
        return self._categories_data.get("categories", [])

    def list_languages(self) -> List[Dict[str, Any]]:
        """Returns all registered programming languages and specifications."""
        return self._languages_data.get("languages", [])

    def list_datasets(self, category: Optional[str] = None, status: str = "active") -> List[Dict[str, Any]]:
        """Lists registered datasets with optional category and status filtering."""
        datasets = self._registry_data.get("datasets", [])
        results: List[Dict[str, Any]] = []
        for ds in datasets:
            if status and ds.get("status") != status:
                continue
            if category and ds.get("category", "").lower() != category.lower():
                continue
            results.append(ds)
        return results

    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Finds dataset entry in registry by ID."""
        for ds in self._registry_data.get("datasets", []):
            if ds.get("dataset_id") == dataset_id:
                return ds
        return None

    def get_dataset_metadata(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Loads the full .meta.json descriptor for a dataset."""
        ds = self.get_dataset(dataset_id)
        if not ds or "descriptor_path" not in ds:
            return None
        return self.loader.load_descriptor(ds["descriptor_path"])

    def get_dataset_records(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Loads all records from a dataset content file."""
        ds = self.get_dataset(dataset_id)
        if not ds or "content_path" not in ds:
            return []
        content = self.loader.load_dataset(ds["content_path"])
        return content.get("records", [])
