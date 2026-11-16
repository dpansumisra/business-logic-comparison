"""Knowledge Search Engine for Universal Data Format & Semantics Knowledge Repository.

Provides multi-criteria search by concept ID, stable ID, language pairs, keywords, and tags.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from services.registry_service import RegistryService
from services.knowledge_loader import KnowledgeLoader


class KnowledgeSearchEngine:
    """Executes indexed and structured search over the knowledge repository."""

    def __init__(self, registry_service: RegistryService, loader: Optional[KnowledgeLoader] = None) -> None:
        self.registry_service = registry_service
        self.loader = loader or registry_service.loader

    def get_by_record_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Exact lookup by stable record ID."""
        return self.loader.get_record_by_id(record_id)

    def search_by_concept(self, concept_id: str) -> List[Dict[str, Any]]:
        """Retrieves all records matching a semantic concept ID."""
        matches: List[Dict[str, Any]] = []
        for ds in self.registry_service.list_datasets():
            records = self.registry_service.get_dataset_records(ds["dataset_id"])
            for rec in records:
                if rec.get("concept_id") == concept_id:
                    matches.append({
                        "record": rec,
                        "dataset_id": ds["dataset_id"],
                        "category": ds["category"],
                        "relevance": "exact_concept_match"
                    })
        return matches

    def search_by_language_pair(
        self,
        source_language: str,
        target_language: str,
        category: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Searches cross-language mappings matching source and target languages."""
        matches: List[Dict[str, Any]] = []
        datasets = self.registry_service.list_datasets(category="cross_language_mappings")
        for ds in datasets:
            records = self.registry_service.get_dataset_records(ds["dataset_id"])
            for rec in records:
                src = rec.get("source", {})
                tgt = rec.get("target", {})
                src_lang = src.get("language", "").upper()
                tgt_lang = tgt.get("language", "").upper()

                if src_lang == source_language.upper() and tgt_lang == target_language.upper():
                    matches.append({
                        "mapping_id": rec.get("mapping_id"),
                        "concept_id": rec.get("concept_id"),
                        "compatibility": rec.get("compatibility"),
                        "record": rec,
                        "dataset_id": ds["dataset_id"],
                        "relevance": "exact_language_pair"
                    })
        return matches

    def query(
        self,
        query: Optional[str] = None,
        category: Optional[str] = None,
        source_language: Optional[str] = None,
        target_language: Optional[str] = None,
        concepts: Optional[List[str]] = None,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """Unified multi-criteria search combining keyword, category, language, and concept filters."""
        results: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        # 1. Check direct stable ID match if query is an ID
        if query:
            cleaned_q = query.strip()
            direct = self.get_by_record_id(cleaned_q)
            if direct:
                rec = direct["record"]
                rec_id = rec.get("record_id") or rec.get("mapping_id")
                results.append({
                    "knowledge_id": rec_id,
                    "name": rec.get("name", rec_id),
                    "category": direct.get("category"),
                    "relevance": "exact_id_match",
                    "content_path": direct.get("content_path"),
                    "record": rec
                })
                seen_ids.add(rec_id)

        # 2. Search mappings if language pair is supplied
        if source_language and target_language:
            pair_matches = self.search_by_language_pair(source_language, target_language, category=category)
            for m in pair_matches:
                mid = m.get("mapping_id")
                if mid not in seen_ids:
                    results.append({
                        "knowledge_id": mid,
                        "name": m["record"].get("concept_id", mid),
                        "category": "cross_language_mappings",
                        "relevance": "exact_language_pair",
                        "record": m["record"]
                    })
                    seen_ids.add(mid)

        # 3. Concept matches
        if concepts:
            for cid in concepts:
                c_matches = self.search_by_concept(cid)
                for cm in c_matches:
                    r = cm["record"]
                    rid = r.get("record_id") or r.get("mapping_id")
                    if rid not in seen_ids:
                        results.append({
                            "knowledge_id": rid,
                            "name": r.get("name", rid),
                            "category": cm.get("category"),
                            "relevance": "concept_match",
                            "record": r
                        })
                        seen_ids.add(rid)

        # 4. Keyword / Tag scoring across datasets
        if query:
            tokens = [t.lower() for t in query.split() if len(t) > 1]
            for ds in self.registry_service.list_datasets(category=category):
                records = self.registry_service.get_dataset_records(ds["dataset_id"])
                for r in records:
                    rid = r.get("record_id") or r.get("mapping_id")
                    if rid in seen_ids:
                        continue

                    # Search inside record fields
                    blob = " ".join([
                        str(r.get("name", "")),
                        str(r.get("pattern", "")),
                        str(r.get("concept_id", "")),
                        str(r.get("interpretation", "")),
                        " ".join(r.get("validation_rules", [])),
                        " ".join(r.get("related_concepts", []))
                    ]).lower()

                    matches_count = sum(1 for t in tokens if t in blob)
                    if matches_count > 0:
                        results.append({
                            "knowledge_id": rid,
                            "name": r.get("name", rid),
                            "category": ds["category"],
                            "relevance": f"keyword_score_{matches_count}",
                            "record": r
                        })
                        seen_ids.add(rid)

        # Truncate to limit
        final_results = results[:limit]
        return {
            "query": query,
            "category": category,
            "source_language": source_language,
            "target_language": target_language,
            "total": len(final_results),
            "results": final_results
        }
