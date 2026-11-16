"""Cross Language Mapper for Universal Data Format & Semantics Knowledge Repository.

Navigates the semantic compatibility graph to locate, filter, and evaluate conversion mappings.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from services.registry_service import RegistryService


class CrossLanguageMapper:
    """Manages cross-language semantic graph lookups and compatibility analysis."""

    def __init__(self, registry_service: RegistryService) -> None:
        self.registry_service = registry_service

    def find_mapping(
        self,
        source_language: str,
        target_language: str,
        concept_id: Optional[str] = None,
        source_representation: Optional[str] = None,
        target_representation: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Finds the most specific cross-language mapping matching source and target criteria."""
        datasets = self.registry_service.list_datasets(category="cross_language_mappings")
        best_candidate: Optional[Dict[str, Any]] = None
        best_score = -1

        for ds in datasets:
            records = self.registry_service.get_dataset_records(ds["dataset_id"])
            for rec in records:
                src = rec.get("source", {})
                tgt = rec.get("target", {})

                if (
                    src.get("language", "").upper() != source_language.upper()
                    or tgt.get("language", "").upper() != target_language.upper()
                ):
                    continue

                score = 10  # Language pair matched

                if concept_id:
                    if rec.get("concept_id") == concept_id:
                        score += 20
                    else:
                        continue  # Must match concept if requested

                if source_representation:
                    src_rep = src.get("representation", "").lower()
                    s_input = source_representation.lower()
                    if s_input == src_rep:
                        score += 30
                    elif s_input in src_rep or src_rep in s_input:
                        score += 15

                if target_representation:
                    tgt_rep = tgt.get("representation", "").lower()
                    t_input = target_representation.lower()
                    if t_input == tgt_rep:
                        score += 30
                    elif t_input in tgt_rep or tgt_rep in t_input:
                        score += 15

                if score > best_score:
                    best_score = score
                    best_candidate = rec

        return best_candidate

    def get_all_mappings(self) -> List[Dict[str, Any]]:
        """Returns all registered cross-language mappings in the repository."""
        datasets = self.registry_service.list_datasets(category="cross_language_mappings")
        all_mappings: List[Dict[str, Any]] = []
        for ds in datasets:
            records = self.registry_service.get_dataset_records(ds["dataset_id"])
            all_mappings.extend(records)
        return all_mappings
