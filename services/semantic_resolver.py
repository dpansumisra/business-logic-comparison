"""Semantic Resolver for Universal Data Format & Semantics Knowledge Repository.

Compares source and target code semantics, identifies behavioral divergences, evaluates
compatibility classifications, and produces behavior-preserving guidance and evidence.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from services.cross_language_mapper import CrossLanguageMapper
from services.knowledge_search import KnowledgeSearchEngine


class SemanticResolutionResult:
    """Structured response from the semantic resolution engine."""

    def __init__(
        self,
        concept: str,
        compatibility: str,
        source_semantics: Dict[str, Any],
        target_semantics: Dict[str, Any],
        identified_risks: List[str],
        recommended_mapping: Optional[str],
        preservation_rules: List[str],
        confidence: str,
        evidence: List[str],
        conversion_strategy: Optional[str] = None,
        code_example: Optional[str] = None,
        unresolved_assumptions: Optional[List[str]] = None,
    ) -> None:
        self.concept = concept
        self.compatibility = compatibility
        self.source_semantics = source_semantics
        self.target_semantics = target_semantics
        self.identified_risks = identified_risks
        self.recommended_mapping = recommended_mapping
        self.preservation_rules = preservation_rules
        self.confidence = confidence
        self.evidence = evidence
        self.conversion_strategy = conversion_strategy
        self.code_example = code_example
        self.unresolved_assumptions = unresolved_assumptions or []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "concept": self.concept,
            "compatibility": self.compatibility,
            "source_semantics": self.source_semantics,
            "target_semantics": self.target_semantics,
            "identified_risks": self.identified_risks,
            "recommended_mapping": self.recommended_mapping,
            "preservation_rules": self.preservation_rules,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "conversion_strategy": self.conversion_strategy,
            "code_example": self.code_example,
            "unresolved_assumptions": self.unresolved_assumptions,
        }


class SemanticResolver:
    """Analyzes source vs target operations and resolves semantic equivalence."""

    def __init__(
        self,
        mapper: CrossLanguageMapper,
        search_engine: KnowledgeSearchEngine
    ) -> None:
        self.mapper = mapper
        self.search_engine = search_engine

    def resolve(
        self,
        source_language: str,
        target_language: str,
        operation: Optional[str] = None,
        source_representation: Optional[str] = None,
        target_representation: Optional[str] = None,
        known_context: Optional[Dict[str, Any]] = None,
    ) -> SemanticResolutionResult:
        """Determines semantic compatibility and preservation strategy for an operation."""
        known_context = known_context or {}

        # 1. Map operation to potential concept ID if not directly specified
        concept_id = None
        op_norm = (operation or "").upper()
        src_rep_lower = (source_representation or "").lower()
        tgt_rep_lower = (target_representation or "").lower()

        # Differentiate between Timestamp Instant (datetime/timestamp) and Calendar Date (date)
        is_timestamp = (
            "DATETIME" in op_norm
            or "TIMESTAMP" in op_norm
            or "datetime.datetime" in tgt_rep_lower
            or "numeric_datetime" in src_rep_lower
            or ("timestamp" in src_rep_lower or "timestamp" in tgt_rep_lower)
            or ("time" in op_norm and "date" not in op_norm)
        )

        if is_timestamp:
            concept_id = "CONCEPT_TIMESTAMP_INSTANT"
        elif (
            "DATE" in op_norm
            or src_rep_lower == "numeric_date"
            or "datetime.date" in tgt_rep_lower
            or tgt_rep_lower == "date"
        ):
            concept_id = "CONCEPT_CALENDAR_DATE"
        elif "INT_DIV" in op_norm or "DIVISION" in op_norm:
            concept_id = "CONCEPT_INTEGER_DIVISION"
        elif "NULL" in op_norm or "MISSING" in op_norm:
            concept_id = "CONCEPT_SQL_NULL" if source_language.upper() == "SQL" else "CONCEPT_SAS_MISSING"
        elif "ROUND" in op_norm:
            concept_id = "CONCEPT_ROUNDING_MODE"
        elif "DECIMAL" in op_norm:
            concept_id = "CONCEPT_FIXED_DECIMAL"

        # 2. Look up matching cross-language mapping
        mapping = self.mapper.find_mapping(
            source_language=source_language,
            target_language=target_language,
            concept_id=concept_id,
            source_representation=source_representation,
            target_representation=target_representation,
        )

        # 3. If exact mapping found, build comprehensive resolution result
        if mapping:
            conv = mapping.get("conversion", {})
            risks = [d.get("risk") for d in mapping.get("semantic_differences", []) if d.get("risk")]
            if not risks:
                risks = [d.get("difference") for d in mapping.get("semantic_differences", [])]

            return SemanticResolutionResult(
                concept=mapping.get("concept_id", "CONCEPT_UNKNOWN"),
                compatibility=mapping.get("compatibility", "CONVERTIBLE"),
                source_semantics={
                    "language": mapping.get("source", {}).get("language"),
                    "representation": mapping.get("source", {}).get("representation"),
                    "unit": mapping.get("source", {}).get("unit"),
                    "epoch": mapping.get("source", {}).get("epoch"),
                },
                target_semantics={
                    "language": mapping.get("target", {}).get("language"),
                    "representation": mapping.get("target", {}).get("representation"),
                    "type": mapping.get("target", {}).get("type"),
                },
                identified_risks=risks,
                recommended_mapping=mapping.get("mapping_id"),
                preservation_rules=mapping.get("preservation_rules", []),
                confidence=mapping.get("confidence", "high"),
                evidence=mapping.get("evidence_sources", []) + mapping.get("related_knowledge", []),
                conversion_strategy=conv.get("strategy"),
                code_example=conv.get("code_example"),
                unresolved_assumptions=[]
            )

        # 4. Fallback when context is insufficient: UNCERTAIN
        # Never silently claim equivalence without evidence
        assumptions = []
        if not source_representation:
            assumptions.append("source_representation_unspecified")
        if not target_representation:
            assumptions.append("target_representation_unspecified")
        if not operation:
            assumptions.append("operation_unspecified")

        return SemanticResolutionResult(
            concept=concept_id or "CONCEPT_UNKNOWN",
            compatibility="UNCERTAIN",
            source_semantics={"language": source_language, "representation": source_representation},
            target_semantics={"language": target_language, "representation": target_representation},
            identified_risks=["insufficient_context_to_guarantee_semantic_equivalence"],
            recommended_mapping=None,
            preservation_rules=["Establish explicit data types and locale context before code conversion."],
            confidence="low",
            evidence=[],
            conversion_strategy=None,
            code_example=None,
            unresolved_assumptions=assumptions
        )
