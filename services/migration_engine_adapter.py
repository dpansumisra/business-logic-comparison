"""Migration Engine Adapter for Universal Data Format & Semantics Knowledge Repository.

Provides seamless, backward-compatible integration for external migration engines,
facilitating semantic operation detection, RCA diagnostics, and behavioral corrections.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from services.registry_service import RegistryService
from services.knowledge_search import KnowledgeSearchEngine
from services.cross_language_mapper import CrossLanguageMapper
from services.semantic_resolver import SemanticResolver, SemanticResolutionResult
from services.llm_context_builder import LLMContextBuilder


class MigrationEngineAdapter:
    """Zero-disruption adapter interface for cross-language migration engines."""

    def __init__(
        self,
        enabled: bool = True,
        registry_service: Optional[RegistryService] = None,
        max_context_tokens: int = 4096,
    ) -> None:
        self.enabled = enabled
        self.registry_service = registry_service or RegistryService()
        self.search_engine = KnowledgeSearchEngine(self.registry_service)
        self.mapper = CrossLanguageMapper(self.registry_service)
        self.resolver = SemanticResolver(self.mapper, self.search_engine)
        self.context_builder = LLMContextBuilder(self.search_engine, max_token_budget=max_context_tokens)

    def analyze_semantic_operation(
        self,
        source_language: str,
        target_language: str,
        operation: str,
        source_expression: str,
        target_expression: str,
        source_type: Optional[str] = None,
        target_type: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Core integration entry point: inspects source/target expressions and returns RCA findings."""
        if not self.enabled:
            return {
                "knowledge_assisted": False,
                "compatibility": "BYPASS",
                "message": "Knowledge repository integration is disabled via feature flag."
            }

        context = context or {}

        # 1. Resolve semantics
        resolution: SemanticResolutionResult = self.resolver.resolve(
            source_language=source_language,
            target_language=target_language,
            operation=operation,
            source_representation=source_type or source_expression,
            target_representation=target_type or target_expression,
            known_context=context,
        )

        # 2. Build LLM prompt guidance package
        llm_context = self.context_builder.build_context(resolution)
        prompt_snippet = self.context_builder.format_as_prompt_section(llm_context)

        # 3. Detect if target expression matches dangerous naive patterns
        needs_correction = False
        correction_reason = None

        if resolution.compatibility in ["CONVERTIBLE", "CONDITIONALLY_EQUIVALENT", "INCOMPATIBLE"]:
            if source_language.upper() == "SAS" and target_language.upper() == "PYTHON":
                if "date" in (source_type or "").lower() and "+" in target_expression:
                    # e.g., target has `old_date + 30`
                    if "timedelta" not in target_expression:
                        needs_correction = True
                        correction_reason = "SAS numeric date addition of integers (+30) must use timedelta(days=30) in Python."
                elif "missing" in (source_type or "").lower() or "<" in target_expression:
                    if "is not None" not in target_expression and "<" in target_expression:
                        needs_correction = True
                        correction_reason = "SAS missing values evaluate as < 0; Python None < 0 raises TypeError."

        return {
            "knowledge_assisted": True,
            "source": {
                "language": source_language,
                "expression": source_expression,
                "type": source_type,
            },
            "target": {
                "language": target_language,
                "expression": target_expression,
                "type": target_type,
            },
            "semantic_decision": resolution.to_dict(),
            "needs_correction": needs_correction,
            "correction_reason": correction_reason,
            "recommended_target_code": resolution.code_example,
            "llm_prompt_guidance": prompt_snippet,
            "evidence_citations": resolution.evidence,
        }
