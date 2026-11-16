"""LLM Context Builder for Universal Data Format & Semantics Knowledge Repository.

Assembles compact, relevant, token-budgeted knowledge payloads tailored for LLM migration prompts.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from services.semantic_resolver import SemanticResolutionResult
from services.knowledge_search import KnowledgeSearchEngine


class LLMContextBuilder:
    """Formats targeted knowledge evidence into compact LLM context blocks."""

    def __init__(
        self,
        search_engine: KnowledgeSearchEngine,
        max_token_budget: int = 4096
    ) -> None:
        self.search_engine = search_engine
        self.max_token_budget = max_token_budget
        self._approx_chars_per_token = 4

    def build_context(
        self,
        resolution: SemanticResolutionResult,
        extra_query: Optional[str] = None,
        max_records: int = 4
    ) -> Dict[str, Any]:
        """Constructs a budgeted, structured knowledge package for an LLM prompt."""
        context_payload: Dict[str, Any] = {
            "semantic_summary": {
                "concept": resolution.concept,
                "compatibility": resolution.compatibility,
                "confidence": resolution.confidence,
                "identified_risks": resolution.identified_risks,
                "conversion_strategy": resolution.conversion_strategy,
                "recommended_code": resolution.code_example,
                "preservation_rules": resolution.preservation_rules,
            },
            "knowledge_evidence": [],
            "provenance": {
                "evidence_sources": resolution.evidence,
                "unresolved_assumptions": resolution.unresolved_assumptions
            }
        }

        # Retrieve detailed records for evidence IDs
        included_records: List[Dict[str, Any]] = []
        for evidence_id in resolution.evidence[:max_records]:
            rec_entry = self.search_engine.get_by_record_id(evidence_id)
            if rec_entry:
                rec = rec_entry["record"]
                # Keep compact subset
                compact_rec = {
                    "id": evidence_id,
                    "name": rec.get("name"),
                    "interpretation": rec.get("interpretation"),
                    "validation_rules": rec.get("validation_rules", []),
                    "silent_gotchas": rec.get("silent_gotchas", []),
                    "sources": rec.get("source_references", [])
                }
                included_records.append(compact_rec)

        context_payload["knowledge_evidence"] = included_records

        # Budget enforcement
        serialized = json.dumps(context_payload, indent=2)
        char_limit = self.max_token_budget * self._approx_chars_per_token
        if len(serialized) > char_limit:
            # Trim evidence to fit budget
            while len(included_records) > 1 and len(serialized) > char_limit:
                included_records.pop()
                context_payload["knowledge_evidence"] = included_records
                serialized = json.dumps(context_payload, indent=2)

        return context_payload

    def format_as_prompt_section(self, context_payload: Dict[str, Any]) -> str:
        """Formats the context dict as an explicit Markdown section ready for LLM injection."""
        summary = context_payload.get("semantic_summary", {})
        evidence = context_payload.get("knowledge_evidence", [])
        rules = summary.get("preservation_rules", [])

        lines = [
            "### Universal Knowledge Repository Guidance",
            f"- **Semantic Concept:** {summary.get('concept')}",
            f"- **Compatibility Decision:** `{summary.get('compatibility')}` (Confidence: {summary.get('confidence')})",
        ]

        if summary.get("conversion_strategy"):
            lines.append(f"- **Recommended Strategy:** {summary.get('conversion_strategy')}")
        if summary.get("recommended_code"):
            lines.append(f"- **Reference Target Code:** `{summary.get('recommended_code')}`")

        if rules:
            lines.append("\n**Preservation Rules (Must Follow):**")
            for r in rules:
                lines.append(f"  * {r}")

        if summary.get("identified_risks"):
            lines.append("\n**Identified Behavioral Risks:**")
            for rk in summary.get("identified_risks"):
                lines.append(f"  ! {rk}")

        if evidence:
            lines.append("\n**Supporting Knowledge Records & Evidence:**")
            for ev in evidence:
                lines.append(f"  * [{ev.get('id')}] {ev.get('name')}")
                if ev.get("silent_gotchas"):
                    for g in ev.get("silent_gotchas"):
                        lines.append(f"    - Gotcha: {g}")

        return "\n".join(lines)
