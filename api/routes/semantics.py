"""Semantics resolution routes for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from typing import Any, Dict, Optional
from fastapi import APIRouter
from pydantic import BaseModel, Field
from services.registry_service import RegistryService
from services.cross_language_mapper import CrossLanguageMapper
from services.knowledge_search import KnowledgeSearchEngine
from services.semantic_resolver import SemanticResolver
from services.llm_context_builder import LLMContextBuilder

router = APIRouter(prefix="/api/v1/semantics", tags=["Semantics"])
registry_service = RegistryService()
search_engine = KnowledgeSearchEngine(registry_service)
mapper = CrossLanguageMapper(registry_service)
resolver = SemanticResolver(mapper, search_engine)
context_builder = LLMContextBuilder(search_engine)


class SemanticResolveRequest(BaseModel):
    source_language: str
    target_language: str
    operation: Optional[str] = Field(None, description="e.g. DATE_CONVERSION, NULL_CHECK, INT_DIV")
    source_representation: Optional[str] = None
    target_representation: Optional[str] = None
    known_context: Optional[Dict[str, Any]] = None


@router.post("/resolve")
def resolve_semantics(req: SemanticResolveRequest) -> Dict[str, Any]:
    """Compares source vs target semantics and outputs compatibility classification and preservation advice."""
    res = resolver.resolve(
        source_language=req.source_language,
        target_language=req.target_language,
        operation=req.operation,
        source_representation=req.source_representation,
        target_representation=req.target_representation,
        known_context=req.known_context,
    )
    llm_context = context_builder.build_context(res)
    prompt_snippet = context_builder.format_as_prompt_section(llm_context)

    result_dict = res.to_dict()
    result_dict["llm_prompt_guidance"] = prompt_snippet
    return result_dict
