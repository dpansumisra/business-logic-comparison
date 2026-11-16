"""Search routes for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter
from pydantic import BaseModel, Field
from services.registry_service import RegistryService
from services.knowledge_search import KnowledgeSearchEngine

router = APIRouter(prefix="/api/v1/knowledge", tags=["Search"])
registry_service = RegistryService()
search_engine = KnowledgeSearchEngine(registry_service)


class SearchRequest(BaseModel):
    query: Optional[str] = Field(None, description="Free text or record ID")
    category: Optional[str] = Field(None, description="Category filter (e.g. dates, numbers, nulls)")
    source_language: Optional[str] = Field(None, description="Source programming language")
    target_language: Optional[str] = Field(None, description="Target programming language")
    concepts: Optional[List[str]] = Field(None, description="List of semantic concept IDs")
    limit: int = Field(10, ge=1, le=100)


@router.post("/search")
def search_knowledge(req: SearchRequest) -> Dict[str, Any]:
    """Multi-criteria search over knowledge repository."""
    return search_engine.query(
        query=req.query,
        category=req.category,
        source_language=req.source_language,
        target_language=req.target_language,
        concepts=req.concepts,
        limit=req.limit,
    )
