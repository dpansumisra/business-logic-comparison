"""Mapping routes for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services.registry_service import RegistryService
from services.cross_language_mapper import CrossLanguageMapper

router = APIRouter(prefix="/api/v1/mappings", tags=["Mappings"])
registry_service = RegistryService()
mapper = CrossLanguageMapper(registry_service)


class MappingQueryRequest(BaseModel):
    source_language: str
    target_language: str
    concept_id: Optional[str] = None
    source_representation: Optional[str] = None
    target_representation: Optional[str] = None


@router.post("/find")
def find_mapping(req: MappingQueryRequest) -> Dict[str, Any]:
    """Finds cross-language mappings between source and target representations."""
    mapping = mapper.find_mapping(
        source_language=req.source_language,
        target_language=req.target_language,
        concept_id=req.concept_id,
        source_representation=req.source_representation,
        target_representation=req.target_representation,
    )
    if not mapping:
        raise HTTPException(
            status_code=404,
            detail=f"No mapping found for {req.source_language} -> {req.target_language}."
        )
    return mapping


@router.get("")
def list_mappings() -> List[Dict[str, Any]]:
    """Returns all registered cross-language mappings."""
    return mapper.get_all_mappings()
