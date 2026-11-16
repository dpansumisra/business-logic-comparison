"""Knowledge routes for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from services.registry_service import RegistryService

router = APIRouter(prefix="/api/v1/knowledge", tags=["Knowledge"])
registry_service = RegistryService()


@router.get("/categories")
def list_categories() -> List[Dict[str, Any]]:
    """Lists all knowledge categories."""
    return registry_service.list_categories()


@router.get("/datasets")
def list_datasets(category: Optional[str] = Query(None)) -> List[Dict[str, Any]]:
    """Lists registered datasets, optionally filtered by category."""
    return registry_service.list_datasets(category=category)


@router.get("/datasets/{dataset_id}")
def get_dataset(dataset_id: str) -> Dict[str, Any]:
    """Retrieves metadata descriptor and record overview for a dataset."""
    meta = registry_service.get_dataset_metadata(dataset_id)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    records = registry_service.get_dataset_records(dataset_id)
    return {
        "metadata": meta,
        "records": records
    }


@router.get("/records/{record_id}")
def get_record(record_id: str) -> Dict[str, Any]:
    """Retrieves a specific knowledge record by stable ID."""
    entry = registry_service.loader.get_record_by_id(record_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Record {record_id} not found")
    return entry
