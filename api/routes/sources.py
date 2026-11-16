"""Sources routes for Universal Data Format & Semantics Knowledge Repository."""

from __future__ import annotations

from typing import Any, Dict, List
from fastapi import APIRouter
from services.source_collector import SourceCollector

router = APIRouter(prefix="/api/v1/sources", tags=["Sources"])
collector = SourceCollector()


@router.get("")
def list_sources() -> List[Dict[str, Any]]:
    """Lists all registered external sources and standards."""
    catalog = collector.load_catalog()
    return catalog.get("sources", [])


@router.get("/status")
def source_status() -> List[Dict[str, Any]]:
    """Returns provenance and health status of all sources."""
    return collector.check_source_status()
