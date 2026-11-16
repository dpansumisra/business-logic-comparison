"""Admin routes for repository validation, snapshots, and health monitoring."""

from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter
from services.schema_validator import RepositoryValidator
from services.source_collector import SourceCollector

router = APIRouter(prefix="/api/v1/admin", tags=["Admin"])
validator = RepositoryValidator()
collector = SourceCollector()


@router.post("/validate")
def validate_repository() -> Dict[str, Any]:
    """Runs full structural and semantic validation over all datasets, descriptors, and schemas."""
    report = validator.validate_repository()
    return report.to_dict()


@router.post("/snapshot")
def create_snapshot(label: str = "manual_backup") -> Dict[str, Any]:
    """Creates a timestamped snapshot of the repository for rollback safety."""
    path = collector.create_snapshot(label=label)
    return {
        "status": "success",
        "snapshot_path": str(path)
    }
