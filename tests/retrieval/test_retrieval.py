from pathlib import Path
from services.registry_service import RegistryService
from services.knowledge_search import KnowledgeSearchEngine

root_dir = Path(__file__).resolve().parent.parent.parent


def test_search_by_stable_id():
    service = RegistryService(base_dir=root_dir)
    search_engine = KnowledgeSearchEngine(service)

    result = search_engine.get_by_record_id("FMT_DDMMYY")
    assert result is not None
    assert result["record"]["name"] == "Day Month Year Two-Digit"
    assert result["category"] == "dates"


def test_search_by_concept():
    service = RegistryService(base_dir=root_dir)
    search_engine = KnowledgeSearchEngine(service)

    matches = search_engine.search_by_concept("CONCEPT_CALENDAR_DATE")
    assert len(matches) > 0
    record_ids = [m["record"].get("record_id") or m["record"].get("mapping_id") for m in matches]
    assert "FMT_ISO_8601_DATE" in record_ids
    assert "EPOCH_SAS_DATE" in record_ids


def test_search_by_language_pair():
    service = RegistryService(base_dir=root_dir)
    search_engine = KnowledgeSearchEngine(service)

    matches = search_engine.search_by_language_pair("SAS", "Python")
    assert len(matches) >= 3
    mapping_ids = [m["mapping_id"] for m in matches]
    assert "MAP_SAS_DATE_TO_PYTHON_DATE" in mapping_ids
    assert "MAP_SAS_DATETIME_TO_PYTHON_DATETIME" in mapping_ids


def test_unified_query():
    service = RegistryService(base_dir=root_dir)
    search_engine = KnowledgeSearchEngine(service)

    # Search with keywords
    res = search_engine.query(query="bankers rounding half even", category="numbers")
    assert res["total"] > 0
    assert any("RND_HALF_EVEN" in r["knowledge_id"] for r in res["results"])
