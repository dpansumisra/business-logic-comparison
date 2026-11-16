from pathlib import Path
from services.registry_service import RegistryService

root_dir = Path(__file__).resolve().parent.parent.parent


def test_registry_loading():
    service = RegistryService(base_dir=root_dir)
    info = service.get_registry_info()
    assert info["registry_version"] == "1.0.0"
    assert info["category_count"] > 0
    assert info["dataset_count"] >= 16


def test_list_categories():
    service = RegistryService(base_dir=root_dir)
    categories = service.list_categories()
    cat_ids = [c["category_id"] for c in categories]
    assert "DATES" in cat_ids
    assert "NUMBERS" in cat_ids
    assert "NULLS" in cat_ids
    assert "CROSS_LANGUAGE_MAPPINGS" in cat_ids


def test_list_languages():
    service = RegistryService(base_dir=root_dir)
    languages = service.list_languages()
    lang_ids = [l["language_id"] for l in languages]
    assert "LANG_SAS" in lang_ids
    assert "LANG_SQL" in lang_ids
    assert "LANG_PYTHON" in lang_ids


def test_get_dataset_metadata_and_records():
    service = RegistryService(base_dir=root_dir)
    meta = service.get_dataset_metadata("DATE_FORMATS_001")
    assert meta is not None
    assert meta["dataset_id"] == "DATE_FORMATS_001"
    assert "ddmmyy" in meta["supported_formats"]

    records = service.get_dataset_records("DATE_FORMATS_001")
    assert len(records) > 0
    assert any(r.get("record_id") == "FMT_ISO_8601_DATE" for r in records)
