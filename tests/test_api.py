from fastapi.testclient import TestClient
from api.app import app

client = TestClient(app)


def test_api_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "online"


def test_api_categories():
    response = client.get("/api/v1/knowledge/categories")
    assert response.status_code == 200
    cats = response.json()
    assert any(c["category_id"] == "DATES" for c in cats)


def test_api_datasets():
    response = client.get("/api/v1/knowledge/datasets?category=dates")
    assert response.status_code == 200
    datasets = response.json()
    assert any(d["dataset_id"] == "DATE_FORMATS_001" for d in datasets)


def test_api_get_record():
    response = client.get("/api/v1/knowledge/records/FMT_DDMMYY")
    assert response.status_code == 200
    data = response.json()
    assert data["record"]["record_id"] == "FMT_DDMMYY"


def test_api_search():
    payload = {
        "query": "ISO 8601",
        "category": "dates",
        "limit": 5
    }
    response = client.post("/api/v1/knowledge/search", json=payload)
    assert response.status_code == 200
    res = response.json()
    assert res["total"] > 0


def test_api_semantics_resolve():
    payload = {
        "source_language": "SAS",
        "target_language": "Python",
        "operation": "DATE_CONVERSION",
        "source_representation": "numeric_date",
        "target_representation": "datetime.date"
    }
    response = client.post("/api/v1/semantics/resolve", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["compatibility"] == "CONVERTIBLE"
    assert data["recommended_mapping"] == "MAP_SAS_DATE_TO_PYTHON_DATE"
    assert "llm_prompt_guidance" in data


def test_api_admin_validate():
    response = client.post("/api/v1/admin/validate")
    assert response.status_code == 200
    report = response.json()
    assert report["is_valid"] is True
    assert report["error_count"] == 0
