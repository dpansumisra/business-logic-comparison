import io
from fastapi.testclient import TestClient
from api.app import app
from services.code_comparator import CodeComparator

client = TestClient(app)


def test_code_comparator_direct():
    comparator = CodeComparator()

    sas_code = """
    data result;
        set input_table;
        new_date = old_date + 30;
        if score < 0 then flag = 'ERR';
    run;
    """

    py_code = """
    # Naively migrated python code
    new_date = old_date + 30
    if score < 0:
        flag = 'ERR'
    """

    report = comparator.compare_files(
        source_code=sas_code,
        target_code=py_code,
        source_filename="process.sas",
        target_filename="process.py"
    )

    assert report["source_language"] == "SAS"
    assert report["target_language"] == "Python"
    assert report["total_operations_detected"] >= 2
    assert report["corrections_needed_count"] >= 1
    assert any("timedelta" in f.get("correction_reason", "") for f in report["findings"])


def test_compare_endpoint_upload():
    sas_bytes = b"new_date = old_date + 30;"
    py_bytes = b"new_date = old_date + 30"

    files = {
        "source_file": ("test.sas", io.BytesIO(sas_bytes), "text/plain"),
        "target_file": ("test.py", io.BytesIO(py_bytes), "text/plain"),
    }

    response = client.post("/api/v1/compare/files", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["source_language"] == "SAS"
    assert data["target_language"] == "Python"
    assert data["corrections_needed_count"] >= 1


def test_compare_ui_get():
    response = client.get("/compare")
    assert response.status_code == 200
    assert "Cross-Language Semantic Code Comparator" in response.text
