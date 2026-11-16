# Universal Data Format & Semantics Knowledge Repository

[![Repository Validation](https://img.shields.io/badge/Validation-Passing-brightgreen.svg)]()
[![Tests](https://img.shields.io/badge/Tests-24%20Passed-success.svg)]()
[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)]()
[![FastAPI](https://img.shields.io/badge/FastAPI-1.0.0-teal.svg)]()

A standalone, JSON-first semantic knowledge infrastructure built to assist cross-language code migration engines, semantic analysis tools, and Root Cause Analysis (RCA) systems.

---

## 🎯 Purpose & Core Capabilities

When migrating code between disparate languages and database engines (e.g., SAS to Python, SQL to Python, JavaScript to C++), apparent syntactic equivalence often hides catastrophic semantic divergence.

This repository provides deterministic, machine-readable knowledge and semantic compatibility graphs:
- **Date & Time Epochs:** Distinguishes SAS numeric dates (days since 1960-01-01) from SAS datetimes (seconds since 1960-01-01) and Unix epochs (1970-01-01).
- **Ambiguous Formats:** Detects two-digit year century cutoff discrepancies (SAS `YEARCUTOFF` vs Python `%y`) and locale month/day permutations (`01/02/2024`).
- **Null & Missing Values:** Dissects SQL three-valued logic (`UNKNOWN`), SAS numeric missings (`.` and `.A-.Z` sorting smaller than negative numbers), and Python `None`.
- **Numeric Semantics:** Identifies integer division truncation towards zero vs flooring towards negative infinity, IEEE-754 precision loss, and Banker's rounding vs round-half-up.
- **Cross-Language Semantic Compatibility Graph:** Formally defines mappings, preconditions, edge cases, risks, and behavior-preserving target transformations.

---

## 🚀 Quick Start

### 1. File Upload & Semantic Comparison
You can compare two code files (source & target) in three ways:

* **Interactive Web UI:**
  Start the server and open **[http://localhost:8000/compare](http://localhost:8000/compare)** in your browser to drag-and-drop 2 files.
  ```bash
  python -m uvicorn api.app:app --reload --port 8000
  ```
* **Command Line (CLI):**
  ```bash
  python scripts/compare_files.py --source path/to/source.sas --target path/to/target.py
  ```
* **REST API Endpoint:**
  ```bash
  curl -X POST "http://localhost:8000/api/v1/compare/files" \
    -F "source_file=@path/to/source.sas" \
    -F "target_file=@path/to/target.py"
  ```

### 2. Run Repository Validation
Ensure all JSON files, descriptors, schemas, and stable ID references are intact:
```bash
python scripts/validate_repository.py
```

### 2. Run Test Suite
Run the full test suite (covering schemas, retrieval, semantic resolution, adapter, and API):
```bash
python -m pytest -v tests
```

### 3. Launch REST API
Start the FastAPI service:
```bash
uvicorn api.app:app --host 0.0.0.0 --port 8000
```
Explore the interactive OpenAPI documentation at: `http://localhost:8000/docs`

---

## 🔍 Semantic Resolution Example

Querying semantic compatibility when migrating SAS date arithmetic:

```python
from services.migration_engine_adapter import MigrationEngineAdapter

adapter = MigrationEngineAdapter()

result = adapter.analyze_semantic_operation(
    source_language="SAS",
    target_language="Python",
    operation="DATE_CONVERSION",
    source_expression="new_date = old_date + 30",
    target_expression="new_date = old_date + 30",
    source_type="numeric_date",
    target_type="datetime.date"
)

print(result["semantic_decision"]["compatibility"])  # "CONVERTIBLE"
print(result["needs_correction"])                   # True
print(result["recommended_target_code"])           # "datetime.date(1960, 1, 1) + datetime.timedelta(days=int(sas_date))"
```

---

## 📚 Documentation
- [Architecture Assessment](docs/architecture_assessment.md)
- [Developer & Operational Guide](docs/developer_guide.md)
- [Original Implementation Plan](Implementation_plan.md)
