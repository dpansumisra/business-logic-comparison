# Universal Data Format & Semantics Knowledge Repository: Developer & Operational Guide

## 1. Overview

The **Universal Data Format & Semantics Knowledge Repository** provides a decoupled, structured, machine-readable JSON knowledge infrastructure designed to empower cross-language code migration engines and semantic Root Cause Analysis (RCA).

It equips migration engines with deterministic knowledge regarding data types, internal representations, epochs, missing values, precision/rounding quirks, and behavioral divergence between source and target runtimes.

---

## 2. Repository Layout

```
MIGRATION_RCA/
├── registry/
│   ├── knowledge_registry.json     # Central registry index
│   ├── category_registry.json      # Knowledge taxonomy & tags
│   └── language_registry.json      # Supported language profiles & semantics
├── schemas/
│   ├── common.schema.json          # Shared ID, date, provenance definitions
│   ├── descriptor.schema.json      # Schema for *.meta.json files
│   ├── registry.schema.json        # Schema for central registry
│   ├── date.schema.json            # Schema for date/epoch/calendar knowledge
│   ├── numeric.schema.json         # Schema for numeric/integer/float knowledge
│   ├── null.schema.json            # Schema for null/missing value semantics
│   ├── string.schema.json          # Schema for string & encoding knowledge
│   ├── mapping.schema.json         # Schema for cross-language compatibility mappings
│   └── source.schema.json          # Schema for source catalog & jobs
├── knowledge/
│   ├── dates/                      # Formats, epochs, timezones, ambiguity
│   ├── numbers/                    # Types, overflow, division, rounding
│   ├── nulls/                      # Missing values, SQL NULL, 3-valued logic
│   ├── strings/                    # Unicode, UTF-8 vs UTF-16, normalization
│   ├── languages/                  # SAS, SQL, Python, JavaScript deep semantics
│   └── cross_language_mappings/    # Compatibility graph mappings & preservation rules
├── sources/
│   ├── source_catalog.json         # Authoritative sources catalog
│   └── source_snapshots/           # Rollback snapshots for atomic refresh
├── services/
│   ├── registry_service.py         # Registry discovery & indexing
│   ├── knowledge_loader.py         # Cached lazy JSON loader & stable ID index
│   ├── knowledge_search.py         # Multi-criteria search engine
│   ├── schema_validator.py         # JSON Schema validator & reference checker
│   ├── cross_language_mapper.py    # Semantic graph evaluator
│   ├── semantic_resolver.py        # Semantic equivalence & decision engine
│   ├── llm_context_builder.py      # Budget-conscious LLM prompt packager
│   ├── source_collector.py         # Source snapshotting & atomic management
│   └── migration_engine_adapter.py # Plug-and-play adapter for migration engines
├── api/
│   ├── routes/                     # REST routes for knowledge, search, semantics, admin
│   └── app.py                      # FastAPI application
├── scripts/
│   ├── validate_repository.py      # Full repository integrity verification
│   ├── rebuild_registry.py         # Automated registry indexer from descriptors
│   ├── check_source_updates.py     # Source freshness checker
│   └── collect_knowledge.py        # Snapshot and collection runner
└── tests/                          # 24 unit and integration tests across all modules
```

---

## 3. Specifications

### 3.1 Metadata Descriptors (`*.meta.json`)
Every dataset in `knowledge/` must have a corresponding `.meta.json` file adhering to `schemas/descriptor.schema.json`:
- `dataset_id`: Permanent uppercase ID (e.g., `DATE_FORMATS_001`).
- `name`: Human-readable title.
- `category` & `subcategory`: Categorical placement.
- `version`: Semantic version string.
- `status`: `"active"`, `"deprecated"`, or `"draft"`.
- `content_path`: Relative path to content JSON.
- `supported_languages`: Target languages.
- `concepts`: Semantic concept IDs covered.
- `when_to_retrieve` / `when_not_to_retrieve`: Explicit boundary instructions for query planners.
- `retrieval_guidance`: Critical caveats for LLM agents.
- `sources`: Source IDs cited from `sources/source_catalog.json`.

### 3.2 Cross-Language Semantic Compatibility Mappings (`mapping.schema.json`)
Mappings define pairwise transitions between language representations:
- `mapping_id`: e.g. `MAP_SAS_DATE_TO_PYTHON_DATE`.
- `concept_id`: e.g. `CONCEPT_CALENDAR_DATE`.
- `source` & `target`: Declarations of language, representation, epoch, and units.
- `compatibility`:
  - `EQUIVALENT`: Identical semantics across operations.
  - `CONDITIONALLY_EQUIVALENT`: Equivalent under verified preconditions.
  - `CONVERTIBLE`: Well-defined conversion function exists.
  - `INCOMPATIBLE`: Divergent semantics requiring redesign.
  - `UNCERTAIN`: Insufficient context to guarantee safety.
- `conversion`: Concrete code example and strategy description.
- `semantic_differences`: Specific hazards (e.g. integer addition differences, 3-valued logic).
- `preservation_rules`: Non-negotiable implementation mandates.

---

## 4. How to Extend the Repository

### 4.1 Adding a New Category
1. Add an entry to `registry/category_registry.json` under `"categories"`.
2. Create the directory `knowledge/<category_name>/`.
3. If new field structures are required, define `schemas/<category_name>.schema.json`.
4. Create datasets (`<dataset>.json`) and their descriptor (`<dataset>.meta.json`).
5. Run `python scripts/rebuild_registry.py` to index the dataset into `knowledge_registry.json`.
6. Run `python scripts/validate_repository.py` to verify structural integrity.

### 4.2 Adding a New Language
1. Add language metadata to `registry/language_registry.json` under `"languages"` (define epoch date, default numeric type, missing value syntax, dialects).
2. Create `knowledge/languages/<language_name>/` and author semantics files.
3. Author cross-language mappings in `knowledge/cross_language_mappings/`.
4. Register the files in `knowledge_registry.json`.
5. Run tests with `python -m pytest tests/`.

---

## 5. REST API Reference

Start the API with:
```bash
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/knowledge/categories` | List all categories |
| `GET` | `/api/v1/knowledge/datasets` | List registered datasets |
| `GET` | `/api/v1/knowledge/records/{id}` | Direct record retrieval by stable ID |
| `POST` | `/api/v1/knowledge/search` | Multi-criteria search |
| `POST` | `/api/v1/mappings/find` | Find cross-language compatibility mappings |
| `POST` | `/api/v1/semantics/resolve` | Analyze source vs target operations |
| `GET` | `/api/v1/sources` | List authoritative standards & documentation |
| `POST` | `/api/v1/admin/validate` | Full repository schema and integrity check |
| `POST` | `/api/v1/admin/snapshot` | Create rollback snapshot |

---

## 6. Migration Engine Integration

Migration engines can consume the repository via Python directly:

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

if result["needs_correction"]:
    print(f"Warning: {result['correction_reason']}")
    print(f"Recommended target code: {result['recommended_target_code']}")
    # Inject result["llm_prompt_guidance"] into LLM prompt
```
