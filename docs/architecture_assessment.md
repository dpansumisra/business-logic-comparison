# Architecture Assessment: Universal Data Format & Semantics Knowledge Repository

## 1. Executive Summary

This architecture assessment establishes the technical foundation for the **Universal Data Format & Semantics Knowledge Repository** within `MIGRATION_RCA`. The goal is to provide a decoupled, high-fidelity, machine-readable JSON knowledge infrastructure that powers cross-language code migration engines (e.g., SAS to Python, SQL to Python, JavaScript to C++, etc.).

## 2. Workspace & Environment Inspection

- **Workspace Path:** `C:\Users\Lenovo\Downloads\MIGRATION_RCA`
- **Environment:** Windows 10/11, Python 3.12.0
- **Pre-installed Dependencies:** `jsonschema` (4.26.0), `fastapi` (0.162.0), `pydantic` (2.13.4), `pytest` (9.1.1), `uvicorn` (0.51.0), `httpx` (0.28.1), `requests` (2.34.2), `PyYAML` (6.0.3).
- **Existing Codebase State:** The workspace is a dedicated repository initialized with `Implementation_plan.md`. No legacy conflicting code exists in this repository.

## 3. Components to Be Introduced

### 3.1 Registries & Descriptors (`registry/`)
- **Central Knowledge Registry (`knowledge_registry.json`)**: Lightweight catalog index linking datasets, metadata descriptors, and content paths.
- **Category Registry (`category_registry.json`)**: Taxonomy of supported categories (dates, numbers, nulls, strings, identity, financial, booleans, structured data, language semantics, cross-language mappings).
- **Language Registry (`language_registry.json`)**: Supported source and target languages/dialects (SAS, SQL dialects, Python, JavaScript, Java, C++, C#).

### 3.2 Formal Schemas (`schemas/`)
- `common.schema.json`: Base structures, provenance objects, license rules.
- `descriptor.schema.json`: Schema for `.meta.json` files.
- `registry.schema.json`: Schema for `knowledge_registry.json`.
- `date.schema.json`: Schema for date/time/calendar/epoch definitions.
- `numeric.schema.json`: Schema for integers, floats, fixed-point, overflow, and division rules.
- `null.schema.json`: Schema for null, missing, empty, and sentinel representations.
- `string.schema.json`: Schema for encodings, normalization, and collation.
- `mapping.schema.json`: Schema for cross-language semantic compatibility mappings.
- `source.schema.json`: Schema for external source registry and collection jobs.

### 3.3 Core Knowledge Repositories (`knowledge/`)
- **Dates & Timestamps:** Formats (`ddMMyy`, `yyyyMMdd`, ISO-8601), epochs (SAS 1960-01-01, Unix 1970-01-01, Excel 1899-12-30 / 1900-01-01), timezone semantics, and two-digit year ambiguity.
- **Numeric Semantics:** IEEE-754 floats, arbitrary precision decimals, integer overflow, integer division truncation vs float division.
- **Null & Missing Values:** SQL `NULL`, Python `None`, JS `null`/`undefined`, SAS special missing values (`.` and `.A` through `.Z`), comparison and truthiness semantics.
- **Language Semantics:** Deep language profiles for SAS, SQL, Python, JavaScript, C++, Java, C#.
- **Cross-Language Semantic Compatibility Graph:** Formal mappings connecting concepts, source representations, target representations, conversion formulas, semantic differences, edge cases, and preservation rules.

### 3.4 Service Layer (`services/`)
- `registry_service.py`: Querying and indexing the central registry.
- `knowledge_loader.py`: Cached, lazy loading of JSON datasets.
- `knowledge_search.py`: Multi-criteria querying (by concept, ID, tags, languages).
- `schema_validator.py`: Comprehensive JSON Schema validation and cross-reference integrity checks.
- `source_collector.py`: Acquisition, provenance tracking, snapshotting, and atomic refresh pipeline.
- `cross_language_mapper.py`: Compatibility graph navigation and candidate mapping evaluation.
- `semantic_resolver.py`: Core decision engine classifying compatibility (`EQUIVALENT`, `CONDITIONALLY_EQUIVALENT`, `CONVERTIBLE`, `INCOMPATIBLE`, `UNCERTAIN`) with concrete evidence and preservation requirements.
- `llm_context_builder.py`: Token-budgeted, structured context preparation for LLM-based migration agents.
- `migration_engine_adapter.py`: Zero-coupling adapter providing clean programmatic hooks for migration pipelines.

### 3.5 API Layer (`api/`)
- FastAPI application with REST endpoints:
  - `/api/v1/knowledge/categories`
  - `/api/v1/knowledge/datasets`
  - `/api/v1/knowledge/records/{record_id}`
  - `/api/v1/knowledge/search`
  - `/api/v1/mappings/find`
  - `/api/v1/semantics/resolve`
  - `/api/v1/sources`
  - `/api/v1/admin/validate`
  - `/api/v1/admin/refresh`

### 3.6 Evaluation & Testing (`tests/`)
- Unit and integration tests covering all schemas, registry operations, search mechanisms, semantic resolution logic, and migration evaluation scenarios.

## 4. Proposed Integration Points with Existing Migration Engines

The knowledge repository integrates at the **Intermediate Representation (IR) & Semantic Analysis Stage**:
1. Migration Engine parses source AST/IR and target AST/IR.
2. Detected operations (e.g. `DATE_ADD`, `NULL_CHECK`, `INT_DIV`) are submitted to `SemanticResolver.resolve()` or the `/api/v1/semantics/resolve` endpoint.
3. The resolver inspects source/target representations and yields:
   - Compatibility classification (`EQUIVALENT`, `CONDITIONALLY_EQUIVALENT`, `CONVERTIBLE`, `INCOMPATIBLE`, `UNCERTAIN`).
   - Semantic differences & identified risks.
   - Recommended code transformation strategy and code snippet.
   - Evidence sources and knowledge record citations.
4. Migration Engine uses this structured advice to perform behavior-preserving target code synthesis and root-cause analysis (RCA).

## 5. Potential Compatibility Risks & Mitigation Strategies

| Risk | Mitigation Strategy |
| :--- | :--- |
| **Silent Equivalence Assumption** | Explicit default to `UNCERTAIN` when types or locale context are unspecified. |
| **Schema Drift & Broken Cross-References** | Strict automated CI tests with `SchemaValidator` verifying all stable IDs. |
| **LLM Context Window Overflow** | `LLMContextBuilder` enforces strict token/character budgets and relevance scoring. |
| **Corrupted Knowledge Updates** | Atomic file writes with staging and rollback snapshot retention. |
| **External Dependency Lock-in** | JSON-first architecture with pure Python standard library + standard open libraries (`jsonschema`, `pydantic`). |

## 6. Implementation Roadmap

- **Phase 1:** Schemas, Registries, and Foundation Services.
- **Phase 2:** P0 Knowledge Datasets (Dates, Numbers, Nulls, Cross-Language Mappings).
- **Phase 3:** Language Semantics Datasets (SAS, SQL, Python, JavaScript) & Sources Catalog.
- **Phase 4:** Resolution Engine, Search, LLM Context Builder, and API Routes.
- **Phase 5:** Automated Validation Scripts, Migration RCA Test Scenarios, and Verification.
