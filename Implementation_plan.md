# **Detailed implementation plan: Universal Data Format & Semantics Knowledge Repository**

## **1. Project objective**

Build a reusable, JSON-based knowledge repository that helps the existing **cross-language code migration engine** understand how different programming languages, databases, and systems represent and process data.

The repository will provide structured knowledge about:

- Data formats and representations.
- Data types and their behavior.
- Language-specific semantics.
- Conversion rules between languages.
- Edge cases and ambiguities.
- Standards, official documentation, and their versions.
- Cross-language compatibility and incompatibility rules.

The existing migration engine will use this repository when analyzing source and target code. It will help determine whether two operations are semantically equivalent and, when they are not, how to correct the target implementation while preserving the source behavior.

**Example:** If the source uses a SAS date value and the target uses Python `datetime`, the migration engine should understand the underlying date representation, epoch, conversion formula, missing-value behavior, and formatting rules before recommending a correction.

This is a knowledge infrastructure component, not a standalone test-data generation system.

## **2. High-level architecture**

**Existing Code Migration Engine**

Source code + target code → semantic analysis

**Knowledge Retrieval Interface**

Concept detection · Query planning · Relevant knowledge retrieval

**JSON Knowledge Repository**

Dates, numbers, strings, nulls, identities, structured data, etc.

**Semantic Compatibility Graph**

Mappings, conversions, differences, rules and edge cases

**Semantic Resolution**

Source meaning vs target meaning → compatibility decision and evidence

**Migration Engine**

Use the result to identify differences and generate a behavior-preserving correction

The repository should remain independently usable. It should not depend on a particular LLM, migration framework, or programming language.

## **3. Recommended project structure**

Use JSON as the primary storage format for all knowledge, metadata, registries, source records, and mappings.

```
universal-data-knowledge/
│
├── registry/
│   ├── knowledge_registry.json
│   ├── knowledge_schema.json
│   ├── descriptor_schema.json
│   ├── source_schema.json
│   ├── category_registry.json
│   └── language_registry.json
│
├── knowledge/
│   ├── dates/
│   │   ├── date_formats.json
│   │   ├── date_formats.meta.json
│   │   ├── datetime_formats.json
│   │   ├── timestamp_formats.json
│   │   ├── timezone_semantics.json
│   │   ├── calendars.json
│   │   ├── epoch_serial_dates.json
│   │   └── ambiguous_dates.json
│   │
│   ├── numbers/
│   │   ├── numeric_types.json
│   │   ├── numeric_formats.json
│   │   ├── precision_rounding.json
│   │   ├── overflow_semantics.json
│   │   └── numeric_conversions.json
│   │
│   ├── nulls/
│   │   ├── null_missing_empty.json
│   │   └── language_null_semantics.json
│   │
│   ├── strings/
│   │   ├── encoding_semantics.json
│   │   ├── unicode_normalization.json
│   │   ├── collation_semantics.json
│   │   └── case_sensitivity.json
│   │
│   ├── identity/
│   │   ├── email_formats.json
│   │   ├── phone_formats.json
│   │   ├── postal_codes.json
│   │   ├── country_codes.json
│   │   └── identifier_formats.json
│   │
│   ├── financial/
│   │   ├── currency_codes.json
│   │   ├── currency_precision.json
│   │   ├── payment_card_reference.json
│   │   └── financial_rounding.json
│   │
│   ├── booleans/
│   │   ├── boolean_representations.json
│   │   └── truthiness_semantics.json
│   │
│   ├── structured_data/
│   │   ├── json_semantics.json
│   │   ├── xml_semantics.json
│   │   ├── csv_semantics.json
│   │   └── yaml_semantics.json
│   │
│   ├── languages/
│   │   ├── sas/
│   │   ├── sql/
│   │   ├── python/
│   │   ├── javascript/
│   │   ├── cpp/
│   │   ├── java/
│   │   └── csharp/
│   │
│   └── cross_language_mappings/
│       ├── date_mappings.json
│       ├── numeric_mappings.json
│       ├── null_mappings.json
│       └── string_mappings.json
│
├── sources/
│   ├── source_catalog.json
│   ├── collection_jobs.json
│   └── source_snapshots/
│
├── schemas/
│   ├── common.schema.json
│   ├── date.schema.json
│   ├── numeric.schema.json
│   ├── null.schema.json
│   ├── mapping.schema.json
│   └── semantic_rule.schema.json
│
├── services/
│   ├── registry_service.py
│   ├── knowledge_loader.py
│   ├── knowledge_search.py
│   ├── source_collector.py
│   ├── schema_validator.py
│   ├── semantic_resolver.py
│   ├── cross_language_mapper.py
│   └── llm_context_builder.py
│
├── api/
│   ├── routes/
│   │   ├── knowledge.py
│   │   ├── search.py
│   │   ├── mappings.py
│   │   └── sources.py
│   └── app.py
│
├── scripts/
│   ├── collect_knowledge.py
│   ├── validate_repository.py
│   ├── rebuild_registry.py
│   └── check_source_updates.py
│
├── tests/
│   ├── registry/
│   ├── schemas/
│   ├── retrieval/
│   ├── semantic_resolution/
│   └── mappings/
│
├── config/
│   └── settings.json
│
└── README.md
```

This is the proposed logical structure. The coding agent should first inspect the existing project and adapt the structure to its conventions rather than creating a disconnected application.

## **4. Data model and JSON standards**

Before collecting large amounts of knowledge, establish a consistent JSON structure. Without this, data collected from different sources will become difficult to search, validate, compare, and use in LLM context.

### **4.1 Central knowledge registry**

File: `registry/knowledge_registry.json`

This is the entry point for the entire repository. It tells the application what knowledge exists and where it is stored.

Example:

```
{
  "registry_version": "1.0.0",
  "last_updated": "2026-11-16",
  "categories": [
    {
      "category_id": "DATES",
      "name": "Date and Time",
      "descriptor_path": "knowledge/dates/date_formats.meta.json",
      "content_path": "knowledge/dates/date_formats.json",
      "tags": ["date", "datetime", "timestamp", "calendar"]
    },
    {
      "category_id": "NUMBERS",
      "name": "Numeric Representations",
      "descriptor_path": "knowledge/numbers/numeric_types.meta.json",
      "content_path": "knowledge/numbers/numeric_types.json",
      "tags": ["integer", "decimal", "float", "precision"]
    }
  ]
}
```

The registry must support:

- Stable IDs for every knowledge item.
- Category and subcategory lookup.
- Descriptor and content file locations.
- Aliases and search tags.
- Version and active/deprecated status.
- Language and format filters.

Do not make the registry contain all the actual knowledge. It should remain a lightweight index.

### **4.2 Metadata descriptor for each dataset**

Every knowledge dataset must have a separate `.meta.json` descriptor.

Example: `knowledge/dates/date_formats.meta.json`

```
{
  "dataset_id": "DATE_FORMATS_001",
  "name": "Universal Date Format Reference",
  "description": "Definitions of date patterns, their component order, interpretation rules, locale dependencies and conversion considerations.",
  "category": "dates",
  "subcategory": "date_formats",
  "version": "1.0.0",
  "status": "active",
  "content_path": "knowledge/dates/date_formats.json",
  "record_count": 0,
  "supported_languages": [
    "SAS",
    "SQL",
    "Python",
    "JavaScript",
    "Java",
    "C++",
    "C#"
  ],
  "supported_formats": [
    "ddmmyy",
    "mmddyyyy",
    "yyyyMMdd",
    "ISO-8601"
  ],
  "tags": [
    "date",
    "date-format",
    "date-parsing",
    "date-conversion"
  ],
  "concepts": [
    "DATE_PARSE",
    "DATE_FORMAT",
    "DATE_CONVERSION",
    "DATE_COMPARISON"
  ],
  "when_to_retrieve": [
    "When source or target code parses a date",
    "When two date representations need comparison",
    "When migrating date-related logic between languages"
  ],
  "when_not_to_retrieve": [
    "When code only performs unrelated string operations"
  ],
  "retrieval_guidance": "Retrieve the specific format and language semantics required by the migration operation. Do not assume ambiguous date formats without locale or source context.",
  "source_type": "combined",
  "sources": [
    "SRC_UNICODE_CLDR"
  ],
  "date_created": "2026-11-16",
  "date_update": "2026-11-16",
  "date_current": null,
  "license": "See individual source records",
  "schema_ref": "schemas/date.schema.json"
}
```

**Important distinction between date fields:**

- `date_created`: When the dataset was initially created.
- `date_update`: When its content or metadata was last updated.
- `date_current`: A dynamic runtime date only when a dataset genuinely requires a current-date reference. Do not store today's date here as if it were a permanent property of the dataset.

### **4.3 Knowledge content format**

File: `knowledge/dates/date_formats.json`

Each record should describe a concept, not merely provide an example string.

```
{
  "dataset_id": "DATE_FORMATS_001",
  "version": "1.0.0",
  "records": [
    {
      "format_id": "FMT_DDMMYY",
      "name": "Day Month Year",
      "pattern": "ddMMyy",
      "components": [
        {
          "component": "day",
          "position": 1,
          "length": 2
        },
        {
          "component": "month",
          "position": 3,
          "length": 2
        },
        {
          "component": "year",
          "position": 5,
          "length": 2
        }
      ],
      "example": "290224",
      "interpretation": {
        "calendar": "gregorian",
        "locale": "en-GB",
        "year_digits": 2,
        "century_policy": "requires_explicit_pivot"
      },
      "ambiguity": {
        "is_ambiguous": true,
        "reason": "Two-digit years require a century interpretation policy."
      },
      "canonical_representation": "2024-02-29",
      "validation_rules": [
        "Day must be valid for the selected month and year.",
        "Month must be between 1 and 12.",
        "Century interpretation must be explicitly determined."
      ],
      "related_concepts": [
        "FMT_MMDDYY",
        "FMT_YYYYMMDD"
      ]
    }
  ]
}
```

The schema should allow different record shapes for different knowledge categories. A timezone record, decimal type, email syntax, and SAS missing-value definition do not have the same attributes. Use shared common fields with category-specific schemas rather than forcing everything into one generic structure.

### **4.4 Stable IDs and relationships**

Assign permanent IDs such as:

- `FMT_DDMMYY`
- `TYPE_SAS_NUMERIC`
- `NULL_SQL`
- `MAP_SAS_DATE_TO_PYTHON_DATE`
- `SEM_CPP_INTEGER_DIVISION`

Use IDs to connect records across files. Do not depend on display names or filenames as identifiers, because these can change.

## **5. Knowledge collection: what to build**

Implement the knowledge repository in the following order. The first categories are the most relevant to code migration because they commonly cause silent behavior changes.

| Priority | Knowledge area          | Required coverage                                     |
| -------- | ----------------------- | ----------------------------------------------------- |
| P0       | Dates and timestamps    | Patterns, calendars, epochs, timezones, ambiguity     |
| P0       | Numeric semantics       | Types, precision, rounding, overflow, division        |
| P0       | Null and missing values | NULL, None, undefined, SAS missing, empty values      |
| P0       | Cross-language mappings | Source-to-target conversions and compatibility        |
| P1       | Strings and encoding    | Unicode, encoding, casing, collation, indexing        |
| P1       | Structured data         | JSON, CSV, XML, YAML behavior                         |
| P1       | Boolean and enum        | True/false, 1/0, Y/N, truthiness                      |
| P1       | Language semantics      | SAS, SQL, Python, JS, C++, Java, C#                   |
| P2       | Identity formats        | Email, phone, postal code, country codes              |
| P2       | Financial formats       | Currency, minor units, card-number reference formats  |
| P2       | Domain-specific formats | Geography, measurements, scientific and business data |

### **5.1 Dates and timestamps**

This should be the first fully implemented category.

Collect and represent:

- `ddmmyy`, `mmddyy`, `mmddyyyy`, `ddmmyyyy`.
- `yyyyMMdd`, `yyMMdd`, `yyyy-MM-dd`.
- ISO 8601 and RFC 3339 date-time forms.
- Month names and locale-dependent formats.
- Time-only values, datetime values, timestamps and durations.
- UTC offsets, named timezones and daylight-saving transitions.
- Epoch seconds, milliseconds, microseconds and nanoseconds.
- Excel serial dates and SAS date/datetime values.
- Julian dates, week dates, quarter and partial-date representations.
- Two-digit year pivot rules and ambiguous numeric date strings.

For every pattern, record its components, component order, precision, locale assumptions, calendar, timezone behavior, validation rules, examples, and ambiguity.

Never treat `010224` as a definite date without enough context to decide whether it means 1 February 2024 or 2 January 2024.

### **5.2 Numeric representations**

Include:

- Signed and unsigned integers.
- Fixed-width and arbitrary-precision integers.
- Float32, float64 and decimal/fixed-point types.
- Currency precision and decimal scales.
- Scientific notation and percentage representations.
- Rounding modes, truncation and precision loss.
- Overflow, underflow, NaN, infinity and negative zero.
- Integer division and modulo behavior.

For every type, record its precision, range, representation, conversion behavior, and language-specific differences.

### **5.3 Null, missing and empty values**

Explicitly distinguish:

- SQL `NULL`.
- Python `None`.
- JavaScript `null` and `undefined`.
- Java and C# null references.
- SAS numeric missing values (`.` and `.A` through `.Z`) and character missing values.
- Empty strings.
- Whitespace-only strings.
- Missing object properties.
- NaN and sentinel values.

Record comparison, truthiness, sorting, aggregation, joins, serialization, and conversion behavior.

This category is particularly important because apparently equivalent conditions can produce different results after migration.

### **5.4 Strings and encoding**

Include:

- UTF-8, UTF-16 and ASCII.
- Unicode normalization forms.
- Bytes versus code points versus grapheme clusters.
- Case sensitivity and case folding.
- Locale-specific casing.
- Accent sensitivity and collation.
- Whitespace and trimming behavior.
- String indexing and length differences.
- Escaping and serialization rules.

### **5.5 Identity and financial formats**

Include public format definitions and synthetic examples for email addresses, phone numbers, postal codes, country codes, UUIDs, currencies and payment-card reference formats.

Use reserved example domains and synthetic values. Never collect or store real personal information, live payment card details, or credentials.

### **5.6 Language-specific semantics**

Create separate datasets for each supported language and database dialect.

For example:

```
knowledge/languages/
├── sas/
│   ├── date_semantics.json
│   ├── missing_value_semantics.json
│   ├── numeric_semantics.json
│   └── format_informat_reference.json
│
├── python/
│   ├── datetime_semantics.json
│   ├── numeric_semantics.json
│   ├── null_semantics.json
│   └── string_semantics.json
│
├── javascript/
│   ├── date_semantics.json
│   ├── number_semantics.json
│   ├── null_undefined_semantics.json
│   └── string_semantics.json
│
└── sql/
    ├── null_semantics.json
    ├── numeric_semantics.json
    ├── date_semantics.json
    └── dialect_differences.json
```

The language list should be extensible. Do not assume that every SQL database or language follows identical semantics.

## **6. External knowledge collection and updating**

The repository should combine three kinds of knowledge:

1. **Standards-based knowledge:** Official specifications and machine-readable reference datasets.
2. **Language-specific knowledge:** Official programming-language and database documentation.
3. **Curated semantic knowledge:** Our own structured explanations, compatibility rules, conversion guidance, and edge cases derived from the first two.

Do not simply scrape websites and put their text into JSON. The collection pipeline should convert trusted source material into structured records, retain provenance, and validate the resulting knowledge.

### **6.1 Source catalogue**

File: `sources/source_catalog.json`

Every external source must be registered before its content is imported.

Example:

```
{
  "sources": [
    {
      "source_id": "SRC_UNICODE_CLDR",
      "name": "Unicode Common Locale Data Repository",
      "publisher": "Unicode Consortium",
      "source_url": "https://www.unicode.org/cldr/",
      "source_type": "official_standard_data",
      "categories": ["dates", "numbers", "locale"],
      "collection_method": "official_release_data",
      "license": "verify_current_release_terms",
      "version": null,
      "retrieved_at": null,
      "checksum": null,
      "status": "pending_collection"
    }
  ]
}
```

Recommended source catalogue:

| Knowledge                                  | Preferred source                                                                                                                                                     |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Locale-specific dates and numbers          | [Unicode CLDR date and time data](https://www.unicode.org/reports/tr35/tr35-dates.html?utm_source=chatgpt.com)                                                       |
| Timezone history and daylight-saving rules | [IANA Time Zone Database](https://www.iana.org/time-zones?authuser=1\&utm_source=chatgpt.com)                                                                        |
| Internet timestamp format                  | [RFC 3339](https://www.rfc-editor.org/info/rfc3339/?utm_source=chatgpt.com) and its update, RFC 9557                                                                 |
| SAS date, time and datetime semantics      | [SAS date and datetime documentation](https://support.sas.com/documentation/cdl/en/lrcon/65287/HTML/default/p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm?utm_source=chatgpt.com) |
| SAS date informats                         | [SAS informat reference](https://support.sas.com/documentation/cdl/en/etsug/60372/HTML/default/etsug_intervals_sect008.htm?utm_source=chatgpt.com)                   |
| Language-specific behavior                 | Official Python, ECMAScript, C++, Java, .NET and database documentation                                                                                              |
| Currency and country codes                 | Official standards bodies and appropriately licensed reference datasets                                                                                              |
| Unicode encoding and normalization         | Unicode specifications and Unicode Character Database                                                                                                                |

For example, CLDR provides locale-aware date/time and number formatting information. IANA provides machine-readable timezone history that changes as governments change their rules.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://www.unicode.org\&sz=32)

Unicode+1



SAS documentation is especially important for this project: SAS date values represent days since January 1, 1960, while SAS datetime values represent seconds since that reference date. That difference needs to be represented as two distinct concepts in the knowledge base, not as one generic date conversion.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://support.sas.com\&sz=32)

support.sas.com+1



### **6.2 Collection pipeline**

Implement the collection process in these stages:

1. **Source discovery**

   Identify official datasets, standards, documentation pages, release archives and machine-readable downloads.
2. **Source registration**

   Register the publisher, URL, source type, license, version and permitted collection method.
3. **Data acquisition**

   Download official JSON, XML, CSV or text data where permitted. Use HTML extraction only when no structured source exists.
4. **Normalization**

   Convert acquired information into the repository's category-specific JSON schema.
5. **Semantic enrichment**

   Add definitions, interpretation rules, language mappings, examples, ambiguity flags and edge cases.
6. **Validation**

   Validate the JSON schema, record relationships, source references, duplicate IDs and required fields.
7. **Publication**

   Update the knowledge files and registry atomically, preserving the previous valid version.

### **6.3 Source update mechanism**

Build `source_collector.py` and `check_source_updates.py` to:

- Check for new releases or changed source files.
- Compare source versions and checksums.
- Identify which knowledge datasets are affected.
- Re-import only changed datasets.
- Preserve the old version for rollback.
- Record when a source was checked and when it was last successfully updated.
- Mark records as potentially stale if an upstream source changes.

Do not overwrite knowledge just because an external webpage changed. Changes should go through normalization, validation and publication.

Use scheduled updates for frequently changing datasets such as timezone data, and less frequent checks for stable language semantics. The update frequency should be configurable per source.

## **7. Cross-Language Semantic Compatibility Graph**

This is the most important additional component. It will make the repository useful to the migration engine rather than just a collection of definitions.

The idea is to connect a concept with its representation in different languages and the rules needed to preserve its behavior.

### **7.1 Graph structure**

Each mapping should connect:

- A common semantic concept.
- Source-language representation.
- Target-language representation.
- Conversion rules.
- Behavioral differences.
- Edge cases.
- Preservation strategy.
- Supporting knowledge records.

Example:

**Common concept: Calendar date**

Semantic concept ID: `CONCEPT_CALENDAR_DATE`

**SAS**

# **Days**

Since 01-01-1960

**Python**

# **Date**

`datetime.date`

**Mapping:&#x20;**`MAP_SAS_DATE_TO_PYTHON_DATE`

**Preservation requirements**

- Preserve the SAS epoch reference.
- Convert days, not seconds.
- Preserve missing-value behavior.
- Do not accidentally interpret a calendar date as a timezone-aware instant.

### **7.2 Mapping JSON example**

File: `knowledge/cross_language_mappings/date_mappings.json`

```
{
  "mapping_id": "MAP_SAS_DATE_TO_PYTHON_DATE",
  "concept_id": "CONCEPT_CALENDAR_DATE",
  "source": {
    "language": "SAS",
    "representation": "numeric_date",
    "epoch": "1960-01-01",
    "unit": "days"
  },
  "target": {
    "language": "Python",
    "representation": "datetime.date",
    "epoch": null,
    "unit": "calendar_date"
  },
  "conversion": {
    "strategy": "add_source_day_offset_to_epoch",
    "formula_description": "Add the SAS numeric date value as a number of calendar days to 1960-01-01.",
    "code_example": "datetime.date(1960, 1, 1) + datetime.timedelta(days=sas_date)"
  },
  "semantic_differences": [
    {
      "difference": "SAS date is stored as a numeric day offset while Python datetime.date represents a calendar date.",
      "risk": "incorrect_unit_conversion"
    }
  ],
  "edge_cases": [
    "negative_date_values",
    "leap_years",
    "SAS_missing_values"
  ],
  "preservation_rules": [
    "Do not multiply the SAS date value by 86400.",
    "Handle SAS missing values before performing date arithmetic."
  ],
  "related_knowledge": [
    "FMT_SAS_DATE",
    "TYPE_PYTHON_DATE",
    "NULL_SAS_NUMERIC_MISSING"
  ],
  "confidence": "high",
  "evidence_sources": [
    "SRC_SAS_DATE_DOCUMENTATION"
  ]
}
```

The formula description is intentionally explicit. It prevents the migration engine from confusing SAS dates with SAS datetimes, which use a different unit.

### **7.3 Other important mapping examples**

Build mappings for:

| Source               | Target                 | Main risk                               |
| -------------------- | ---------------------- | --------------------------------------- |
| SAS date             | Python date            | Epoch and day units                     |
| SAS datetime         | Python datetime        | Epoch, seconds and timezone assumptions |
| SQL NULL             | Python None            | Missing-value and comparison behavior   |
| JavaScript Number    | C++ integer            | Precision, range and fractional values  |
| C++ integer division | JavaScript arithmetic  | Truncation and negative operands        |
| SQL DECIMAL          | Python Decimal         | Scale and rounding                      |
| SAS missing numeric  | Python None            | Missing-value comparisons and ordering  |
| JavaScript undefined | JSON property omission | Serialization behavior                  |

### **7.4 Compatibility decision model**

Each mapping should support a clear decision:

- `EQUIVALENT`: Source and target behavior are equivalent for the relevant operation.
- `CONDITIONALLY_EQUIVALENT`: Equivalent only when specified conditions hold.
- `CONVERTIBLE`: A conversion exists, but the code must apply it explicitly.
- `INCOMPATIBLE`: The target behavior differs and requires correction.
- `UNCERTAIN`: Available evidence is insufficient.

Do not infer equivalence merely because two variables have the same name or both use a numeric type.

## **8. Knowledge retrieval and semantic resolution**

The LLM should not receive the entire repository. It should receive only the records relevant to the current migration operation.

### **8.1 Retrieval workflow**

1. The migration engine parses source and target code.
2. Its semantic analysis identifies operations such as `DATE_PARSE`, `DATE_COMPARE`, `NULL_CHECK`, `DECIMAL_ROUND` or `STRING_COMPARE`.
3. The knowledge retrieval service receives the concept, language pair and any known type/format information.
4. It looks up matching descriptors in the registry.
5. It retrieves the detailed JSON records and related mappings.
6. The context builder packages the relevant evidence for the LLM.
7. The semantic resolver returns a structured result to the migration engine.

Example request:

```
{
  "source_language": "SAS",
  "target_language": "Python",
  "operation": "DATE_CONVERSION",
  "source_representation": "SAS_NUMERIC_DATE",
  "target_representation": "PYTHON_DATETIME_DATE",
  "known_context": {
    "source_format": "DATE9."
  }
}
```

Example response:

```
{
  "concept": "CONCEPT_CALENDAR_DATE",
  "compatibility": "CONVERTIBLE",
  "source_semantics": {
    "language": "SAS",
    "unit": "days",
    "epoch": "1960-01-01"
  },
  "target_semantics": {
    "language": "Python",
    "type": "datetime.date"
  },
  "identified_risks": [
    "incorrect_epoch",
    "incorrect_unit",
    "missing_value_handling"
  ],
  "recommended_mapping": "MAP_SAS_DATE_TO_PYTHON_DATE",
  "confidence": "high",
  "evidence": [
    "SRC_SAS_DATE_DOCUMENTATION"
  ]
}
```

### **8.2 Retrieval strategy**

Start with a deterministic JSON-based retrieval implementation:

- Exact lookup by stable ID.
- Search by concept ID.
- Search by language pair.
- Search by category, aliases and tags.
- Search by source or target representation.
- Related-record lookup.

For the first version, load the registry into memory and build indexes for these fields. For larger datasets, add a search index or database later.

A vector database is not required initially. Semantic search can be added as a secondary retrieval mechanism when exact and indexed retrieval are insufficient.

### **8.3 Context builder**

File: `services/llm_context_builder.py`

Its responsibility is to prepare a compact, structured knowledge package.

It should:

- Include only relevant records.
- Include related language mappings and edge cases.
- Distinguish official standards from curated interpretations.
- Include source/version provenance.
- Include uncertainty and ambiguity warnings.
- Enforce a configurable token or size budget.
- Preserve stable knowledge IDs in the output.

The LLM must not invent knowledge IDs, claim that an unverified rule is official, or silently resolve ambiguous formats.

### **8.4 Semantic resolver**

File: `services/semantic_resolver.py`

The resolver should compare the source and target semantics using retrieved evidence.

Its output should include:

- Source behavior.
- Target behavior.
- Whether they are equivalent.
- Differences and affected conditions.
- Relevant knowledge IDs.
- Confidence and unresolved assumptions.
- Recommended preservation strategy.

It should not directly rewrite code. Code correction remains the responsibility of the existing migration engine.

## **9. Service and API implementation**

The repository should expose a clean interface so the migration engine can retrieve knowledge without knowing how files are organized internally.

### **9.1 Core service responsibilities**

| Service                    | Responsibility                                     |
| -------------------------- | -------------------------------------------------- |
| `registry_service.py`      | Load and query the knowledge registry              |
| `knowledge_loader.py`      | Load JSON content and cache datasets               |
| `knowledge_search.py`      | Search concepts, aliases, tags and representations |
| `source_collector.py`      | Acquire and normalize external knowledge           |
| `schema_validator.py`      | Validate JSON structure and references             |
| `semantic_resolver.py`     | Compare source and target semantics                |
| `cross_language_mapper.py` | Find and evaluate language mappings                |
| `llm_context_builder.py`   | Prepare relevant knowledge for the LLM             |

Keep these services independent and testable. Do not put all functionality in one large service class.

### **9.2 Suggested API endpoints**

| Method | Endpoint                                  | Purpose                              |
| ------ | ----------------------------------------- | ------------------------------------ |
| GET    | `/api/v1/knowledge/categories`            | List knowledge categories            |
| GET    | `/api/v1/knowledge/datasets`              | List available datasets              |
| GET    | `/api/v1/knowledge/datasets/{dataset_id}` | Retrieve dataset metadata            |
| GET    | `/api/v1/knowledge/records/{record_id}`   | Retrieve a specific knowledge record |
| POST   | `/api/v1/knowledge/search`                | Search knowledge                     |
| POST   | `/api/v1/mappings/find`                   | Find cross-language mappings         |
| POST   | `/api/v1/semantics/resolve`               | Compare source and target semantics  |
| GET    | `/api/v1/sources`                         | List registered sources              |
| POST   | `/api/v1/admin/validate`                  | Validate repository integrity        |
| POST   | `/api/v1/admin/refresh`                   | Trigger a permitted source refresh   |

The admin endpoints should be protected. Normal migration requests should have read-only access to published knowledge.

### **9.3 Search request example**

```
{
  "query": "SAS date conversion to Python",
  "category": "dates",
  "source_language": "SAS",
  "target_language": "Python",
  "concepts": [
    "DATE_CONVERSION"
  ],
  "limit": 10
}
```

### **9.4 Search response example**

```
{
  "results": [
    {
      "knowledge_id": "MAP_SAS_DATE_TO_PYTHON_DATE",
      "name": "SAS date to Python date",
      "category": "cross_language_mappings",
      "relevance": "exact_language_pair",
      "content_path": "knowledge/cross_language_mappings/date_mappings.json"
    }
  ],
  "total": 1
}
```

Use stable IDs in the response. The service should return the actual record content or a reference that the caller can resolve through the service; avoid making the migration engine construct filesystem paths itself.

## **10. Integration with the existing migration engine**

This is a critical implementation constraint: the new repository must enhance the existing migration system without disrupting its current behavior.

### **10.1 Integration points**

Integrate at the semantic-analysis and code-comparison stage.

```
                 EXISTING MIGRATION SYSTEM
                           |
                    Source / Target
                           |
                      Code Parsing
                           |
                   Intermediate IR
                           |
                 Semantic Operation Detection
                           |
                           v
              UNIVERSAL KNOWLEDGE INTERFACE
                           |
                  Concept Identification
                           |
                  Relevant JSON Retrieval
                           |
               Cross-Language Mapping Lookup
                           |
                  Semantic Resolution
                           |
                           v
                 EXISTING MIGRATION SYSTEM
                           |
                 Semantic Difference Report
                           |
                  Target Code Correction
                           |
                   Validation / Review
```

### **10.2 Example migration scenario**

Source SAS code:

```
data result;
    set input_data;
    new_date = old_date + 30;
run;
```

Suppose the target implementation is:

```
new_date = old_date + 30
```

The migration engine must not assume that the two expressions mean the same thing.

It should determine:

1. Is `old_date` a SAS numeric date value or a normal numeric variable?
2. Is the target `old_date` a Python `datetime.date`, a `datetime.datetime`, or an integer?
3. Does adding `30` mean 30 calendar days in both implementations?
4. How are missing values handled?
5. Does the target implementation preserve the source's behavior?

The knowledge repository supplies the language and representation semantics needed to answer those questions.

If the target uses Python `datetime.date`, the correct target operation may be:

```
new_date = old_date + datetime.timedelta(days=30)
```

But this correction should only be recommended after the migration engine has established the actual source and target types. The repository must not blindly transform every numeric addition into date arithmetic.

### **10.3 Backward-compatible integration**

Implement the following rules:

- Existing migration behavior should remain the default until the new integration is validated.
- Add the repository as an optional dependency or service interface.
- If knowledge retrieval fails, report the failure and follow the existing migration fallback policy.
- Never silently treat missing knowledge as proof of equivalence.
- Keep source/target parsing and code-generation responsibilities unchanged.
- Add configuration to enable or disable knowledge-assisted analysis.
- Log the knowledge IDs used in every semantic decision.

This makes it possible to test the new functionality against existing migration cases without changing the core engine all at once.

## **11. Validation and quality assurance**

The repository itself needs a quality-control system. Incorrect knowledge can lead to incorrect code corrections, so validating the knowledge is as important as collecting it.

### **11.1 JSON schema validation**

Every dataset must be checked for:

- Valid JSON syntax.
- Required metadata fields.
- Valid record structures.
- Unique stable IDs.
- Valid cross-references.
- Valid source references.
- Correct category-specific fields.
- Supported schema versions.
- Correct content paths.

Use JSON Schema for structural validation.

### **11.2 Semantic validation**

Add category-specific validation rules.

**Date validation**

- Validate month and day ranges.
- Check leap-year rules.
- Check two-digit year ambiguity.
- Check calendar and timezone requirements.
- Distinguish date, datetime and timestamp semantics.

**Numeric validation**

- Validate declared ranges and precision.
- Check rounding and overflow descriptions.
- Detect incompatible unit or scale definitions.

**Mapping validation**

- Confirm both source and target references exist.
- Confirm all referenced knowledge IDs exist.
- Check that a mapping declares its preconditions.
- Ensure that a claimed equivalent mapping has supporting evidence.

### **11.3 Provenance validation**

Every externally sourced record should have enough information to trace its origin:

- Source ID.
- Publisher.
- Original URL.
- Source version or publication date, where available.
- Retrieval timestamp.
- Content checksum, where practical.
- License and permitted use.
- Whether the record was imported directly or curated.

Never claim a curated interpretation is directly specified by a standard unless the source supports that claim.

### **11.4 Migration-specific evaluation**

Create a set of known semantic scenarios to evaluate the repository's usefulness.

These are evaluation cases for the migration engine, not a separate test-data generation product.

Examples:

| Scenario                                   | Expected outcome                               |
| ------------------------------------------ | ---------------------------------------------- |
| SAS date → Python date                     | Recognize different internal representations   |
| SAS datetime → Python datetime             | Recognize epoch and seconds conversion         |
| SQL NULL → Python None                     | Identify missing-value differences             |
| C++ integer division → JavaScript division | Identify possible fractional-result difference |
| SQL DECIMAL → JavaScript Number            | Identify precision risk                        |
| Ambiguous `010224` → date                  | Require interpretation context                 |
| UTF-8 string → UTF-16 string               | Preserve textual meaning                       |
| SAS missing numeric → Python None          | Preserve missing-value semantics               |

The evaluation should measure whether the system finds the relevant knowledge, identifies the right semantic difference, and avoids unsupported conclusions.

## **12. Security, performance and reliability**

### **Security**

- Never collect real personal information, payment credentials or secrets.
- Use synthetic examples for identity and financial formats.
- Treat downloaded source files as untrusted input.
- Validate file sizes, JSON nesting and record counts.
- Restrict external collection to approved domains and protocols.
- Protect administrative refresh endpoints.
- Keep external collection separate from normal migration requests.

### **Performance**

For the initial implementation:

- Load the registry once and cache it.
- Load frequently used datasets lazily.
- Index stable IDs, concepts, tags and language pairs.
- Cache frequently requested mappings.
- Avoid loading all knowledge into the LLM context.
- Support retrieval by dataset and record ID.
- Use incremental refresh rather than rebuilding the entire repository.
- Avoid repeated filesystem scans for every migration operation.

### **Reliability**

- Publish new knowledge versions atomically.
- Retain the last known-good repository snapshot.
- Reject invalid datasets instead of partially publishing them.
- Record retrieval failures and source-update failures.
- Provide a repository health-check operation.
- Make source refresh jobs idempotent.

## **13. Implementation phases and deliverables**

Implement this in phases. The first milestone should deliver a functioning knowledge repository and retrieval interface, not a huge collection of partially verified data.

1. **Phase 1 — Existing codebase inspection and design**

   First priority
   - Inspect the existing migration engine.
   - Identify language parsers, IR structures and semantic analysis.
   - Identify integration points and existing API conventions.
   - Identify the current technology stack, configuration and deployment model.
   - Finalize the repository structure and JSON schemas.

   **Deliverables:** Architecture assessment, integration design, schema definitions and implementation backlog.
2. **Phase 2 — JSON repository foundation**
   - Create the central registry.
   - Implement metadata descriptors.
   - Implement category-specific schemas.
   - Implement the JSON loader and registry service.
   - Implement stable ID and reference validation.
   - Add sample knowledge records.

   **Deliverables:** Working repository with registry, descriptors, schemas and validation.
3. **Phase 3 — Core knowledge population**

   High priority
   - Populate date/time knowledge.
   - Populate numeric knowledge.
   - Populate null and missing-value knowledge.
   - Populate string and encoding knowledge.
   - Add initial language-specific records for SAS, SQL, Python and JavaScript.
   - Record source provenance for every imported or curated record.

   **Deliverables:** Verified core knowledge datasets with metadata and source records.
4. **Phase 4 — External source collector**
   - Implement source registration.
   - Implement permitted download and extraction methods.
   - Normalize collected content.
   - Track source versions and checksums.
   - Implement refresh and rollback.
   - Add source-specific update policies.

   **Deliverables:** Repeatable, auditable knowledge collection and refresh pipeline.
5. **Phase 5 — Compatibility mappings**
   - Implement the mapping JSON schema.
   - Build SAS-to-Python date mappings.
   - Build SQL-to-Python null mappings.
   - Build numeric and string mappings.
   - Add preconditions, risks, edge cases and evidence.
   - Implement compatibility decision classification.

   **Deliverables:** Initial cross-language semantic compatibility graph represented in JSON.
6. **Phase 6 — Retrieval and semantic resolution**
   - Implement concept-based search.
   - Implement language-pair and representation lookup.
   - Implement related-record retrieval.
   - Implement semantic resolver.
   - Implement compact LLM context builder.
   - Return structured semantic decisions with evidence.

   **Deliverables:** Retrieval API and semantic-resolution service.
7. **Phase 7 — Migration engine integration**
   - Connect semantic operation detection to knowledge retrieval.
   - Connect retrieved mappings to source/target comparison.
   - Pass relevant evidence to the migration LLM.
   - Preserve existing correction and validation flows.
   - Add feature flags and fallback behavior.

   **Deliverables:** Working knowledge-assisted code migration flow.
8. **Phase 8 — Evaluation and production readiness**
   - Run semantic evaluation scenarios.
   - Check mapping correctness.
   - Check retrieval precision and response latency.
   - Test large repository loading and concurrent requests.
   - Test source refresh, rollback and invalid datasets.
   - Document how to add new categories and languages.

   **Deliverables:** Evaluation results, operational documentation and production-ready release.

### **Suggested milestone boundaries**

| Milestone | Completion criteria                                |
| --------- | -------------------------------------------------- |
| M1        | Registry, metadata and schema validation work      |
| M2        | Core date, numeric and null datasets are populated |
| M3        | External collection and provenance work            |
| M4        | Cross-language mappings and retrieval work         |
| M5        | Existing migration engine consumes knowledge       |
| M6        | Evaluation and production-readiness checks pass    |

Do not start with every possible data format. Complete the foundational schemas and a small but reliable set of knowledge first, then expand through the collection pipeline.

## **14. Definition of done / acceptance criteria**

The implementation should not be considered complete until the following conditions are satisfied.

| Area                  | Acceptance criteria                                                                   |
| --------------------- | ------------------------------------------------------------------------------------- |
| JSON repository       | Every dataset has valid JSON content and a metadata descriptor                        |
| Registry              | Every active dataset can be discovered through the central registry                   |
| Extensibility         | New categories and languages can be added without changing the core architecture      |
| Provenance            | External records can be traced to their sources and versions                          |
| Collection            | Approved sources can be refreshed without damaging existing knowledge                 |
| Retrieval             | Knowledge can be retrieved by ID, concept, category and language pair                 |
| Semantic mappings     | Source-to-target mappings include conversion rules and edge cases                     |
| Ambiguity             | Ambiguous formats are identified instead of silently interpreted                      |
| LLM integration       | Only relevant knowledge is included in the LLM context                                |
| Migration integration | The existing migration engine can use knowledge without breaking its current workflow |
| Reliability           | Invalid knowledge cannot replace the last valid repository version                    |

### **Important design decisions**

I recommend keeping these decisions fixed throughout implementation:

1. **JSON is the source of truth.** Do not introduce a database as a mandatory dependency in the first version.
2. **Metadata and content are separate.** Every dataset has its own descriptor and actual knowledge file.
3. **Use category-specific schemas.** Different types of knowledge need different structures.
4. **Prefer official machine-readable sources.** Use web scraping only when structured source data is unavailable.
5. **Use stable IDs for relationships.** This is essential for reliable mappings and retrieval.
6. **Keep the compatibility graph in JSON initially.** A graph database can be considered if the relationships eventually become too large or complex.
7. **Use the LLM for interpretation, not as the authority for facts.** Knowledge records, source provenance and deterministic rules should ground its decisions.
8. **Do not make assumptions when context is missing.** Return `UNCERTAIN` when a format, type, locale, timezone or conversion policy cannot be established.

## **15. Instructions to give your coding agent**

The following is the implementation task I would hand to the coding agent.

# Implementation Task: Universal Data Format & Semantics Knowledge Repository

## Objective

Implement a Universal Data Format & Semantics Knowledge Repository that integrates with our existing cross-language code migration engine.

The repository must provide structured, reusable JSON knowledge about data representations, formats, types, language-specific behavior, conversion rules, and semantic differences.

Its primary purpose is to help our migration engine understand the actual meaning and behavior of source and target code, identify semantic incompatibilities, and support behavior-preserving target-code corrections.

This is NOT a standalone test-data generation platform.

## Step 1: Inspect the Existing Project Before Making Changes

Before implementing anything:

1. Explore the complete existing project structure.
2. Identify the source-code and target-code ingestion flow.
3. Understand the current language parsers and intermediate representation (IR).
4. Identify where semantic operations and data types are detected.
5. Understand the existing LLM integration and context-building process.
6. Identify the code-comparison and target-code correction stages.
7. Review the current API, configuration, logging, testing and deployment conventions.
8. Identify the safest integration point for the new knowledge repository.

Prepare an architecture assessment describing:

- Existing components that can be reused.
- Components that need to be introduced.
- Proposed integration points.
- Potential compatibility risks.
- Any existing functionality that must remain unchanged.

Do not replace existing architecture or create a disconnected application without first understanding the project.

## Step 2: Implement the JSON Repository Foundation

Create a modular repository containing:

- Central knowledge registry.
- Category registry.
- Language registry.
- Dataset metadata descriptors.
- Category-specific JSON schemas.
- Knowledge content files.
- Source catalogue and provenance records.

Every knowledge dataset must have:

1. A `.meta.json` descriptor.
2. A JSON content file.
3. A stable dataset ID.
4. A version.
5. A description.
6. A category and subcategory.
7. Tags and aliases.
8. Supported languages and representations.
9. Retrieval guidance.
10. Source and license information.
11. Creation and update timestamps.

Use `date_current` only where a dynamic current-date reference is genuinely required. Do not use it as a replacement for `date_update`.

Implement validation for JSON syntax, required fields, unique IDs, file references and cross-record relationships.

## Step 3: Populate the Knowledge Categories

Implement the following categories:

### A. Dates and timestamps

Include:

- ddmmyy, mmddyy, mmddyyyy, ddmmyyyy.
- yyyyMMdd, yyMMdd, yyyy-MM-dd.
- ISO 8601 and RFC 3339.
- Locale-specific formats and month names.
- Date, datetime, timestamp, time and duration.
- Timezones and UTC offsets.
- Epoch seconds, milliseconds, microseconds and nanoseconds.
- Excel and SAS serial date representations.
- Julian dates, week dates, quarters and partial dates.
- Two-digit year interpretation and ambiguous formats.

### B. Numeric semantics

Include:

- Integer and floating-point representations.
- Signed and unsigned types.
- Fixed decimal and arbitrary precision.
- Precision, scale and rounding.
- Scientific notation and percentages.
- Integer division and modulo.
- Overflow, underflow, NaN, infinity and negative zero.

### C. Null and missing values

Include:

- SQL NULL.
- Python None.
- JavaScript null and undefined.
- Java and C# null.
- SAS numeric and character missing values.
- Empty strings and whitespace-only values.
- Missing fields and sentinel values.

### D. Strings and encoding

Include:

- UTF-8, UTF-16 and ASCII.
- Unicode normalization.
- Case sensitivity and case folding.
- Locale-specific casing.
- Collation and accent sensitivity.
- String length and indexing.
- Whitespace, escaping and serialization.

### E. Identity and financial formats

Include public format references for:

- Email addresses.
- Phone numbers.
- Postal codes.
- Country and currency codes.
- UUIDs and identifiers.
- Currency precision and rounding.
- Payment-card format references.

Use synthetic examples only. Never collect real personal information, credentials or payment-card details.

### F. Boolean and structured data

Include:

- Boolean representations and language truthiness.
- JSON, XML, CSV and YAML semantics.
- Missing keys, nulls, arrays, type coercion and serialization behavior.

### G. Language-specific semantics

Initially prioritize SAS, SQL, Python and JavaScript. Keep the architecture extensible to C++, Java, C# and other languages.

Include language-specific date, numeric, null, string, casting and comparison behavior.

## Step 4: Implement External Knowledge Collection

Create a source collection mechanism that:

1. Registers approved official sources.
2. Retrieves machine-readable reference data where available.
3. Supports permitted documentation extraction when structured data is unavailable.
4. Normalizes collected information into our JSON schemas.
5. Stores source URL, publisher, version, retrieval time, checksum and license information.
6. Tracks upstream changes.
7. Updates only affected datasets.
8. Validates new content before publication.
9. Retains previous valid versions for rollback.

Prioritize Unicode CLDR, IANA timezone data, RFCs, official language documentation, official database documentation and other appropriate standards sources.

Respect source licenses, terms of use and rate limits. Do not indiscriminately scrape websites.

Separate externally imported data from our own curated explanations and interpretations.

## Step 5: Implement the Cross-Language Semantic Compatibility Graph

Represent relationships between common concepts, language-specific representations, conversion rules and semantic differences using JSON records and stable IDs.

Each mapping must contain:

- Mapping ID.
- Common semantic concept ID.
- Source language and representation.
- Target language and representation.
- Conversion strategy.
- Preconditions and assumptions.
- Semantic differences.
- Edge cases.
- Preservation rules.
- Related knowledge IDs.
- Supporting sources.
- Confidence and compatibility classification.

Support these compatibility classifications:

- EQUIVALENT.
- CONDITIONALLY_EQUIVALENT.
- CONVERTIBLE.
- INCOMPATIBLE.
- UNCERTAIN.

Create initial mappings for:

- SAS date to Python date.
- SAS datetime to Python datetime.
- SQL NULL to Python None.
- JavaScript Number to C++ integer.
- C++ integer division to JavaScript arithmetic.
- SQL DECIMAL to Python Decimal.
- SAS missing numeric values to Python missing-value handling.

Do not claim two representations are equivalent just because they have the same variable name or apparent data type.

## Step 6: Implement Repository Services

Implement modular services for:

- Registry lookup.
- Knowledge loading and caching.
- Knowledge search.
- Source collection.
- JSON schema validation.
- Cross-language mapping lookup.
- Semantic resolution.
- LLM context preparation.

Initially use deterministic lookup and indexed JSON retrieval. Do not make a vector database mandatory.

Add a REST API if that matches the existing application's architecture. Otherwise expose the same functionality through the project's existing service/interface conventions.

## Step 7: Implement Semantic Retrieval

The retrieval interface must accept:

- Source language.
- Target language.
- Detected operation or concept.
- Source representation.
- Target representation.
- Known format, type, locale or other context.

It must return only the relevant knowledge records, mappings and evidence.

The LLM context builder must:

- Include relevant definitions and edge cases.
- Include related mappings.
- Distinguish official source facts from curated interpretation.
- Preserve knowledge IDs and provenance.
- Respect configurable context-size limits.
- Identify ambiguity and missing information.

Never load the entire knowledge repository into an LLM prompt.

## Step 8: Integrate with the Existing Migration Engine

Integrate at the semantic-analysis and comparison stages.

The flow should be:

Source and target parsing → IR → semantic operation detection → knowledge retrieval → compatibility mapping → semantic resolution → existing migration comparison → target correction.

Use the retrieved knowledge to help the existing migration engine identify semantic differences and determine how to preserve source behavior.

Do not move code generation into the repository.

Preserve the existing migration engine's parsing, comparison, correction and validation responsibilities.

Add a feature flag to enable or disable knowledge-assisted analysis. Keep existing behavior available as a fallback.

A failed knowledge lookup must never be interpreted as proof of semantic equivalence.

## Step 9: Validation and Evaluation

Implement:

- JSON schema validation.
- Stable-ID and reference validation.
- Duplicate detection.
- Provenance validation.
- Source refresh and rollback tests.
- Retrieval correctness checks.
- Mapping consistency checks.
- Semantic evaluation scenarios.

Evaluate representative migrations, particularly SAS-to-Python and SQL-to-Python.

Check that the system:

- Finds the right knowledge.
- Identifies semantic differences correctly.
- Handles ambiguous values safely.
- Provides evidence for its decisions.
- Does not invent conversion rules.
- Does not falsely classify uncertain cases as equivalent.

## Step 10: Documentation and Delivery

Provide:

1. Updated project structure.
2. Architecture documentation.
3. JSON schema documentation.
4. Metadata descriptor specification.
5. Knowledge record specification.
6. Source collection and refresh instructions.
7. Instructions for adding a new category.
8. Instructions for adding a new language.
9. Retrieval API documentation.
10. Migration engine integration documentation.
11. Evaluation results and known limitations.

## Critical Implementation Constraints

- JSON must remain the primary knowledge storage format.
- Do not build a standalone test-data generator.
- Do not rewrite the existing migration engine.
- Do not make a vector database or graph database mandatory initially.
- Do not put all knowledge into one huge JSON file.
- Do not treat all language implementations as semantically identical.
- Do not assume date formats or locales when context is missing.
- Do not silently discard missing-value or precision differences.
- Do not publish unvalidated external data.
- Keep all components modular, extensible and independently testable.

## Expected Final Outcome

A reusable JSON-based knowledge repository that can be queried by the existing migration engine to understand data representations and language semantics, retrieve appropriate conversion rules, identify compatibility problems and support accurate, behavior-preserving cross-language code migration.

Begin by inspecting the existing project and presenting the architecture assessment. Then implement the repository incrementally, validating each phase before integrating it with the existing migration workflow.

## **My recommendation**

Build **Phase 1 through Phase 3 first**, with dates, numeric semantics and null/missing-value semantics as the initial knowledge domains. These three areas expose many subtle migration errors, especially in SAS, SQL and Python conversions.

The most valuable innovation here is the **semantic compatibility graph**. A conventional documentation repository tells the LLM what a SAS date or Python date is. The compatibility graph tells it precisely what changes when one is translated into the other, which assumptions must be preserved, and what can go wrong. That is what makes this repository useful to your migration engine rather than just another documentation search system.