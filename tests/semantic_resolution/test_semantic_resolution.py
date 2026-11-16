from pathlib import Path
from services.registry_service import RegistryService
from services.knowledge_search import KnowledgeSearchEngine
from services.cross_language_mapper import CrossLanguageMapper
from services.semantic_resolver import SemanticResolver

root_dir = Path(__file__).resolve().parent.parent.parent


def setup_resolver() -> SemanticResolver:
    service = RegistryService(base_dir=root_dir)
    search_engine = KnowledgeSearchEngine(service)
    mapper = CrossLanguageMapper(service)
    return SemanticResolver(mapper, search_engine)


def test_scenario_sas_date_to_python_date():
    resolver = setup_resolver()
    result = resolver.resolve(
        source_language="SAS",
        target_language="Python",
        operation="DATE_CONVERSION",
        source_representation="numeric_date",
        target_representation="datetime.date",
        known_context={"source_format": "DATE9."}
    )

    assert result.concept == "CONCEPT_CALENDAR_DATE"
    assert result.compatibility == "CONVERTIBLE"
    assert result.source_semantics["unit"] == "days"
    assert result.source_semantics["epoch"] == "1960-01-01"
    assert result.recommended_mapping == "MAP_SAS_DATE_TO_PYTHON_DATE"
    assert "incorrect_unit_conversion" in result.identified_risks
    assert any("timedelta" in rule for rule in result.preservation_rules)
    assert result.confidence in ["high", "verified"]


def test_scenario_sas_datetime_to_python_datetime():
    resolver = setup_resolver()
    result = resolver.resolve(
        source_language="SAS",
        target_language="Python",
        operation="DATETIME_CONVERSION",
        source_representation="numeric_datetime",
        target_representation="datetime.datetime"
    )

    assert result.concept == "CONCEPT_TIMESTAMP_INSTANT"
    assert result.compatibility == "CONVERTIBLE"
    assert result.source_semantics["unit"] == "seconds"
    assert result.source_semantics["epoch"] == "1960-01-01T00:00:00"
    assert result.recommended_mapping == "MAP_SAS_DATETIME_TO_PYTHON_DATETIME"


def test_scenario_sql_null_to_python_none():
    resolver = setup_resolver()
    result = resolver.resolve(
        source_language="SQL",
        target_language="Python",
        operation="NULL_CHECK",
        source_representation="NULL",
        target_representation="None"
    )

    assert result.concept == "CONCEPT_SQL_NULL"
    assert result.compatibility == "CONDITIONALLY_EQUIVALENT"
    assert result.recommended_mapping == "MAP_SQL_NULL_TO_PYTHON_NONE"
    assert any("equality_logic_inversion" in r for r in result.identified_risks)


def test_scenario_cpp_int_div_to_python():
    resolver = setup_resolver()
    result = resolver.resolve(
        source_language="C++",
        target_language="Python",
        operation="INT_DIV",
        source_representation="a / b",
        target_representation="int(a / b)"
    )

    assert result.concept == "CONCEPT_INTEGER_DIVISION"
    assert result.compatibility == "CONDITIONALLY_EQUIVALENT"
    assert result.recommended_mapping == "MAP_CPP_INT_DIV_TO_PYTHON"
    assert "silent_off_by_one_negative_division" in result.identified_risks


def test_scenario_missing_context_returns_uncertain():
    resolver = setup_resolver()
    result = resolver.resolve(
        source_language="UnknownLang",
        target_language="AnotherLang"
    )

    assert result.compatibility == "UNCERTAIN"
    assert len(result.unresolved_assumptions) > 0
    assert result.confidence == "low"
