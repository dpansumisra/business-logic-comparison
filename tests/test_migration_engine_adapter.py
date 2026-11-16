from pathlib import Path
from services.migration_engine_adapter import MigrationEngineAdapter

root_dir = Path(__file__).resolve().parent.parent


def test_adapter_detects_naive_date_addition_flaw():
    adapter = MigrationEngineAdapter()

    # Scenario: SAS DATA step `new_date = old_date + 30;` migrated naively to Python `new_date = old_date + 30`
    result = adapter.analyze_semantic_operation(
        source_language="SAS",
        target_language="Python",
        operation="DATE_CONVERSION",
        source_expression="new_date = old_date + 30",
        target_expression="new_date = old_date + 30",
        source_type="numeric_date",
        target_type="datetime.date"
    )

    assert result["knowledge_assisted"] is True
    assert result["needs_correction"] is True
    assert "timedelta" in result["correction_reason"]
    assert "datetime.timedelta(days=int(sas_date))" in result["recommended_target_code"]
    assert "### Universal Knowledge Repository Guidance" in result["llm_prompt_guidance"]


def test_adapter_bypass_flag():
    adapter = MigrationEngineAdapter(enabled=False)
    result = adapter.analyze_semantic_operation(
        source_language="SAS",
        target_language="Python",
        operation="DATE_CONVERSION",
        source_expression="x = y + 1",
        target_expression="x = y + 1"
    )

    assert result["knowledge_assisted"] is False
    assert result["compatibility"] == "BYPASS"
