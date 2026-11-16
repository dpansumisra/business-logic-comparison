from pathlib import Path
from services.schema_validator import RepositoryValidator

root_dir = Path(__file__).resolve().parent.parent.parent


def test_full_repository_schema_validation():
    validator = RepositoryValidator(base_dir=root_dir)
    report = validator.validate_repository()

    assert report.is_valid, f"Validation errors: {report.errors}"
    assert report.validated_files >= 30
    assert report.validated_records >= 50
    assert len(report.errors) == 0
