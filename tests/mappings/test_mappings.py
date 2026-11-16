from pathlib import Path
from services.registry_service import RegistryService
from services.cross_language_mapper import CrossLanguageMapper

root_dir = Path(__file__).resolve().parent.parent.parent


def test_all_mappings_conform_to_rules():
    service = RegistryService(base_dir=root_dir)
    mapper = CrossLanguageMapper(service)
    mappings = mapper.get_all_mappings()

    assert len(mappings) >= 10
    valid_compatibilities = {
        "EQUIVALENT",
        "CONDITIONALLY_EQUIVALENT",
        "CONVERTIBLE",
        "INCOMPATIBLE",
        "UNCERTAIN"
    }

    for m in mappings:
        assert m["mapping_id"].startswith("MAP_")
        assert m["compatibility"] in valid_compatibilities
        assert "conversion" in m
        assert "strategy" in m["conversion"]
        assert len(m.get("preservation_rules", [])) > 0
        assert len(m.get("evidence_sources", [])) > 0
