"""Tests for committee-to-sector mapping module."""

import pytest

from cppi.enrichment.sector_mapping import (
    COMMITTEE_SECTOR_MAP,
    SECTORS,
    SectorExposure,
    compute_member_sector_exposures,
    get_all_sectors,
    get_committee_sectors,
    get_sector_for_committee,
)


class TestSectorExposure:
    """Test the SectorExposure dataclass."""

    def test_sector_exposure_creation(self):
        """Test creating a SectorExposure instance."""
        exposure = SectorExposure(
            sector="Defense",
            score=0.8,
            source_committees=["ssas00", "hsar01"],
        )
        assert exposure.sector == "Defense"
        assert exposure.score == 0.8
        assert exposure.source_committees == ["ssas00", "hsar01"]

    def test_sector_exposure_to_dict(self):
        """Test SectorExposure.to_dict() conversion."""
        exposure = SectorExposure(
            sector="Financial",
            score=1.0,
            source_committees=["ssfi00"],
        )
        d = exposure.to_dict()
        assert d["sector"] == "Financial"
        assert d["score"] == 1.0
        assert d["source_committees"] == ["ssfi00"]


class TestCommitteeSectorMapping:
    """Test committee to sector mapping functions."""

    def test_get_committee_sectors_exact_match(self):
        """Test exact match for committee code."""
        mapping = get_committee_sectors("ssfi")
        assert mapping is not None
        assert mapping["primary"] == "Financial"
        assert "Healthcare" in mapping["secondary"]

    def test_get_committee_sectors_with_suffix(self):
        """Test matching committee code with numeric suffix."""
        mapping = get_committee_sectors("ssfi00")
        assert mapping is not None
        assert mapping["primary"] == "Financial"

    def test_get_committee_sectors_case_insensitive(self):
        """Test case insensitivity."""
        mapping = get_committee_sectors("SSFI")
        assert mapping is not None
        assert mapping["primary"] == "Financial"

    def test_get_committee_sectors_unknown(self):
        """Test unknown committee returns None."""
        mapping = get_committee_sectors("xxxx")
        assert mapping is None

    def test_get_sector_for_committee(self):
        """Test getting primary sector for committee."""
        assert get_sector_for_committee("ssas") == "Defense"
        assert get_sector_for_committee("ssfi") == "Financial"
        assert get_sector_for_committee("sseg") == "Energy"
        assert get_sector_for_committee("xxxx") is None

    def test_armed_services_maps_to_defense(self):
        """Test Armed Services committees map to Defense."""
        senate = get_committee_sectors("ssas")
        house = get_committee_sectors("hsas")

        assert senate["primary"] == "Defense"
        assert house["primary"] == "Defense"
        assert "Industrial" in senate["secondary"]
        assert "Technology" in senate["secondary"]

    def test_finance_banking_maps_to_financial(self):
        """Test Finance/Banking committees map to Financial."""
        senate_finance = get_committee_sectors("ssfi")
        senate_banking = get_committee_sectors("ssbk")
        house_ways_means = get_committee_sectors("hswm")
        house_financial = get_committee_sectors("hsba")

        assert senate_finance["primary"] == "Financial"
        assert senate_banking["primary"] == "Financial"
        assert house_ways_means["primary"] == "Financial"
        assert house_financial["primary"] == "Financial"

    def test_agriculture_maps_correctly(self):
        """Test Agriculture committees map to Agriculture."""
        senate = get_committee_sectors("ssaf")
        house = get_committee_sectors("hsag")

        assert senate["primary"] == "Agriculture"
        assert house["primary"] == "Agriculture"


class TestComputeMemberSectorExposures:
    """Test sector exposure computation."""

    def test_empty_committees(self):
        """Test empty committee list returns empty exposures."""
        exposures = compute_member_sector_exposures([])
        assert exposures == []

    def test_single_committee(self):
        """Test single committee exposure."""
        committees = [{"code": "ssas00"}]
        exposures = compute_member_sector_exposures(committees)

        assert len(exposures) > 0
        # Defense should be the highest (primary)
        assert exposures[0].sector == "Defense"
        assert exposures[0].score == 1.0

    def test_multiple_committees_same_sector(self):
        """Test multiple committees reinforcing same sector."""
        committees = [
            {"code": "ssas"},  # Armed Services - Defense primary
            {"code": "hlig"},  # Intelligence - Defense primary
        ]
        exposures = compute_member_sector_exposures(committees)

        # Find Defense exposure
        defense = next(e for e in exposures if e.sector == "Defense")
        assert defense.score == 1.0
        assert len(defense.source_committees) == 2

    def test_multiple_committees_different_sectors(self):
        """Test multiple committees with different sectors."""
        committees = [
            {"code": "ssfi"},  # Finance - Financial primary
            {"code": "ssas"},  # Armed Services - Defense primary
        ]
        exposures = compute_member_sector_exposures(committees)

        # Should have both Financial and Defense at top
        sectors = [e.sector for e in exposures[:2]]
        assert "Financial" in sectors
        assert "Defense" in sectors

    def test_secondary_sector_weight(self):
        """Test secondary sectors have lower weight."""
        committees = [
            {"code": "ssas00"},  # Defense primary, Industrial/Technology secondary
        ]
        exposures = compute_member_sector_exposures(
            committees, primary_weight=1.0, secondary_weight=0.5
        )

        # Defense should have higher score than secondary sectors
        defense = next(e for e in exposures if e.sector == "Defense")
        industrial = next((e for e in exposures if e.sector == "Industrial"), None)

        assert defense.score > industrial.score if industrial else True

    def test_system_code_field_name(self):
        """Test using systemCode field name (Congress.gov format)."""
        committees = [{"systemCode": "ssfi00"}]
        exposures = compute_member_sector_exposures(committees)

        assert len(exposures) > 0
        assert exposures[0].sector == "Financial"

    def test_exposures_sorted_by_score(self):
        """Test exposures are sorted by score descending."""
        committees = [
            {"code": "ssfi"},
            {"code": "ssas"},
            {"code": "sseg"},
        ]
        exposures = compute_member_sector_exposures(committees)

        scores = [e.score for e in exposures]
        assert scores == sorted(scores, reverse=True)

    def test_unmapped_committees_ignored(self):
        """Test unmapped committees don't cause errors."""
        committees = [
            {"code": "ssfi"},  # Mapped
            {"code": "unknown"},  # Not mapped
            {"code": ""},  # Empty
        ]
        exposures = compute_member_sector_exposures(committees)

        # Should only have exposures from ssfi
        assert len(exposures) > 0


class TestSectorConstants:
    """Test module constants."""

    def test_sectors_defined(self):
        """Test that sectors are defined."""
        assert len(SECTORS) > 0
        assert "Defense" in SECTORS
        assert "Financial" in SECTORS
        assert "Healthcare" in SECTORS
        assert "Technology" in SECTORS

    def test_get_all_sectors(self):
        """Test get_all_sectors returns copy of sectors."""
        sectors = get_all_sectors()
        assert sectors == SECTORS
        # Verify it's a copy
        sectors.append("Test")
        assert "Test" not in SECTORS

    def test_committee_map_has_major_committees(self):
        """Test that major committees are mapped."""
        # Armed Services
        assert "ssas" in COMMITTEE_SECTOR_MAP
        assert "hsas" in COMMITTEE_SECTOR_MAP

        # Finance/Banking
        assert "ssfi" in COMMITTEE_SECTOR_MAP
        assert "ssbk" in COMMITTEE_SECTOR_MAP

        # Appropriations
        assert "ssap" in COMMITTEE_SECTOR_MAP
        assert "hsap" in COMMITTEE_SECTOR_MAP

        # Intelligence
        assert "ssin" in COMMITTEE_SECTOR_MAP
        assert "hlig" in COMMITTEE_SECTOR_MAP

    def test_committee_map_structure(self):
        """Test committee map entries have correct structure."""
        for code, mapping in COMMITTEE_SECTOR_MAP.items():
            assert "primary" in mapping, f"Missing primary for {code}"
            assert "secondary" in mapping, f"Missing secondary for {code}"
            assert mapping["primary"] in SECTORS, f"Invalid primary sector for {code}"
            for sec in mapping["secondary"]:
                assert sec in SECTORS, f"Invalid secondary sector {sec} for {code}"
