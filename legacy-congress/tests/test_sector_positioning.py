"""Tests for sector positioning computation."""

from datetime import datetime

import pytest

from cppi.scoring import (
    ScoredTransaction,
    SectorPositioning,
    compute_sector_positioning,
)


def make_scored_transaction(
    member_id: str,
    final_score: float,
    ticker: str = "TEST",
) -> ScoredTransaction:
    """Helper to create a scored transaction for testing."""
    return ScoredTransaction(
        member_id=member_id,
        ticker=ticker,
        transaction_type="purchase" if final_score > 0 else "sale",
        execution_date=datetime.now(),
        amount_min=1000,
        amount_max=15000,
        owner_type="self",
        base_value=abs(final_score),
        direction=1.0 if final_score > 0 else -1.0,
        staleness_penalty=1.0,
        owner_weight=1.0,
        resolution_confidence=1.0,
        signal_weight=1.0,
        raw_score=final_score,
        final_score=final_score,
    )


class TestSectorPositioning:
    """Test the SectorPositioning dataclass."""

    def test_sector_positioning_creation(self):
        """Test creating a SectorPositioning instance."""
        sp = SectorPositioning(
            sector="Defense",
            breadth_pct=0.5,
            buyers=3,
            sellers=1,
            volume_net=50000.0,
            volume_buy=75000.0,
            volume_sell=25000.0,
            member_count=4,
            total_exposure=3.5,
        )
        assert sp.sector == "Defense"
        assert sp.breadth_pct == 0.5
        assert sp.buyers == 3
        assert sp.sellers == 1

    def test_sector_positioning_to_dict(self):
        """Test SectorPositioning.to_dict() conversion."""
        sp = SectorPositioning(
            sector="Financial",
            breadth_pct=0.25,
            buyers=5,
            sellers=3,
            volume_net=10000.0,
            volume_buy=15000.0,
            volume_sell=5000.0,
            member_count=8,
            total_exposure=6.0,
        )
        d = sp.to_dict()
        assert d["sector"] == "Financial"
        assert d["breadth_pct"] == 0.25
        assert d["buyers"] == 5
        assert d["member_count"] == 8


class TestComputeSectorPositioning:
    """Test sector positioning computation."""

    def test_empty_transactions(self):
        """Test empty transaction list returns empty result."""
        result = compute_sector_positioning([], {})
        assert result == []

    def test_no_sector_exposures(self):
        """Test transactions without sector exposures are skipped."""
        txns = [
            make_scored_transaction("M001", 1000.0),
            make_scored_transaction("M002", -500.0),
        ]
        # Empty sector exposures
        result = compute_sector_positioning(txns, {})
        assert result == []

    def test_single_member_single_sector(self):
        """Test single member with single sector exposure."""
        txns = [make_scored_transaction("M001", 1000.0)]
        exposures = {
            "M001": [{"sector": "Defense", "score": 1.0}]
        }

        result = compute_sector_positioning(txns, exposures)

        assert len(result) == 1
        assert result[0].sector == "Defense"
        assert result[0].volume_net == 1000.0
        assert result[0].buyers == 1
        assert result[0].sellers == 0
        assert result[0].breadth_pct == 1.0

    def test_single_member_multiple_sectors(self):
        """Test single member with multiple sector exposures."""
        txns = [make_scored_transaction("M001", 1000.0)]
        exposures = {
            "M001": [
                {"sector": "Defense", "score": 0.6},
                {"sector": "Technology", "score": 0.4},
            ]
        }

        result = compute_sector_positioning(txns, exposures)

        # Should have two sectors
        assert len(result) == 2

        # Find Defense and Technology results
        defense = next(r for r in result if r.sector == "Defense")
        tech = next(r for r in result if r.sector == "Technology")

        # Position split proportionally (60/40)
        assert abs(defense.volume_net - 600.0) < 0.1
        assert abs(tech.volume_net - 400.0) < 0.1

    def test_multiple_members_same_sector(self):
        """Test multiple members contributing to same sector."""
        txns = [
            make_scored_transaction("M001", 1000.0),  # Buyer
            make_scored_transaction("M002", -500.0),  # Seller
        ]
        exposures = {
            "M001": [{"sector": "Defense", "score": 1.0}],
            "M002": [{"sector": "Defense", "score": 1.0}],
        }

        result = compute_sector_positioning(txns, exposures)

        assert len(result) == 1
        assert result[0].sector == "Defense"
        assert result[0].volume_net == 500.0  # 1000 - 500
        assert result[0].volume_buy == 1000.0
        assert result[0].volume_sell == 500.0
        assert result[0].buyers == 1
        assert result[0].sellers == 1
        assert result[0].member_count == 2
        assert result[0].breadth_pct == 0.0  # 1-1 / 2 = 0

    def test_multiple_members_different_sectors(self):
        """Test members in different sectors."""
        txns = [
            make_scored_transaction("M001", 1000.0),
            make_scored_transaction("M002", 2000.0),
        ]
        exposures = {
            "M001": [{"sector": "Defense", "score": 1.0}],
            "M002": [{"sector": "Financial", "score": 1.0}],
        }

        result = compute_sector_positioning(txns, exposures)

        # Should have two separate sectors
        assert len(result) == 2

        sectors = {r.sector for r in result}
        assert sectors == {"Defense", "Financial"}

    def test_sorted_by_volume(self):
        """Test results are sorted by absolute volume."""
        txns = [
            make_scored_transaction("M001", 100.0),
            make_scored_transaction("M002", 1000.0),
            make_scored_transaction("M003", 500.0),
        ]
        exposures = {
            "M001": [{"sector": "Defense", "score": 1.0}],
            "M002": [{"sector": "Financial", "score": 1.0}],
            "M003": [{"sector": "Technology", "score": 1.0}],
        }

        result = compute_sector_positioning(txns, exposures)

        # Should be sorted by absolute volume: Financial, Technology, Defense
        assert result[0].sector == "Financial"
        assert result[1].sector == "Technology"
        assert result[2].sector == "Defense"

    def test_seller_member(self):
        """Test seller member properly attributed."""
        txns = [make_scored_transaction("M001", -1000.0)]  # Seller
        exposures = {
            "M001": [{"sector": "Energy", "score": 1.0}]
        }

        result = compute_sector_positioning(txns, exposures)

        assert len(result) == 1
        assert result[0].sector == "Energy"
        assert result[0].volume_net == -1000.0
        assert result[0].volume_sell == 1000.0
        assert result[0].volume_buy == 0.0
        assert result[0].buyers == 0
        assert result[0].sellers == 1
        assert result[0].breadth_pct == -1.0

    def test_mixed_members_mixed_sectors(self):
        """Test complex scenario with multiple members and sectors."""
        txns = [
            make_scored_transaction("M001", 1000.0),
            make_scored_transaction("M002", -800.0),
            make_scored_transaction("M003", 500.0),
        ]
        exposures = {
            "M001": [
                {"sector": "Defense", "score": 0.8},
                {"sector": "Technology", "score": 0.2},
            ],
            "M002": [
                {"sector": "Defense", "score": 0.5},
                {"sector": "Financial", "score": 0.5},
            ],
            "M003": [
                {"sector": "Technology", "score": 1.0},
            ],
        }

        result = compute_sector_positioning(txns, exposures)

        # Should have Defense, Technology, and Financial
        sectors = {r.sector for r in result}
        assert sectors == {"Defense", "Technology", "Financial"}

        # Defense: M001 (+800) + M002 (-400) = +400
        defense = next(r for r in result if r.sector == "Defense")
        assert abs(defense.volume_net - 400.0) < 0.1

        # Technology: M001 (+200) + M003 (+500) = +700
        tech = next(r for r in result if r.sector == "Technology")
        assert abs(tech.volume_net - 700.0) < 0.1

        # Financial: M002 (-400)
        fin = next(r for r in result if r.sector == "Financial")
        assert abs(fin.volume_net - (-400.0)) < 0.1

    def test_exposure_dict_format(self):
        """Test with exposure dict format from sector_mapping."""
        txns = [make_scored_transaction("M001", 1000.0)]
        # Format returned by compute_member_sector_exposures().to_dict()
        exposures = {
            "M001": [
                {
                    "sector": "Defense",
                    "score": 0.75,
                    "source_committees": ["ssas00"],
                },
            ]
        }

        result = compute_sector_positioning(txns, exposures)

        assert len(result) == 1
        assert result[0].sector == "Defense"
