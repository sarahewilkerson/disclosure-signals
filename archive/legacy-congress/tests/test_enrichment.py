"""Tests for Congress.gov API enrichment module."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cppi.enrichment.congress_gov import (
    CONGRESS_API_BASE,
    CURRENT_CONGRESS,
    CongressGovClient,
    Member,
)


# Sample API response fixtures
SAMPLE_MEMBER_RESPONSE = {
    "members": [
        {
            "bioguideId": "S001191",
            "name": "Sinema, Kyrsten",
            "partyName": "Independent",
            "state": "Arizona",
            "terms": {
                "item": [
                    {"chamber": "Senate", "startYear": 2019}
                ]
            }
        },
        {
            "bioguideId": "P000197",
            "name": "Pelosi, Nancy",
            "partyName": "Democrat",
            "state": "California",
            "district": "11",
            "terms": {
                "item": [
                    {"chamber": "House of Representatives", "startYear": 1987}
                ]
            }
        },
        {
            "bioguideId": "M000355",
            "name": "McConnell, Mitch",
            "partyName": "Republican",
            "state": "Kentucky",
            "terms": {
                "item": [
                    {"chamber": "Senate", "startYear": 1985}
                ]
            }
        },
    ]
}

SAMPLE_MEMBER_DETAIL_RESPONSE = {
    "member": {
        "bioguideId": "S001191",
        "committees": [
            {
                "systemCode": "ssfi00",
                "name": "Finance Committee",
                "chamber": "Senate",
            },
            {
                "systemCode": "sshr00",
                "name": "Homeland Security and Governmental Affairs",
                "chamber": "Senate",
            }
        ]
    }
}

SAMPLE_COMMITTEES_RESPONSE = {
    "committees": [
        {
            "systemCode": "ssfi00",
            "name": "Finance Committee",
            "url": "https://api.congress.gov/v3/committee/senate/ssfi00",
        },
        {
            "systemCode": "ssbk00",
            "name": "Banking, Housing, and Urban Affairs",
            "url": "https://api.congress.gov/v3/committee/senate/ssbk00",
        }
    ]
}


class TestMemberDataclass:
    """Test the Member dataclass."""

    def test_member_creation(self):
        """Test creating a Member instance."""
        member = Member(
            bioguide_id="T001234",
            name="Test, Member",
            party="Democrat",
            state="California",
            chamber="house",
            district="12",
        )
        assert member.bioguide_id == "T001234"
        assert member.name == "Test, Member"
        assert member.party == "Democrat"
        assert member.state == "California"
        assert member.chamber == "house"
        assert member.district == "12"
        assert member.in_office is True
        assert member.committees == []

    def test_member_to_dict(self):
        """Test Member.to_dict() conversion."""
        member = Member(
            bioguide_id="T001234",
            name="Test, Member",
            party="Republican",
            state="Texas",
            chamber="senate",
            committees=[{"code": "ssfi00", "name": "Finance"}],
        )
        d = member.to_dict()

        assert d["bioguide_id"] == "T001234"
        assert d["name"] == "Test, Member"
        assert d["party"] == "Republican"
        assert d["state"] == "Texas"
        assert d["chamber"] == "senate"
        assert d["in_office"] == 1
        assert json.loads(d["committees"]) == [{"code": "ssfi00", "name": "Finance"}]

    def test_member_defaults(self):
        """Test Member default values."""
        member = Member(
            bioguide_id="X000001",
            name="Default, Test",
            party="Independent",
            state="Maine",
            chamber="senate",
        )
        assert member.district is None
        assert member.in_office is True
        assert member.committees == []


class TestCongressGovClient:
    """Test the CongressGovClient."""

    def test_client_initialization(self):
        """Test client initialization with API key."""
        with patch.dict("os.environ", {"CONGRESS_API_KEY": "test_key"}):
            client = CongressGovClient()
            assert client.api_key == "test_key"

    def test_client_initialization_with_explicit_key(self):
        """Test client initialization with explicit API key."""
        client = CongressGovClient(api_key="explicit_key")
        assert client.api_key == "explicit_key"

    def test_client_initialization_no_key(self):
        """Test client initialization without API key warns."""
        with patch.dict("os.environ", {}, clear=True):
            # Remove CONGRESS_API_KEY if it exists
            import os
            os.environ.pop("CONGRESS_API_KEY", None)

            with patch("cppi.enrichment.congress_gov.logger") as mock_logger:
                client = CongressGovClient(api_key=None)
                assert client.api_key is None
                mock_logger.warning.assert_called()

    @patch("requests.Session")
    def test_get_current_members(self, mock_session_class):
        """Test fetching current members."""
        # Setup mock
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_MEMBER_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        # Test
        client = CongressGovClient(api_key="test_key")
        client.session = mock_session
        members = client.get_current_members()

        assert len(members) == 3

        # Check first member (Senate)
        sinema = next(m for m in members if m.bioguide_id == "S001191")
        assert sinema.name == "Sinema, Kyrsten"
        assert sinema.party == "Independent"
        assert sinema.state == "Arizona"
        assert sinema.chamber == "senate"

        # Check second member (House)
        pelosi = next(m for m in members if m.bioguide_id == "P000197")
        assert pelosi.name == "Pelosi, Nancy"
        assert pelosi.party == "Democrat"
        assert pelosi.chamber == "house"

    @patch("requests.Session")
    def test_get_current_members_chamber_filter(self, mock_session_class):
        """Test filtering members by chamber."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_MEMBER_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        client = CongressGovClient(api_key="test_key")
        client.session = mock_session

        # Filter to Senate only
        senate_members = client.get_current_members(chamber="senate")
        assert len(senate_members) == 2
        assert all(m.chamber == "senate" for m in senate_members)

        # Filter to House only
        house_members = client.get_current_members(chamber="house")
        assert len(house_members) == 1
        assert all(m.chamber == "house" for m in house_members)

    @patch("requests.Session")
    def test_get_member_committees(self, mock_session_class):
        """Test fetching member committees."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_MEMBER_DETAIL_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        client = CongressGovClient(api_key="test_key")
        client.session = mock_session
        committees = client.get_member_committees("S001191")

        assert len(committees) == 2
        assert committees[0]["code"] == "ssfi00"
        assert committees[0]["name"] == "Finance Committee"

    @patch("requests.Session")
    def test_get_all_committees(self, mock_session_class):
        """Test fetching all committees."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_COMMITTEES_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        client = CongressGovClient(api_key="test_key")
        client.session = mock_session
        committees = client.get_all_committees()

        # Called 3 times: house, senate, joint
        assert mock_session.get.call_count == 3
        # Returns committees from all chambers
        assert len(committees) == 6  # 2 per chamber * 3 chambers

    def test_cache_operations(self):
        """Test saving and loading from cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = CongressGovClient(api_key="test_key", cache_dir=Path(tmpdir))

            members = [
                Member(
                    bioguide_id="T001234",
                    name="Test, One",
                    party="Democrat",
                    state="California",
                    chamber="house",
                    committees=[{"code": "hsju00", "name": "Judiciary"}],
                ),
                Member(
                    bioguide_id="T005678",
                    name="Test, Two",
                    party="Republican",
                    state="Texas",
                    chamber="senate",
                ),
            ]

            # Save to cache
            client.save_to_cache(members)

            # Load from cache
            loaded = client.load_from_cache()

            assert loaded is not None
            assert len(loaded) == 2
            assert loaded[0].bioguide_id == "T001234"
            assert loaded[0].committees == [{"code": "hsju00", "name": "Judiciary"}]
            assert loaded[1].bioguide_id == "T005678"

    def test_load_from_cache_no_file(self):
        """Test loading from cache when no cache exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            client = CongressGovClient(api_key="test_key", cache_dir=Path(tmpdir))
            loaded = client.load_from_cache()
            assert loaded is None

    @patch("requests.Session")
    def test_rate_limiting(self, mock_session_class):
        """Test that rate limiting is enforced."""
        import time

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {"members": []}
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        client = CongressGovClient(api_key="test_key", rate_limit=0.1)
        client.session = mock_session

        start = time.time()
        client._get("/test1")
        client._get("/test2")
        elapsed = time.time() - start

        # Should have waited at least rate_limit seconds between requests
        assert elapsed >= 0.1

    @patch("requests.Session")
    def test_api_error_handling(self, mock_session_class):
        """Test handling of API errors."""
        import requests

        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Simulate HTTP error
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_session.get.return_value = mock_response

        client = CongressGovClient(api_key="test_key")
        client.session = mock_session

        result = client._get("/test")
        assert result is None

    def test_no_api_key_returns_none(self):
        """Test that requests fail gracefully without API key."""
        import os
        os.environ.pop("CONGRESS_API_KEY", None)

        client = CongressGovClient(api_key=None)
        result = client._get("/test")
        assert result is None


class TestCongressConstants:
    """Test module constants."""

    def test_api_base_url(self):
        """Test API base URL is set correctly."""
        assert CONGRESS_API_BASE == "https://api.congress.gov/v3"

    def test_current_congress(self):
        """Test current Congress number is set."""
        assert CURRENT_CONGRESS == 119  # 119th Congress (2025-2027)
