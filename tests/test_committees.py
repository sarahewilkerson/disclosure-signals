"""Tests for congressional committee membership data."""

from __future__ import annotations

from signals.congress.committees import (
    MemberInfo,
    _normalize_name,
    _parse_committee_yaml,
    check_committee_sector_match,
    get_committee_sectors,
    resolve_filer,
)


def test_normalize_name():
    assert _normalize_name("HON. NANCY PELOSI") == "nancy pelosi"
    assert _normalize_name("Sen. Elizabeth Warren") == "elizabeth warren"
    assert _normalize_name("Rep. Adam Smith") == "adam smith"
    assert _normalize_name("Pelosi, Nancy") == "nancy pelosi"
    assert _normalize_name("Mr. John Smith III") == "john smith iii"
    assert _normalize_name("") is None
    assert _normalize_name(None) is None


def test_parse_committee_yaml():
    yaml_text = """SSBA:
- name: Tim Scott
  party: majority
  rank: 1
  title: Chairman
  bioguide: S001184
- name: Elizabeth Warren
  party: minority
  rank: 2
  bioguide: W000817
HSBA:
- name: French Hill
  party: majority
  rank: 1
  bioguide: H001072
"""
    result = _parse_committee_yaml(yaml_text)
    assert "SSBA" in result
    assert "HSBA" in result
    assert len(result["SSBA"]) == 2
    assert result["SSBA"][0]["bioguide"] == "S001184"
    assert result["SSBA"][0]["title"] == "Chairman"
    assert result["SSBA"][1]["bioguide"] == "W000817"


def test_resolve_filer_exact_match():
    members = [
        MemberInfo("P000197", "Pelosi, Nancy", "Democratic", "California", "House", []),
        MemberInfo("W000817", "Warren, Elizabeth", "Democratic", "Massachusetts", "Senate", []),
    ]
    assert resolve_filer("HON. NANCY PELOSI", "CA", members) == "P000197"
    assert resolve_filer("Senator Warren", "MA", members) == "W000817"


def test_resolve_filer_disambiguation_by_state():
    members = [
        MemberInfo("S001", "Smith, John", None, "Texas", "Senate", []),
        MemberInfo("S002", "Smith, Jane", None, "California", "House", []),
    ]
    # Both have last name Smith, disambiguate by state
    assert resolve_filer("John Smith", "TX", members) == "S001"


def test_resolve_filer_returns_none_for_unknown():
    members = [
        MemberInfo("P000197", "Pelosi, Nancy", "Democratic", "California", "House", []),
    ]
    assert resolve_filer("Unknown Person", None, members) is None


def test_get_committee_sectors():
    committees = [
        {"code": "ssba", "title": None},
        {"code": "ssas", "title": None},
        {"code": "ssba02", "title": None},  # subcommittee inherits parent
    ]
    sectors = get_committee_sectors(committees)
    assert "Financials" in sectors
    assert "Industrials" in sectors


def test_check_committee_sector_match():
    assert check_committee_sector_match(["Financials", "Industrials"], "Financials") is True
    assert check_committee_sector_match(["Financials", "Industrials"], "Technology") is False
    assert check_committee_sector_match([], "Financials") is False
    assert check_committee_sector_match(["Financials"], None) is False
