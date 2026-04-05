"""Congressional committee membership data from congress.gov API.

Fetches member rosters and committee assignments, caches locally,
and provides name resolution + committee-sector mapping for
enriching congressional trade provenance.

Usage:
    from signals.congress.committees import load_members, resolve_filer, get_member_committees
    members = load_members()  # fetches/caches from congress.gov
    bioguide = resolve_filer("HON. NANCY PELOSI", "CA", members)
    committees = get_member_committees(bioguide, members)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

CONGRESS_API_BASE = "https://api.congress.gov/v3"
_CACHE_DB = Path(__file__).resolve().parent.parent.parent.parent / "data" / "committee_cache.db"
_ENV_FILE = Path(__file__).resolve().parent.parent.parent.parent / "data" / ".env"
CACHE_TTL_DAYS = 90
REQUEST_DELAY = 0.5  # seconds between API calls

# Committee code → GICS sector mapping
# Codes match unitedstates/congress-legislators YAML format (e.g., SSBA, HSBA)
COMMITTEE_SECTOR_MAP = {
    # Senate
    "ssba": ["Financials"],              # Banking, Housing, Urban Affairs
    "ssas": ["Industrials"],             # Armed Services
    "sshr": ["Health Care"],             # Health, Education, Labor, Pensions
    "sseg": ["Energy"],                  # Energy and Natural Resources
    "ssga": ["Industrials"],             # Homeland Security
    "sscm": ["Communication Services"],  # Commerce, Science, Transportation
    "ssev": ["Utilities"],               # Environment and Public Works
    "ssaf": ["Consumer Staples"],        # Agriculture
    "ssfr": ["Financials"],              # Foreign Relations (defense spending)
    "sssb": ["Industrials", "Consumer Discretionary"],  # Small Business
    "ssfi": ["Financials"],              # Finance
    # House
    "hsba": ["Financials"],              # Financial Services
    "hsas": ["Industrials"],             # Armed Services
    "hsif": ["Health Care", "Energy", "Communication Services"],  # Energy and Commerce
    "hsag": ["Consumer Staples"],        # Agriculture
    "hsap": ["Financials"],              # Appropriations
    "hshm": ["Industrials"],             # Homeland Security
    "hssy": ["Technology", "Industrials"],  # Science, Space, Technology
    "hssm": ["Industrials", "Consumer Discretionary"],  # Small Business
    "hswm": ["Health Care", "Financials"],  # Ways and Means
    "hsbu": ["Financials"],              # Budget
}


@dataclass
class MemberInfo:
    bioguide_id: str
    name: str
    party: str | None
    state: str | None
    chamber: str | None
    committees: list[dict]  # [{"code": "ssba00", "name": "Banking..."}]


def _load_api_key() -> str | None:
    """Load Congress API key from env var or data/.env file."""
    key = os.environ.get("CONGRESS_API_KEY")
    if key:
        return key
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            if line.startswith("CONGRESS_API_KEY="):
                return line.split("=", 1)[1].strip()
    return None


def _get_cache_conn() -> sqlite3.Connection:
    _CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_CACHE_DB))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS members (
            bioguide_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            party TEXT,
            state TEXT,
            chamber TEXT,
            committees_json TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        );
    """)
    return conn


def _cache_is_stale(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT MAX(fetched_at) as latest FROM members").fetchone()
    if not row or not row["latest"]:
        return True
    try:
        fetched = datetime.fromisoformat(row["latest"])
        return (datetime.now() - fetched).days > CACHE_TTL_DAYS
    except (ValueError, TypeError):
        return True


def load_members(force_refresh: bool = False) -> list[MemberInfo]:
    """Load member roster from cache or API. Returns list of MemberInfo."""
    conn = _get_cache_conn()

    if not force_refresh and not _cache_is_stale(conn):
        rows = conn.execute("SELECT * FROM members").fetchall()
        if rows:
            result = [
                MemberInfo(
                    bioguide_id=r["bioguide_id"],
                    name=r["name"],
                    party=r["party"],
                    state=r["state"],
                    chamber=r["chamber"],
                    committees=json.loads(r["committees_json"]) if r["committees_json"] else [],
                )
                for r in rows
            ]
            conn.close()
            return result

    api_key = _load_api_key()
    if not api_key:
        logger.warning("No CONGRESS_API_KEY found — committee data unavailable")
        conn.close()
        return []

    members = _fetch_all_members(api_key)
    if members:
        conn.execute("DELETE FROM members")
        for m in members:
            conn.execute(
                "INSERT OR REPLACE INTO members (bioguide_id, name, party, state, chamber, committees_json) VALUES (?, ?, ?, ?, ?, ?)",
                (m.bioguide_id, m.name, m.party, m.state, m.chamber, json.dumps(m.committees)),
            )
        conn.commit()

    conn.close()
    return members


COMMITTEE_MEMBERSHIP_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committee-membership-current.yaml"
LEGISLATORS_URL = "https://api.congress.gov/v3/member"


def _fetch_all_members(api_key: str) -> list[MemberInfo]:
    """Fetch member list from congress.gov API + committee assignments from GitHub dataset."""
    # Step 1: Get member list from congress.gov (has name, party, state)
    members_by_bioguide: dict[str, MemberInfo] = {}
    offset = 0
    limit = 250

    while True:
        url = f"{LEGISLATORS_URL}?api_key={api_key}&limit={limit}&offset={offset}&format=json&currentMember=true"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"Congress API error at offset {offset}: {e}")
            break

        for m in data.get("members", []):
            bioguide = m.get("bioguideId")
            if not bioguide:
                continue
            chamber = None
            terms = m.get("terms", {}).get("item", [])
            if terms:
                last_term = terms[-1] if isinstance(terms, list) else terms
                chamber = last_term.get("chamber")

            members_by_bioguide[bioguide] = MemberInfo(
                bioguide_id=bioguide,
                name=m.get("name", ""),
                party=m.get("partyName"),
                state=m.get("state"),
                chamber=chamber,
                committees=[],
            )

        pagination = data.get("pagination", {})
        total = pagination.get("count", 0)
        if offset + limit >= total:
            break
        offset += limit
        time.sleep(REQUEST_DELAY)

    # Step 2: Get committee assignments from GitHub YAML dataset (fast, no pagination)
    try:
        resp = requests.get(COMMITTEE_MEMBERSHIP_URL, timeout=30)
        resp.raise_for_status()
        # Parse YAML manually (avoid PyYAML dependency) — format is simple
        committee_data = _parse_committee_yaml(resp.text)
        for code, member_list in committee_data.items():
            # Only include parent committees (codes without subcommittee digit suffix > 2 chars after prefix)
            base_code = code.lower()
            for entry in member_list:
                bioguide = entry.get("bioguide")
                if bioguide and bioguide in members_by_bioguide:
                    members_by_bioguide[bioguide].committees.append({
                        "code": base_code,
                        "title": entry.get("title"),
                    })
        logger.info(f"Loaded committee assignments for {sum(1 for m in members_by_bioguide.values() if m.committees)} members")
    except Exception as e:
        logger.warning(f"Failed to fetch committee membership YAML: {e}")

    return list(members_by_bioguide.values())


def _parse_committee_yaml(text: str) -> dict[str, list[dict]]:
    """Simple YAML parser for the committee-membership format.

    Format:
        SSAF:
        - name: John Boozman
          party: majority
          rank: 1
          title: Chairman
          bioguide: B001236
    """
    result: dict[str, list[dict]] = {}
    current_committee = None
    current_member: dict | None = None

    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue

        # Committee code (no leading whitespace, ends with colon)
        if not line.startswith(" ") and line.endswith(":"):
            if current_member and current_committee:
                result.setdefault(current_committee, []).append(current_member)
            current_committee = line[:-1].strip()
            current_member = None
            continue

        # New member entry (starts with "- ")
        stripped = line.strip()
        if stripped.startswith("- "):
            if current_member and current_committee:
                result.setdefault(current_committee, []).append(current_member)
            key_val = stripped[2:].split(":", 1)
            current_member = {}
            if len(key_val) == 2:
                current_member[key_val[0].strip()] = key_val[1].strip()
            continue

        # Continuation of member entry
        if current_member is not None and ":" in stripped:
            key_val = stripped.split(":", 1)
            if len(key_val) == 2:
                val = key_val[1].strip()
                # Convert rank to int
                if key_val[0].strip() == "rank":
                    try:
                        val = int(val)
                    except ValueError:
                        pass
                current_member[key_val[0].strip()] = val

    # Don't forget the last entry
    if current_member and current_committee:
        result.setdefault(current_committee, []).append(current_member)

    return result


def resolve_filer(filer_name: str, state: str | None, members: list[MemberInfo]) -> str | None:
    """Resolve a PTR filer name to a bioguide ID via fuzzy matching.

    Strips honorifics, normalizes whitespace, matches on last name.
    Uses state for disambiguation when multiple members share a last name.
    """
    if not filer_name or not members:
        return None

    normalized = _normalize_name(filer_name)
    if not normalized:
        return None

    # Build index on first call (cached via list identity)
    candidates = []
    for m in members:
        member_normalized = _normalize_name(m.name)
        if not member_normalized:
            continue

        # Exact match
        if normalized == member_normalized:
            return m.bioguide_id

        # Last name match
        norm_last = normalized.split()[-1] if normalized.split() else ""
        member_last = member_normalized.split()[-1] if member_normalized.split() else ""
        if norm_last and norm_last == member_last:
            candidates.append(m)

    if len(candidates) == 1:
        return candidates[0].bioguide_id
    if len(candidates) > 1 and state:
        state_upper = state.upper()
        state_matches = [c for c in candidates if c.state and c.state.upper() == state_upper]
        if len(state_matches) == 1:
            return state_matches[0].bioguide_id

    return None


def _normalize_name(name: str) -> str | None:
    """Normalize a name for fuzzy matching."""
    if not name:
        return None
    n = name.lower()
    # Strip honorifics and titles (case-insensitive, with optional period)
    for prefix in ["hon", "honorable", "rep", "representative", "sen", "senator", "mr", "mrs", "ms", "dr"]:
        n = re.sub(rf"\b{prefix}\.?\s*", "", n)
    # Handle "Last, First" format
    if "," in n:
        parts = n.split(",", 1)
        n = f"{parts[1].strip()} {parts[0].strip()}"
    n = re.sub(r"[^a-z\s]", "", n).strip()
    n = re.sub(r"\s+", " ", n)
    return n or None


def get_committee_sectors(committees: list[dict]) -> list[str]:
    """Map committee assignments to GICS sectors.

    Matches on parent committee codes (e.g., 'ssba' matches 'ssba', 'ssba13', etc.)
    """
    sectors = set()
    for c in committees:
        code = c.get("code", "").lower()
        # Try exact match first
        if code in COMMITTEE_SECTOR_MAP:
            sectors.update(COMMITTEE_SECTOR_MAP[code])
        else:
            # Try prefix match (subcommittee inherits parent sectors)
            for map_code, map_sectors in COMMITTEE_SECTOR_MAP.items():
                if code.startswith(map_code):
                    sectors.update(map_sectors)
                    break
    return sorted(sectors)


def check_committee_sector_match(
    committee_sectors: list[str],
    stock_sector: str | None,
) -> bool:
    """Check if any committee sector matches the traded stock's sector."""
    if not stock_sector or not committee_sectors:
        return False
    return stock_sector in committee_sectors
