"""
Committee to economic sector mapping for CPPI.

Maps congressional committee assignments to GICS-like sector categories
to understand member exposure to different economic areas.
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# GICS-like sector categories for congressional context
SECTORS = [
    "Defense",
    "Energy",
    "Financial",
    "Healthcare",
    "Technology",
    "Agriculture",
    "Infrastructure",
    "Consumer",
    "Industrial",
    "Telecommunications",
    "Real Estate",
    "Materials",
    "Utilities",
    "Government",  # General government operations
]

# Committee code prefix to chamber mapping
# House committees start with 'h', Senate with 's', Joint with 'j'
CHAMBER_PREFIXES = {
    "h": "house",
    "s": "senate",
    "j": "joint",
}

# Committee → Sector mapping
# Maps committee system codes (lowercase prefix) to primary and secondary sectors
# Format: {"code_prefix": {"primary": "Sector", "secondary": ["Sector", ...]}}
COMMITTEE_SECTOR_MAP = {
    # ARMED SERVICES - Defense sector
    "ssas": {"primary": "Defense", "secondary": ["Industrial", "Technology"]},
    "hsas": {"primary": "Defense", "secondary": ["Industrial", "Technology"]},
    "hsar": {"primary": "Defense", "secondary": ["Industrial"]},  # House Armed Services subcommittees

    # FINANCE / BANKING - Financial sector
    "ssfi": {"primary": "Financial", "secondary": ["Healthcare"]},  # Senate Finance (also healthcare via Medicare)
    "ssbk": {"primary": "Financial", "secondary": ["Real Estate", "Consumer"]},  # Senate Banking
    "hswm": {"primary": "Financial", "secondary": ["Healthcare"]},  # House Ways and Means
    "hsba": {"primary": "Financial", "secondary": ["Real Estate", "Consumer"]},  # House Financial Services

    # ENERGY - Energy sector
    "sseg": {"primary": "Energy", "secondary": ["Utilities", "Materials"]},  # Senate Energy
    "hsif": {"primary": "Energy", "secondary": ["Telecommunications", "Consumer"]},  # House Energy & Commerce (broad)

    # AGRICULTURE - Agriculture sector
    "ssaf": {"primary": "Agriculture", "secondary": ["Consumer", "Energy"]},  # Senate Agriculture (includes biofuels)
    "hsag": {"primary": "Agriculture", "secondary": ["Consumer", "Energy"]},  # House Agriculture

    # COMMERCE / TECHNOLOGY
    "sscm": {"primary": "Telecommunications", "secondary": ["Consumer", "Technology"]},  # Senate Commerce
    "hssy": {"primary": "Technology", "secondary": ["Defense"]},  # House Science, Space, Technology

    # HEALTH - Healthcare sector
    "sshr": {"primary": "Government", "secondary": ["Healthcare"]},  # Senate HSGAC (health subcommittees)
    "hsed": {"primary": "Healthcare", "secondary": ["Consumer"]},  # House Education and Labor (includes health)
    "hsvr": {"primary": "Healthcare", "secondary": ["Defense"]},  # House Veterans Affairs
    "ssvs": {"primary": "Healthcare", "secondary": ["Defense"]},  # Senate Veterans Affairs

    # INFRASTRUCTURE / TRANSPORTATION
    "ssev": {"primary": "Infrastructure", "secondary": ["Utilities", "Real Estate"]},  # Senate Environment & Public Works
    "hspw": {"primary": "Infrastructure", "secondary": ["Utilities", "Real Estate"]},  # House Transportation

    # APPROPRIATIONS - Broad exposure
    "ssap": {"primary": "Government", "secondary": ["Defense", "Healthcare", "Infrastructure"]},
    "hsap": {"primary": "Government", "secondary": ["Defense", "Healthcare", "Infrastructure"]},

    # JUDICIARY - Technology (antitrust, IP)
    "ssju": {"primary": "Technology", "secondary": ["Telecommunications", "Consumer"]},
    "hsju": {"primary": "Technology", "secondary": ["Telecommunications", "Consumer"]},

    # SMALL BUSINESS - Consumer/Industrial
    "sssb": {"primary": "Consumer", "secondary": ["Industrial"]},
    "hssm": {"primary": "Consumer", "secondary": ["Industrial"]},

    # BUDGET - Government operations
    "ssbu": {"primary": "Government", "secondary": []},
    "hsbu": {"primary": "Government", "secondary": []},

    # INTELLIGENCE - Defense/Technology
    "ssin": {"primary": "Defense", "secondary": ["Technology"]},  # Senate Intelligence
    "hlig": {"primary": "Defense", "secondary": ["Technology"]},  # House Intelligence

    # FOREIGN RELATIONS / AFFAIRS - Defense, Energy
    "ssfr": {"primary": "Defense", "secondary": ["Energy"]},  # Senate Foreign Relations
    "hsfa": {"primary": "Defense", "secondary": ["Energy"]},  # House Foreign Affairs

    # HOMELAND SECURITY
    "hshm": {"primary": "Defense", "secondary": ["Technology", "Infrastructure"]},
    "ssga": {"primary": "Government", "secondary": ["Defense", "Technology"]},  # Senate Homeland Security & Gov Affairs

    # NATURAL RESOURCES
    "hsii": {"primary": "Energy", "secondary": ["Materials", "Real Estate"]},  # House Natural Resources

    # OVERSIGHT
    "hsgo": {"primary": "Government", "secondary": []},  # House Oversight

    # RULES
    "hsru": {"primary": "Government", "secondary": []},
    "ssra": {"primary": "Government", "secondary": []},

    # ETHICS
    "hset": {"primary": "Government", "secondary": []},
    "slet": {"primary": "Government", "secondary": []},

    # JOINT COMMITTEES
    "jstx": {"primary": "Financial", "secondary": []},  # Joint Taxation
    "jsec": {"primary": "Government", "secondary": []},  # Joint Economic
    "jspr": {"primary": "Government", "secondary": []},  # Joint Printing
    "jsli": {"primary": "Government", "secondary": []},  # Joint Library
}


@dataclass
class SectorExposure:
    """Represents a member's exposure to an economic sector."""

    sector: str
    score: float  # 0-1, indicates strength of exposure
    source_committees: list[str]  # Committee codes contributing to this exposure

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "sector": self.sector,
            "score": self.score,
            "source_committees": self.source_committees,
        }


def get_committee_sectors(committee_code: str) -> Optional[dict]:
    """
    Get sector mapping for a committee code.

    Args:
        committee_code: Committee system code (e.g., "ssfi00", "hsba")

    Returns:
        Dict with "primary" and "secondary" sectors, or None if not mapped
    """
    # Normalize to lowercase
    code = committee_code.lower()

    # Try exact match first
    if code in COMMITTEE_SECTOR_MAP:
        return COMMITTEE_SECTOR_MAP[code]

    # Try prefix match (committee codes often have numeric suffixes for subcommittees)
    # e.g., "ssfi00" -> "ssfi", "hsba01" -> "hsba"
    for prefix in COMMITTEE_SECTOR_MAP:
        if code.startswith(prefix):
            return COMMITTEE_SECTOR_MAP[prefix]

    # Try 4-character prefix (common pattern)
    if len(code) >= 4:
        prefix = code[:4]
        if prefix in COMMITTEE_SECTOR_MAP:
            return COMMITTEE_SECTOR_MAP[prefix]

    logger.debug(f"No sector mapping found for committee code: {committee_code}")
    return None


def compute_member_sector_exposures(
    committees: list[dict],
    primary_weight: float = 1.0,
    secondary_weight: float = 0.5,
) -> list[SectorExposure]:
    """
    Compute sector exposures for a member based on their committee assignments.

    Args:
        committees: List of committee dicts with "code" or "systemCode" field
        primary_weight: Weight for primary sector assignments (default 1.0)
        secondary_weight: Weight for secondary sector assignments (default 0.5)

    Returns:
        List of SectorExposure objects, sorted by score descending
    """
    # Track exposure per sector
    sector_scores: dict[str, float] = {}
    sector_sources: dict[str, list[str]] = {}

    for committee in committees:
        # Get committee code from various possible field names
        code = committee.get("code") or committee.get("systemCode") or ""
        if not code:
            continue

        # Get sector mapping
        mapping = get_committee_sectors(code)
        if not mapping:
            continue

        # Add primary sector exposure
        primary = mapping.get("primary")
        if primary:
            sector_scores[primary] = sector_scores.get(primary, 0) + primary_weight
            if primary not in sector_sources:
                sector_sources[primary] = []
            sector_sources[primary].append(code)

        # Add secondary sector exposures
        for secondary in mapping.get("secondary", []):
            sector_scores[secondary] = sector_scores.get(secondary, 0) + secondary_weight
            if secondary not in sector_sources:
                sector_sources[secondary] = []
            sector_sources[secondary].append(code)

    # Normalize scores to 0-1 range
    if sector_scores:
        max_score = max(sector_scores.values())
        if max_score > 0:
            for sector in sector_scores:
                sector_scores[sector] /= max_score

    # Build exposure list
    exposures = []
    for sector, score in sector_scores.items():
        exposures.append(
            SectorExposure(
                sector=sector,
                score=round(score, 3),
                source_committees=sector_sources.get(sector, []),
            )
        )

    # Sort by score descending
    exposures.sort(key=lambda x: x.score, reverse=True)

    return exposures


def get_all_sectors() -> list[str]:
    """Return list of all defined sectors."""
    return SECTORS.copy()


def get_sector_for_committee(committee_code: str) -> Optional[str]:
    """
    Get the primary sector for a committee.

    Args:
        committee_code: Committee system code

    Returns:
        Primary sector name, or None if not mapped
    """
    mapping = get_committee_sectors(committee_code)
    if mapping:
        return mapping.get("primary")
    return None
