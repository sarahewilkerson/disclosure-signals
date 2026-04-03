"""
Congress.gov API client for member and committee data.

API Documentation: https://api.congress.gov/
Requires API key from https://api.data.gov/signup/
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

from cppi.config import CACHE_DIR

logger = logging.getLogger(__name__)

# API configuration
CONGRESS_API_BASE = "https://api.congress.gov/v3"
DEFAULT_RATE_LIMIT = 0.5  # seconds between requests
CURRENT_CONGRESS = 119  # 119th Congress (2025-2027)


@dataclass
class Member:
    """Represents a member of Congress."""

    bioguide_id: str
    name: str
    party: str
    state: str
    chamber: str  # 'house' | 'senate'
    district: Optional[str] = None
    in_office: bool = True
    committees: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        return {
            "bioguide_id": self.bioguide_id,
            "name": self.name,
            "party": self.party,
            "state": self.state,
            "chamber": self.chamber,
            "in_office": 1 if self.in_office else 0,
            "committees": json.dumps(self.committees) if self.committees else None,
        }


class CongressGovClient:
    """Client for Congress.gov API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: Optional[Path] = None,
        rate_limit: float = DEFAULT_RATE_LIMIT,
    ):
        """
        Initialize the Congress.gov API client.

        Args:
            api_key: API key from api.data.gov. If not provided, reads from
                     CONGRESS_API_KEY environment variable.
            cache_dir: Directory for caching API responses.
            rate_limit: Minimum seconds between API requests.
        """
        self.api_key = api_key or os.getenv("CONGRESS_API_KEY")
        if not self.api_key:
            logger.warning(
                "No Congress.gov API key provided. Set CONGRESS_API_KEY environment "
                "variable or pass api_key parameter. Get a key at https://api.data.gov/signup/"
            )

        self.cache_dir = Path(cache_dir or CACHE_DIR) / "congress_gov"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.rate_limit = rate_limit
        self._last_request_time: float = 0

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
            }
        )

    def _rate_limit_wait(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """
        Make a rate-limited GET request to the API.

        Args:
            endpoint: API endpoint (e.g., "/member")
            params: Query parameters

        Returns:
            JSON response as dict, or None if request failed
        """
        if not self.api_key:
            logger.error("Cannot make API request without API key")
            return None

        self._rate_limit_wait()

        url = f"{CONGRESS_API_BASE}{endpoint}"
        request_params = {"api_key": self.api_key, "format": "json"}
        if params:
            request_params.update(params)

        logger.debug(f"GET {url}")

        try:
            response = self.session.get(url, params=request_params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited by Congress.gov API, waiting...")
                time.sleep(60)  # Wait 1 minute on rate limit
                return self._get(endpoint, params)  # Retry
            logger.error(f"HTTP error fetching {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching {url}: {e}")
            return None

    def _get_paginated(
        self, endpoint: str, params: Optional[dict] = None, max_results: int = 1000
    ) -> list:
        """
        Fetch paginated results from the API.

        Args:
            endpoint: API endpoint
            params: Query parameters
            max_results: Maximum total results to fetch

        Returns:
            List of all items from paginated responses
        """
        all_items = []
        offset = 0
        limit = 250  # API maximum per page

        while len(all_items) < max_results:
            request_params = {"offset": offset, "limit": limit}
            if params:
                request_params.update(params)

            data = self._get(endpoint, request_params)
            if not data:
                break

            # Extract items from response (structure varies by endpoint)
            items = []
            if "members" in data:
                items = data["members"]
            elif "committees" in data:
                items = data["committees"]
            elif "results" in data:
                items = data["results"]

            if not items:
                break

            all_items.extend(items)
            logger.info(f"Fetched {len(all_items)} items from {endpoint}")

            if len(items) < limit:
                break  # No more pages

            offset += limit

        return all_items[:max_results]

    def get_current_members(self, chamber: Optional[str] = None) -> list[Member]:
        """
        Fetch all current members of Congress.

        Args:
            chamber: Optional filter ('house' or 'senate')

        Returns:
            List of Member objects
        """
        params = {"currentMember": "true"}

        raw_members = self._get_paginated("/member", params, max_results=600)

        members = []
        for m in raw_members:
            # Parse member data
            try:
                # Determine chamber from terms
                member_chamber = None
                terms = m.get("terms", {}).get("item", [])
                if terms:
                    # Get most recent term
                    latest_term = terms[-1] if isinstance(terms, list) else terms
                    member_chamber = latest_term.get("chamber", "").lower()
                    if member_chamber == "house of representatives":
                        member_chamber = "house"

                # Skip if chamber filter doesn't match
                if chamber and member_chamber != chamber:
                    continue

                member = Member(
                    bioguide_id=m.get("bioguideId", ""),
                    name=m.get("name", ""),
                    party=m.get("partyName", ""),
                    state=m.get("state", ""),
                    chamber=member_chamber or "unknown",
                    district=m.get("district"),
                    in_office=True,
                )
                members.append(member)

            except Exception as e:
                logger.warning(f"Error parsing member {m.get('bioguideId', 'unknown')}: {e}")
                continue

        logger.info(f"Parsed {len(members)} current members")
        return members

    def get_member_committees(self, bioguide_id: str) -> list[dict]:
        """
        Fetch committee assignments for a specific member.

        Args:
            bioguide_id: Member's bioguide ID

        Returns:
            List of committee assignments
        """
        # Try to get from member detail endpoint
        data = self._get(f"/member/{bioguide_id}")
        if not data or "member" not in data:
            return []

        member_data = data["member"]

        # Extract committee assignments from depiction or other fields
        committees = []

        # Check for committee memberships in various possible locations
        if "committees" in member_data:
            raw_committees = member_data["committees"]
            if isinstance(raw_committees, list):
                for c in raw_committees:
                    committees.append(
                        {
                            "code": c.get("systemCode", c.get("code", "")),
                            "name": c.get("name", ""),
                            "chamber": c.get("chamber", "").lower(),
                        }
                    )

        return committees

    def get_all_committees(self, congress: int = CURRENT_CONGRESS) -> list[dict]:
        """
        Fetch all committees for a given Congress.

        Args:
            congress: Congress number (default: current)

        Returns:
            List of committee dictionaries
        """
        committees = []

        for chamber in ["house", "senate", "joint"]:
            data = self._get(f"/committee/{congress}/{chamber}")
            if data and "committees" in data:
                for c in data["committees"]:
                    committees.append(
                        {
                            "code": c.get("systemCode", ""),
                            "name": c.get("name", ""),
                            "chamber": chamber,
                            "url": c.get("url", ""),
                        }
                    )

        logger.info(f"Fetched {len(committees)} committees")
        return committees

    def enrich_members_with_committees(self, members: list[Member]) -> list[Member]:
        """
        Enrich member objects with their committee assignments.

        Args:
            members: List of Member objects to enrich

        Returns:
            Same list with committees populated
        """
        for i, member in enumerate(members):
            if i > 0 and i % 50 == 0:
                logger.info(f"Enriched {i}/{len(members)} members with committees")

            committees = self.get_member_committees(member.bioguide_id)
            member.committees = committees

        logger.info(f"Enriched all {len(members)} members with committees")
        return members

    def load_from_cache(self) -> Optional[list[Member]]:
        """
        Load cached member data if available.

        Returns:
            List of Member objects or None if no cache
        """
        cache_file = self.cache_dir / "members.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r") as f:
                data = json.load(f)

            members = []
            for m in data:
                members.append(
                    Member(
                        bioguide_id=m["bioguide_id"],
                        name=m["name"],
                        party=m["party"],
                        state=m["state"],
                        chamber=m["chamber"],
                        district=m.get("district"),
                        in_office=m.get("in_office", True),
                        committees=m.get("committees", []),
                    )
                )

            logger.info(f"Loaded {len(members)} members from cache")
            return members

        except Exception as e:
            logger.warning(f"Error loading cache: {e}")
            return None

    def save_to_cache(self, members: list[Member]) -> None:
        """
        Save member data to cache.

        Args:
            members: List of Member objects to cache
        """
        cache_file = self.cache_dir / "members.json"

        data = []
        for m in members:
            data.append(
                {
                    "bioguide_id": m.bioguide_id,
                    "name": m.name,
                    "party": m.party,
                    "state": m.state,
                    "chamber": m.chamber,
                    "district": m.district,
                    "in_office": m.in_office,
                    "committees": m.committees,
                }
            )

        with open(cache_file, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved {len(members)} members to cache")
