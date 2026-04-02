"""Tests for the REST API."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

# Create temp db BEFORE importing anything that uses config
_db_fd, _db_path = tempfile.mkstemp(suffix=".db")
os.environ["DB_PATH"] = _db_path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now we can safely import config and db
import config
config.DB_PATH = _db_path

from db import init_db, get_connection

# FastAPI test client - import api after config is set
from api import app
from fastapi.testclient import TestClient


class TestAPI(unittest.TestCase):
    """Tests for the REST API endpoints."""

    @classmethod
    def setUpClass(cls):
        """Set up test database before all tests."""
        cls.db_fd = _db_fd
        cls.db_path = _db_path

        cls.client = TestClient(app)

        # Initialize database and add test data
        init_db(cls.db_path)
        cls._setup_test_data()

    @classmethod
    def _setup_test_data(cls):
        """Insert test data into the database."""
        with get_connection(cls.db_path) as conn:
            # Insert test companies
            conn.execute("""
                INSERT INTO companies (cik, ticker, company_name, fortune_rank, revenue, sector)
                VALUES
                    ('0000320193', 'AAPL', 'Apple Inc.', 1, 394328, 'Technology'),
                    ('0000789019', 'MSFT', 'Microsoft Corporation', 2, 198000, 'Technology'),
                    ('0001018724', 'AMZN', 'Amazon.com Inc.', 3, 514000, 'Consumer Cyclical')
            """)

            # Insert test scores
            conn.execute("""
                INSERT INTO company_scores (
                    cik, ticker, window_days, computed_at, signal, score,
                    confidence, confidence_tier, buy_count, sell_count,
                    unique_buyers, unique_sellers, net_buy_value,
                    explanation, filing_accessions
                ) VALUES
                    ('0000320193', 'AAPL', 90, '2024-01-15T10:00:00', 'BULLISH', 0.65,
                     0.8, 'HIGH', 5, 1, 3, 1, 5000000.0,
                     'Strong buying activity', '["0001234-24-000001"]'),
                    ('0000789019', 'MSFT', 90, '2024-01-15T10:00:00', 'NEUTRAL', 0.05,
                     0.6, 'MODERATE', 2, 2, 2, 2, 100000.0,
                     'Mixed signals', '["0001234-24-000002"]'),
                    ('0001018724', 'AMZN', 90, '2024-01-15T10:00:00', 'BEARISH', -0.45,
                     0.7, 'MODERATE', 1, 4, 1, 3, -3000000.0,
                     'Selling pressure', '["0001234-24-000003"]')
            """)

            # Insert aggregate index
            conn.execute("""
                INSERT INTO aggregate_index (
                    window_days, computed_at, risk_appetite_index,
                    bullish_breadth, bearish_breadth, neutral_pct, insufficient_pct,
                    total_companies, companies_with_signal, sector_breakdown
                ) VALUES
                    (90, '2024-01-15T10:00:00', 0.25, 0.33, 0.33, 0.33, 0.0,
                     3, 3, '{"Technology": 0.35, "Consumer Cyclical": -0.45}')
            """)

    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        os.close(cls.db_fd)
        os.unlink(cls.db_path)

    def test_health_endpoint(self):
        """Test health check returns healthy status."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["database"], "connected")
        self.assertIn("timestamp", data)

    def test_status_endpoint(self):
        """Test status endpoint returns database stats."""
        response = self.client.get("/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["companies"], 3)
        self.assertEqual(data["company_scores"], 3)

    def test_scores_endpoint(self):
        """Test scores endpoint returns company scores."""
        response = self.client.get("/scores?window_days=90")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 3)
        self.assertEqual(data["window_days"], 90)
        self.assertEqual(len(data["scores"]), 3)

    def test_scores_filter_by_signal(self):
        """Test filtering scores by signal type."""
        response = self.client.get("/scores?window_days=90&signal=BULLISH")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["scores"][0]["ticker"], "AAPL")

    def test_scores_filter_by_sector(self):
        """Test filtering scores by sector."""
        response = self.client.get("/scores?window_days=90&sector=Technology")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 2)

    def test_scores_filter_by_min_confidence(self):
        """Test filtering scores by minimum confidence."""
        response = self.client.get("/scores?window_days=90&min_confidence=0.75")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["scores"][0]["ticker"], "AAPL")

    def test_scores_pagination(self):
        """Test pagination of scores."""
        response = self.client.get("/scores?window_days=90&limit=2&offset=0")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["scores"]), 2)
        self.assertEqual(data["total"], 3)  # Total should still be 3

    def test_score_by_ticker(self):
        """Test getting score by ticker."""
        response = self.client.get("/scores/AAPL?window_days=90")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["ticker"], "AAPL")
        self.assertEqual(data["signal"], "BULLISH")
        self.assertEqual(data["score"], 0.65)

    def test_score_by_ticker_not_found(self):
        """Test 404 for non-existent ticker."""
        response = self.client.get("/scores/INVALID?window_days=90")
        self.assertEqual(response.status_code, 404)

    def test_aggregate_endpoint(self):
        """Test aggregate index endpoint."""
        response = self.client.get("/aggregate?window_days=90")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["window_days"], 90)
        self.assertEqual(data["risk_appetite_index"], 0.25)
        self.assertEqual(data["total_companies"], 3)
        self.assertIn("Technology", data["sector_breakdown"])

    def test_aggregate_not_found(self):
        """Test 404 for non-existent window_days."""
        response = self.client.get("/aggregate?window_days=30")
        self.assertEqual(response.status_code, 404)

    def test_sectors_endpoint(self):
        """Test sectors endpoint returns unique sectors."""
        response = self.client.get("/sectors")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("Technology", data["sectors"])
        self.assertIn("Consumer Cyclical", data["sectors"])

    def test_companies_endpoint(self):
        """Test companies endpoint returns company list."""
        response = self.client.get("/companies")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["companies"]), 3)

    def test_companies_filter_by_sector(self):
        """Test filtering companies by sector."""
        response = self.client.get("/companies?sector=Technology")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["companies"]), 2)

    def test_openapi_docs(self):
        """Test OpenAPI docs are available."""
        response = self.client.get("/docs")
        self.assertEqual(response.status_code, 200)

    def test_openapi_schema(self):
        """Test OpenAPI schema is available."""
        response = self.client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["info"]["title"], "Insider Trading Signal Engine")


if __name__ == "__main__":
    unittest.main()
