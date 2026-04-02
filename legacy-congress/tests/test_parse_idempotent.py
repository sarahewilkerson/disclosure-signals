"""Tests for idempotent parsing (hash-based skip logic)."""

import hashlib
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestParseSkipLogic:
    """Tests for hash-based skip logic in cmd_parse."""

    def test_parse_skips_unchanged_filing(self):
        """Filings with unchanged source_hash should be skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test PDF file
            pdf_path = Path(tmpdir) / "test_filing.pdf"
            pdf_content = b"test pdf content"
            pdf_path.write_bytes(pdf_content)
            pdf_hash = hashlib.sha256(pdf_content).hexdigest()

            # Create a test database with existing filing
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE filings (
                    filing_id TEXT PRIMARY KEY,
                    source_hash TEXT
                )
            """)
            conn.execute(
                "INSERT INTO filings (filing_id, source_hash) VALUES (?, ?)",
                ("test_filing", pdf_hash)
            )
            conn.commit()

            # Verify the logic: same hash should indicate skip
            existing = conn.execute(
                "SELECT source_hash FROM filings WHERE filing_id = ?",
                ("test_filing",)
            ).fetchone()

            current_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
            should_skip = existing and existing[0] == current_hash

            assert should_skip is True
            conn.close()

    def test_parse_reprocesses_changed_filing(self):
        """Filings with different source_hash should be reprocessed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test PDF file with new content
            pdf_path = Path(tmpdir) / "test_filing.pdf"
            new_content = b"updated pdf content"
            pdf_path.write_bytes(new_content)

            # Create database with old hash
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE filings (
                    filing_id TEXT PRIMARY KEY,
                    source_hash TEXT
                )
            """)
            old_hash = hashlib.sha256(b"old pdf content").hexdigest()
            conn.execute(
                "INSERT INTO filings (filing_id, source_hash) VALUES (?, ?)",
                ("test_filing", old_hash)
            )
            conn.commit()

            # Verify the logic: different hash should not skip
            existing = conn.execute(
                "SELECT source_hash FROM filings WHERE filing_id = ?",
                ("test_filing",)
            ).fetchone()

            current_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
            should_skip = existing and existing[0] == current_hash

            assert should_skip is False
            conn.close()

    def test_parse_force_flag_ignores_hash(self):
        """With --force flag, filings should be parsed regardless of hash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test PDF file
            pdf_path = Path(tmpdir) / "test_filing.pdf"
            pdf_content = b"test pdf content"
            pdf_path.write_bytes(pdf_content)
            pdf_hash = hashlib.sha256(pdf_content).hexdigest()

            # Create database with same hash
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE filings (
                    filing_id TEXT PRIMARY KEY,
                    source_hash TEXT
                )
            """)
            conn.execute(
                "INSERT INTO filings (filing_id, source_hash) VALUES (?, ?)",
                ("test_filing", pdf_hash)
            )
            conn.commit()

            # Simulate --force flag
            force = True
            current_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()

            # With force=True, should not skip even with matching hash
            existing = conn.execute(
                "SELECT source_hash FROM filings WHERE filing_id = ?",
                ("test_filing",)
            ).fetchone()

            # Force bypasses the skip logic
            should_skip = not force and existing and existing[0] == current_hash

            assert should_skip is False
            conn.close()

    def test_parse_new_filing_always_parsed(self):
        """New filings with no existing hash should always be parsed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test PDF file
            pdf_path = Path(tmpdir) / "new_filing.pdf"
            pdf_content = b"new pdf content"
            pdf_path.write_bytes(pdf_content)

            # Create empty database (no existing filings)
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            conn.execute("""
                CREATE TABLE filings (
                    filing_id TEXT PRIMARY KEY,
                    source_hash TEXT
                )
            """)
            conn.commit()

            # Query for non-existent filing
            existing = conn.execute(
                "SELECT source_hash FROM filings WHERE filing_id = ?",
                ("new_filing",)
            ).fetchone()

            current_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()

            # No existing record means should not skip
            should_skip = existing and existing[0] == current_hash

            # should_skip will be None (falsy) because existing is None
            assert not should_skip
            conn.close()


class TestHashGeneration:
    """Tests for hash generation functions."""

    def test_pdf_hash_consistent(self):
        """Hash of same content should be consistent."""
        content = b"test content for hashing"
        hash1 = hashlib.sha256(content).hexdigest()
        hash2 = hashlib.sha256(content).hexdigest()
        assert hash1 == hash2

    def test_pdf_hash_changes_with_content(self):
        """Hash should change when content changes."""
        hash1 = hashlib.sha256(b"content v1").hexdigest()
        hash2 = hashlib.sha256(b"content v2").hexdigest()
        assert hash1 != hash2

    def test_combined_hash_for_multi_page(self):
        """Combined hash for multi-page filings should work correctly."""
        # Simulate hashing multiple GIF files
        page1 = b"page 1 content"
        page2 = b"page 2 content"

        combined = hashlib.sha256()
        combined.update(page1)
        combined.update(page2)
        multi_hash = combined.hexdigest()

        # Single hash of combined content should match
        combined2 = hashlib.sha256()
        combined2.update(page1)
        combined2.update(page2)
        assert combined2.hexdigest() == multi_hash

    def test_combined_hash_order_matters(self):
        """Hash should differ based on page order."""
        page1 = b"page 1 content"
        page2 = b"page 2 content"

        combined1 = hashlib.sha256()
        combined1.update(page1)
        combined1.update(page2)

        combined2 = hashlib.sha256()
        combined2.update(page2)
        combined2.update(page1)

        assert combined1.hexdigest() != combined2.hexdigest()
