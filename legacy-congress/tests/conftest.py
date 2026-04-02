"""Pytest configuration and fixtures for CPPI tests."""

import os
import tempfile

import pytest

from cppi.db import get_connection, init_db


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize the database
    init_db(db_path)

    yield db_path

    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def db_connection(temp_db):
    """Provide a database connection for testing."""
    with get_connection(temp_db) as conn:
        yield conn
