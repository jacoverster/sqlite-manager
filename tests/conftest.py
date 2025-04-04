import pytest
from pathlib import Path

from sqlite_manager.interface import SQLiteInterface


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Fixture for creating a temporary database path."""

    return tmp_path / "test.db"


@pytest.fixture
def test_db(db_path: Path) -> SQLiteInterface:
    """Fixture for creating a temporary SQLite database interface."""

    return SQLiteInterface(db_path)
