import pytest
from pathlib import Path

from sqlite_manager.interface import SQLiteInterface


@pytest.fixture
def test_db(tmp_path: Path) -> SQLiteInterface:
    """Fixture for creating a temporary SQLite database interface."""

    return SQLiteInterface(tmp_path / "test.db")
