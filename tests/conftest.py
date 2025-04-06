import pytest
from pathlib import Path

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator


@pytest.fixture
def test_db(tmp_path: Path) -> SQLiteInterface:
    """Fixture for creating a temporary SQLite database interface."""

    return SQLiteInterface(tmp_path / "test.db")


@pytest.fixture
def test_migrator(test_db, tmp_path: Path) -> SQLiteMigrator:
    """Fixture for creating a temporary SQLite migrator."""

    return SQLiteMigrator(test_db, tmp_path / "migrations", tmp_path / "backups")
