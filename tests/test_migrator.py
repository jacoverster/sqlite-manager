import logging
from pathlib import Path

import pytest

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator


CREATE_QUERY = "CREATE TABLE test (uid INTEGER PRIMARY KEY, name TEXT NOT NULL);"
INVALID_QUERY = "CREATE TABLE test;"
SELECT_QUERY = "SELECT name FROM sqlite_master WHERE type='table' AND name='test';"


@pytest.fixture
def test_migrator(tmp_path: Path) -> SQLiteMigrator:
    """Fixture for creating a temporary SQLite migrator."""

    return SQLiteMigrator(
        tmp_path / "test.db", tmp_path / "migrations", tmp_path / "backups"
    )


def test_get_database_version(test_migrator: SQLiteMigrator):
    assert test_migrator.get_database_version() == 0


def test_create_backup(test_migrator: SQLiteMigrator):
    """Test the creation of a backup of the SQLite database."""

    test_migrator.db.execute_sql("CREATE TABLE test_table (test_column);")

    backup_path = test_migrator.backup_dir / "backup.db"
    test_migrator.create_backup(backup_path)
    assert backup_path.exists()

    # Check that the backup contains test_table
    backup_db = SQLiteInterface(backup_path)
    with backup_db.connection() as con:
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        assert ("test_table",) in tables


def test_migrate(caplog, test_migrator: SQLiteMigrator):
    current_db_version = test_migrator.get_database_version()
    new_db_version = 999

    # Create a simple migration and place it in the migrations directory
    migration_file = test_migrator.migrations_dir / f"migration_{new_db_version}.sql"
    migration_file.write_text(CREATE_QUERY)

    # Confirm that the migration is pending
    assert test_migrator.get_pending_migrations() == {new_db_version: CREATE_QUERY}

    # Execute the migration
    test_migrator.migrate()

    # Confirm that the migration has been executed
    #   - the database version should be the same as the migration version
    assert test_migrator.get_database_version() == new_db_version

    #   - there should be no pending migrations
    assert test_migrator.get_pending_migrations() == {}

    #   - the table should exist in the database
    assert test_migrator.db.fetch_one(SELECT_QUERY) == ("test",)

    #   - a backup of the previous database should have been created
    backup_files = list(
        test_migrator.backup_dir.glob(f"backup_v{current_db_version}*.sqlite3")
    )
    assert len(backup_files) == 1

    #   - a schema.sql file should have been created
    schema_files = list(
        test_migrator.migrations_dir.glob(f"schema_v{new_db_version}.sql")
    )
    assert len(schema_files) == 1

    # Execute the migration again 'No pending migrations to apply.' should be logged
    with caplog.at_level(logging.INFO):
        test_migrator.migrate()
        assert "No pending migrations to apply." in caplog.text

    # Restore the database to the previous version
    test_migrator.restore(backup_files[0])

    # The database version should have been reverted
    #   - the database version should be the same as before the migration
    assert test_migrator.get_database_version() == current_db_version

    #   - the table should not exist in the database
    assert test_migrator.db.fetch_one(SELECT_QUERY) is None

    #   - the schema.sql file should have deleted
    schema_files = list(
        test_migrator.migrations_dir.glob(f"schema_v{new_db_version}.sql")
    )
    assert len(schema_files) == 0


def test_failed_migrate(caplog, test_migrator: SQLiteMigrator):
    current_db_version = test_migrator.get_database_version()
    new_db_version = 999

    # Create a failing migration and place it in the migrations directory
    migration_file = test_migrator.migrations_dir / f"migration_{new_db_version}.sql"
    migration_file.write_text(INVALID_QUERY)

    with caplog.at_level(logging.INFO):
        test_migrator.migrate()
        assert "Migration failed" in caplog.text
        assert "Rolling back changes" in caplog.text

    assert test_migrator.get_database_version() == current_db_version
