import logging

from pytest import TempPathFactory

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator


CREATE_QUERY = "CREATE TABLE test (uid INTEGER PRIMARY KEY, name TEXT NOT NULL);"
INVALID_QUERY = "CREATE TABLE test;"
SELECT_QUERY = "SELECT name FROM sqlite_master WHERE type='table' AND name='test';"


def test_migrate(caplog, test_db: SQLiteInterface, tmp_path_factory: TempPathFactory):
    tmp_database_dir = tmp_path_factory.mktemp("test_migrate")
    migration_dir = tmp_database_dir / "migrations"
    backup_dir = tmp_database_dir / "backups"

    sql_migrator = SQLiteMigrator(test_db.db_path, migration_dir, backup_dir)
    current_db_version = test_db.get_version()
    new_db_version = 999

    # Create a simple migration and place it in the migrations directory
    migration_file = migration_dir / f"migration_{new_db_version}.sql"
    migration_file.write_text(CREATE_QUERY)

    # Confirm that the migration is pending
    assert sql_migrator.get_pending_migrations() == {new_db_version: CREATE_QUERY}

    # Execute the migration
    sql_migrator.migrate()

    # Confirm that the migration has been executed
    #   - the database version should be the same as the migration version
    assert test_db.get_version() == new_db_version

    #   - there should be no pending migrations
    assert sql_migrator.get_pending_migrations() == {}

    #   - the table should exist in the database
    assert test_db.fetch_one(SELECT_QUERY) == ("test",)

    #   - a backup of the previous database should have been created
    backup_files = list(backup_dir.glob(f"backup_v{current_db_version}*.sqlite3"))
    assert len(backup_files) == 1

    #   - a schema.sql file should have been created
    schema_files = list(migration_dir.glob(f"schema_v{new_db_version}.sql"))
    assert len(schema_files) == 1

    # Execute the migration again 'No pending migrations to apply.' should be logged
    with caplog.at_level(logging.INFO):
        sql_migrator.migrate()
        assert "No pending migrations to apply." in caplog.text

    # Restore the database to the previous version
    sql_migrator.restore(backup_files[0])

    # The database version should have been reverted
    #   - the database version should be the same as before the migration
    assert test_db.get_version() == current_db_version

    #   - the table should not exist in the database
    assert test_db.fetch_one(SELECT_QUERY) is None

    #   - the schema.sql file should have deleted
    schema_files = list(migration_dir.glob(f"schema_v{new_db_version}.sql"))
    assert len(schema_files) == 0


def test_failed_migrate(caplog, test_db: SQLiteInterface, tmp_path_factory):
    tmp_database_dir = tmp_path_factory.mktemp("test_failed_migrate")
    migration_dir = tmp_database_dir / "migrations"
    backup_dir = tmp_database_dir / "backups"

    sql_migrator = SQLiteMigrator(test_db.db_path, migration_dir, backup_dir)
    current_db_version = test_db.get_version()
    new_db_version = 999

    # Create a failing migration and place it in the migrations directory
    migration_file = migration_dir / f"migration_{new_db_version}.sql"
    migration_file.write_text(INVALID_QUERY)

    with caplog.at_level(logging.INFO):
        sql_migrator.migrate()
        assert "Migration failed" in caplog.text
        assert "Rolling back changes" in caplog.text

    assert test_db.get_version() == current_db_version
