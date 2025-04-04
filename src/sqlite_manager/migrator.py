import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
import sqlite3

from sqlite_manager.interface import SQLiteInterface


log = logging.getLogger(__name__)


class SQLiteMigrator:
    """A database migrator for SQLite databases.

    A migrations table is created to version the database and record migrations.

    Migrations are SQL scripts that are executed in order to update the database schema.
    SQL scripts are stored in the migrations directory and should be named starting with
    migration_ and a number, for eg. migration_0001.sql. See the migration_template.sql
    for help with creating scripts.

    The migrator will execute all scripts with a version number greater than the
    current database version.

    A backup of the database is created before migration and is stored in the
    backups directory.
    """

    def __init__(self, db_path: Path, migrations_dir: Path, backup_dir: Path) -> None:
        migrations_dir.mkdir(parents=True, exist_ok=True)
        backup_dir.mkdir(parents=True, exist_ok=True)

        self.migrations_dir = migrations_dir
        self.backup_dir = backup_dir

        self.sql_db = SQLiteInterface(db_path)
        self._create_migration_table_if_not_exist()

    def _create_migration_table_if_not_exist(self):
        """Creates the migrations table if it doesn't exist."""

        self.sql_db.execute_sql(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                version INTEGER PRIMARY KEY,
                sql_script TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            count_changes=True,
        )

    def restore(self, backup_path: Path) -> bool:
        """Restores the database to the given backup."""

        database_version = self.sql_db.get_version()
        backup_path.replace(self.sql_db.db_path)
        reverted_version = self.sql_db.get_version()

        # Remove the schema.sql file
        schema_files = list(self.migrations_dir.glob(f"schema_v{database_version}.sql"))
        schema_files[0].unlink()

        return reverted_version != database_version

    def migrate(
        self, data_generator: Callable[[SQLiteInterface], None] | None = None
    ) -> None:
        """Migrate the database to the latest version.

        The data_generator is a callable that is run after the first migration only.
        """

        pending_migrations = self.get_pending_migrations()

        if not pending_migrations:
            log.info("No pending migrations to apply.")
            return

        for version, sql_script in pending_migrations.items():
            self.backup_database()
            # Open a connection for fine-grained control during migration
            connection = sqlite3.connect(self.sql_db.db_path)
            try:
                # Execute the migration script
                connection.executescript(sql_script)

                # Insert migration record
                query = "INSERT INTO migrations (version, sql_script) VALUES (?, ?)"
                connection.execute(query, (version, sql_script))
                connection.commit()

                self.write_db_schema_script(version)
                log.info(f"Successfully migrated database to version {version}")
            except Exception as e:
                log.exception(f"Migration failed: {e}.")
                log.info("Rolling back changes.")
                connection.rollback()
            finally:
                connection.close()

            if version == 1 and data_generator is not None:
                data_generator(self.sql_db)

    def get_pending_migrations(self) -> dict[int, str]:
        """Returns a dictionary of pending migrations as {version: script_content}."""

        def _get_migration_version(migration: Path) -> int:
            try:
                version = migration.stem.split("migration_")[1]
                return int(version)
            except ValueError:
                return -1

        database_version = self.sql_db.get_version()
        migration_files = sorted(self.migrations_dir.glob("migration_*.sql"))
        migrations = {}

        for migration in migration_files:
            migration_version = _get_migration_version(migration)
            if migration_version > database_version:
                migrations[migration_version] = migration.read_text()

        return migrations

    def backup_database(self) -> None:
        """Backs up the database to the backup directory."""

        database_version = self.sql_db.get_version()

        now = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
        back_name = f"backup_v{database_version}_{now}.sqlite3"
        backup_path = self.backup_dir / back_name
        self.sql_db.create_backup(backup_path)
        log.info(f"Backed up database to {backup_path}...")

    def write_db_schema_script(self, version: int) -> None:
        """Writes the version schema.sql to the migrations directory."""

        latest_schema_path = self.migrations_dir / f"schema_v{version}.sql"
        tables = self.sql_db.fetch_all(
            "SELECT sql FROM sqlite_master WHERE type='table'"
        )
        if tables:
            latest_schema_path.write_text(
                (
                    "-- This file is auto-generated by the migration script\n"
                    "-- for reference purposes only. DO NOT EDIT.\n\n"
                )
                + ";\n\n".join(t[0] for t in tables)
                + "\n"
            )
