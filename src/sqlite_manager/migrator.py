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
    current database version in numeric order.

    A backup of the database is created before migration and is stored in the
    backups directory.
    """

    def __init__(
        self, sql_db: SQLiteInterface, migrations_dir: Path, backup_dir: Path
    ) -> None:
        """Initializes the SQLite migrator.

        Args:
            sql_db: SQLite interface
            migrations_dir: path to directory with migration files
            backup_dir: path to directory to store backups
        """

        migrations_dir.mkdir(parents=True, exist_ok=True)
        backup_dir.mkdir(parents=True, exist_ok=True)

        self.db = sql_db
        self.migrations_dir = migrations_dir
        self.backup_dir = backup_dir

        self.create_migration_table()

    def create_migration_table(self):
        """Creates the migrations table if it doesn't exist."""

        self.db.execute_sql(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                version INTEGER PRIMARY KEY,
                sql_script TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def get_database_version(self) -> int:
        """Returns the current database version from the migrations table."""

        query = "SELECT version FROM migrations ORDER BY version DESC LIMIT 1"
        version = self.db.fetch_one(query)

        return version[0] if version else 0

    def create_backup(self, backup_path: Path) -> None:
        """Creates a backup of the SQLite database.

        Args:
            backup_path: Path to the backup file
        """

        self.db.execute_sql("VACUUM main INTO ?", (backup_path.as_posix(),))

    def restore(self, backup_path: Path) -> bool:
        """Restores the database to the given backup.

        Args:
            backup_path: Path to the backup file

        Returns:

        """

        database_version = self.get_database_version()
        backup_path.replace(self.db.db_path)
        reverted_version = self.get_database_version()

        # Remove the schema.sql file
        schema_files = list(self.migrations_dir.glob(f"schema_v{database_version}.sql"))
        schema_files[0].unlink()

        return reverted_version != database_version

    def migrate(
        self, data_generator: Callable[[SQLiteInterface], None] | None = None
    ) -> None:
        """Migrate the database to the latest version.

        Args:
            data_generator: Callable run after the first migration only.
        """

        pending_migrations = self.get_pending_migrations()

        if not pending_migrations:
            log.info("No pending migrations to apply.")
            return

        for version, sql_script in pending_migrations.items():
            self.backup_database()
            # Open a connection for fine-grained control during migration
            connection = sqlite3.connect(self.db.db_path)
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
                data_generator(self.db)

    def get_pending_migrations(self) -> dict[int, str]:
        """Returns a dictionary of pending migrations as {version: script_content}."""

        def get_migration_version(migration: Path) -> int:
            try:
                version = migration.stem.split("migration_")[1]
                return int(version)
            except ValueError:
                return -1

        database_version = self.get_database_version()
        migration_files = sorted(self.migrations_dir.glob("migration_*.sql"))
        migrations = {}

        for migration in migration_files:
            migration_version = get_migration_version(migration)
            if migration_version > database_version:
                migrations[migration_version] = migration.read_text()

        return migrations

    def backup_database(self) -> None:
        """Backs up the database to the backup directory."""

        database_version = self.get_database_version()
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%S")
        back_name = f"backup_v{database_version}_{now}.sqlite3"
        backup_path = self.backup_dir / back_name
        self.create_backup(backup_path)
        log.info(f"Backed up database to {backup_path}...")

    def write_db_schema_script(self, version: int) -> None:
        """Writes the schema.sql file to the migrations directory.

        Args:
            version: integer schema version number
        """

        latest_schema_path = self.migrations_dir / f"schema_v{version}.sql"
        tables = self.db.fetch_all("SELECT sql FROM sqlite_master WHERE type='table'")
        if tables:
            latest_schema_path.write_text(
                (
                    "-- This file is auto-generated by the migration script\n"
                    "-- for reference purposes only. DO NOT EDIT.\n\n"
                )
                + ";\n\n".join(t[0] for t in tables)
                + "\n"
            )
