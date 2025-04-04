from contextlib import closing, contextmanager
import logging
import sqlite3
from collections.abc import Callable
from pathlib import Path
from sqlite3 import Cursor, Row
from typing import Any, Generator, Mapping, TypeVar


log = logging.getLogger(__name__)

T = TypeVar("T", bound=tuple | dict)
RowFactory = Callable[[Cursor, Row], T]

DEFAULT_PRAGMAS = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "foreign_keys": "ON",
    "busy_timeout": 5000,
}


class SQLiteInterface:
    """SQLite interface for handling database connections and queries."""

    def __init__(self, db_path: Path, pragmas: dict = DEFAULT_PRAGMAS) -> None:
        """Initializes the SQLite interface with the given database path and pragmas."""

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.pragmas = pragmas or {}

    @contextmanager
    def connection(
        self, row_factory: RowFactory | None = None
    ) -> Generator[sqlite3.Connection, None, None]:
        """Returns a connection to the SQLite database wih applied pragmas.

        SQLite doesn't close the connection when using context managers, so
        it has to be closed manually.
        """

        with closing(sqlite3.connect(self.db_path)) as con, con:
            for pragma, value in self.pragmas.items():
                con.execute(f"PRAGMA {pragma} = {value};")

            if row_factory:
                con.row_factory = row_factory

            yield con

    def get_version(self) -> int | None:
        """Returns the current database version from the migrations table."""

        try:
            query = "SELECT version FROM migrations ORDER BY version DESC LIMIT 1"
            version = self.fetch_one(query)
            return version[0] if version else 0
        except sqlite3.OperationalError:
            log.info("No migrations table found. Assuming version 0.")
            return 0

    def create_backup(self, backup_path: Path) -> None:
        """Creates a backup of the SQLite database.

        Args:
            backup_path: Path to the backup file
        """

        backup_path.parent.mkdir(parents=True, exist_ok=True)
        self.execute_sql("VACUUM main INTO ?", (backup_path.as_posix(),))

    # self, query: str, params: Mapping[str, Any] = None, count_changes: bool = False

    def execute_sql(
        self, query: str, params: Mapping[str, Any] = {}, count_changes: bool = False
    ) -> None | int:
        """Executes a SQL query and returns the number of changes if requested.

        Args:
            query: SQL query to execute
            params:  Optional parameters to bind to the query
            count_changes: Whether to return the number of rows affected

        Returns:
            Number of affected rows if count_changes is True, None otherwise
        """

        changes = None
        with self.connection() as con:
            cursor = con.execute(query, params)
            if count_changes:
                changes = cursor.execute("select changes();").fetchone()[0]

        return changes

    def execute_many(
        self, query: str, params: Mapping[str, Any] = {}, count_changes: bool = False
    ) -> None | int:
        """Executes a SQL query and returns the number of changes if requested.

        Args:
            query: SQL query to execute
            params:  Optional parameters to bind to the query
            count_changes: Whether to return the number of rows affected

        Returns:
            Number of affected rows if count_changes is True, None otherwise
        """

        changes = None
        with self.connection() as con:
            cursor = con.executemany(query, params)
            if count_changes:
                changes = cursor.execute("select changes();").fetchone()[0]
        return changes

    def fetch_one(
        self,
        query: str,
        params: Mapping[str, Any] = {},
        row_factory: RowFactory[T] | None = None,
    ) -> T | tuple | None:
        """Fetches a single row from the database.

        Args:
            query: SQL query to execute
            params:  Optional parameters to bind to the query
            row_factory: Optional callable that converts rows to desired format

        Returns:
            A single row in the format specified by row_factory, or None if no results
        """

        with self.connection(row_factory) as con:
            return con.execute(query, params).fetchone()

    def fetch_all(
        self,
        query: str,
        params: Mapping[str, Any] = {},
        row_factory: RowFactory[T] | None = None,
    ) -> list[T] | list[tuple] | None:
        """Fetches all rows from the database.

        Args:
            query: SQL query to execute
            params:  Optional parameters to bind to the query
            row_factory: Optional callable that converts rows to desired format

        Returns:
            A list of rows in the format specified by row_factory, or None if no results
        """

        with self.connection() as con:
            if row_factory:
                con.row_factory = row_factory
            return con.execute(query, params).fetchall()

    def get_dict_factory(self) -> Callable[[Cursor, Row], dict]:
        """Returns a dictionary factory for SQLite queries."""

        def dict_factory(cursor, row):
            fields = [column[0] for column in cursor.description]
            return {key: value for key, value in zip(fields, row)}

        return dict_factory
