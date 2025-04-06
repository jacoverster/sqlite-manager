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


class SQLiteInterfaceError(Exception):
    """Base exception for SQLiteInterface errors."""


class SQLiteQueryError(SQLiteInterfaceError):
    """Exception raised for SQL query errors."""


class SQLiteInterface:
    """SQLite interface for handling database connections and queries."""

    def __init__(self, db_path: Path, pragmas: dict = DEFAULT_PRAGMAS) -> None:
        """Initializes the SQLite interface with the given database path and pragmas.

        Args:
            db_path: Path to the SQLite database file
            pragmas: Dict of SQLite PRAGMA settings to apply on each connection
        """
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.pragmas = pragmas or {}

    @contextmanager
    def connection(
        self, row_factory: RowFactory | None = None
    ) -> Generator[sqlite3.Connection, None, None]:
        """Returns a connection to the SQLite database wih applied pragmas.

        Commits the transaction if no exceptions occur. The connection is closed
        automatically after use. If an exception occurs, the connection is rolled back.
        If a row_factory is provided, it is used to convert rows to the desired format.

        Args:
            row_factory: Optional callable that converts rows to desired format

        Yields:
            An active SQLite connection with pragmas applied
        """
        with closing(sqlite3.connect(self.db_path)) as con, con:
            for pragma, value in self.pragmas.items():
                con.execute(f"PRAGMA {pragma} = {value};")

            if row_factory:
                con.row_factory = row_factory

            yield con

    def execute_sql(self, query: str, params: Mapping[str, Any] = {}) -> None | int:
        """Executes a SQL query and returns the number of changes if requested.

        Args:
            query: SQL query to execute
            params:  Optional parameters to bind to the query

        Returns:
            Number of applied changes

        Raises:
            SQLiteQueryError: If the query execution fails
        """

        try:
            with self.connection() as con:
                cursor = con.execute(query, params)
                changes = cursor.execute("select changes()").fetchone()[0]
            return changes
        except sqlite3.Error as e:
            raise SQLiteQueryError(f"Failed to execute query: {e}") from e

    def execute_many(
        self, query: str, params: list[Mapping[str, Any]] = []
    ) -> None | int:
        """Executes a SQL query with multiple parameter sets.

        Args:
            query: SQL query to execute
            params: List of parameter sets to bind to the query

        Returns:
            Number of applied changes

        Raises:
            SQLiteQueryError: If the query execution fails
        """

        try:
            with self.connection() as con:
                cursor = con.executemany(query, params)
                changes = cursor.execute("select changes()").fetchone()[0]
            return changes
        except sqlite3.Error as e:
            raise SQLiteQueryError(f"Failed to execute batch query: {e}") from e

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

        with self.connection(row_factory) as con:
            return con.execute(query, params).fetchall()
