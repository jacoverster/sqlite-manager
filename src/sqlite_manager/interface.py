from contextlib import closing, contextmanager
import logging
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from sqlite3 import Cursor
from typing import Any, Generator, TypeVar, overload


log = logging.getLogger(__name__)

T = TypeVar("T", bound=tuple | dict)
# sqlite3 row factories receive raw tuples as their second argument.
RowFactory = Callable[[Cursor, tuple[Any, ...]], T]

# Accepted parameter types for SQLite query binding.
Params = Sequence[Any] | Mapping[str, Any]

# Default SQLite PRAGMA settings for the database connection.
# These settings are chosen to balance performance and safety for web applications.
DEFAULT_PRAGMAS = {
    # Journal mode WAL allows for greater concurrency (many readers + one writer)
    # https://www.sqlite.org/pragma.html#pragma_journal_mode
    "journal_mode": "WAL",
    # Level of database durability, "NORMAL" (sync every 1000 written pages)
    # https://www.sqlite.org/pragma.html#pragma_synchronous
    "synchronous": "NORMAL",
    # Enforce foreign key constraints
    # https://www.sqlite.org/pragma.html#pragma_foreign_keys
    "foreign_keys": "ON",
    # Impose a limit on the WAL file to prevent unlimited growth (64MB)
    # https://www.sqlite.org/pragma.html#pragma_journal_size_limit
    "journal_size_limit": 67108864,
    # Set the global memory map size for potential performance gains (128MB)
    # https://www.sqlite.org/pragma.html#pragma_mmap_size
    "mmap_size": 134217728,
    # Increase the local connection page cache size
    # https://www.sqlite.org/pragma.html#pragma_cache_size
    "cache_size": 2000,
    # Allowed waiting time (in milliseconds) before raising an exception
    # https://www.sqlite.org/pragma.html#pragma_busy_timeout
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
            db_path: Path to the SQLite database file.
            pragmas: Dict of SQLite PRAGMA settings to apply on each connection.
        """

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.pragmas = pragmas or {}

    @staticmethod
    def _bind_params(params: Params | None) -> Params:
        """Returns params, or an empty tuple when params is None."""
        return params if params is not None else ()

    @contextmanager
    def connection(
        self, row_factory: RowFactory[Any] | None = None
    ) -> Generator[sqlite3.Connection, None, None]:
        """Returns a connection to the SQLite database with applied pragmas.

        Commits the transaction if no exceptions occur. The connection is closed
        automatically after use. If an exception occurs, the connection is rolled back.
        If a row_factory is provided, it is used to convert rows to the desired format.

        Args:
            row_factory: Optional callable that converts rows to desired format.

        Yields:
            An active SQLite connection with pragmas applied.
        """

        with closing(sqlite3.connect(self.db_path)) as con, con:
            for pragma, value in self.pragmas.items():
                con.execute(f"PRAGMA {pragma} = {value};")

            if row_factory:
                con.row_factory = row_factory

            yield con

    def execute_sql(self, query: str, params: Params | None = None) -> int:
        """Executes a SQL query and returns the number of changes if requested.

        Args:
            query: SQL query to execute.
            params:  Optional parameters to bind to the query.

        Returns:
            Number of applied changes.

        Raises:
            SQLiteQueryError: If the query execution fails.
        """

        try:
            with self.connection() as con:
                cursor = con.execute(query, self._bind_params(params))
                changes = cursor.execute("select changes()").fetchone()[0]
            return changes
        except sqlite3.Error as e:
            raise SQLiteQueryError(f"Failed to execute query: {e}") from e

    def execute_many(self, query: str, params: Iterable[Params] | None = None) -> int:
        """Executes a SQL query with multiple parameter sets.

        Args:
            query: SQL query to execute.
            params: Optional iterable of parameter sets to bind to the query.

        Returns:
            Number of applied changes.

        Raises:
            SQLiteQueryError: If the query execution fails.
        """

        try:
            with self.connection() as con:
                cursor = con.executemany(query, params if params is not None else ())
                changes = cursor.execute("select changes()").fetchone()[0]
            return changes
        except sqlite3.Error as e:
            raise SQLiteQueryError(f"Failed to execute batch query: {e}") from e

    @overload
    def fetch_one(
        self,
        query: str,
        params: Params | None = ...,
        row_factory: None = ...,
    ) -> tuple | None: ...

    @overload
    def fetch_one(
        self,
        query: str,
        params: Params | None = ...,
        row_factory: RowFactory[T] = ...,
    ) -> T | None: ...

    def fetch_one(
        self,
        query: str,
        params: Params | None = None,
        row_factory: RowFactory[T] | None = None,
    ) -> T | tuple | None:
        """Fetches a single row from the database.

        Args:
            query: SQL query to execute.
            params: Optional parameters to bind to the query.
            row_factory: Optional callable that converts rows to desired format.

        Returns:
            A single row in the format specified by row_factory, or None if no results.
        """

        with self.connection(row_factory) as con:
            return con.execute(query, self._bind_params(params)).fetchone()

    @overload
    def fetch_all(
        self,
        query: str,
        params: Params | None = ...,
        row_factory: None = ...,
    ) -> list[tuple]: ...

    @overload
    def fetch_all(
        self,
        query: str,
        params: Params | None = ...,
        row_factory: RowFactory[T] = ...,
    ) -> list[T]: ...

    def fetch_all(
        self,
        query: str,
        params: Params | None = None,
        row_factory: RowFactory[T] | None = None,
    ) -> list[T] | list[tuple]:
        """Fetches all rows from the database.

        Args:
            query: SQL query to execute.
            params: Optional parameters to bind to the query.
            row_factory: Optional callable that converts rows to desired format.

        Returns:
            A (possibly empty) list of rows in the format specified by row_factory.
        """

        with self.connection(row_factory) as con:
            return con.execute(query, self._bind_params(params)).fetchall()
