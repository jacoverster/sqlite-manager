import logging
from collections.abc import Mapping
from sqlite3 import Cursor
from typing import Any, Generic, TypeVar, cast

from sqlite_manager.interface import SQLiteInterface, SQLiteQueryError

log = logging.getLogger(__name__)

T = TypeVar("T", bound=Mapping[str, Any])


class CRUDBase(Generic[T]):
    """Base class for CRUD operations on a specific database table.

    This class provides standard Create, Read, Update, Delete operations
    with configurable table name and primary key column.

    Attributes:
        db: The SQLiteInterface instance to use for database operations.
        table_name: The name of the table this CRUD handler manages.
        id_column: The name of the primary key column (defaults to "uid").
    """

    def __init__(
        self, sql_db: SQLiteInterface, table_name: str, id_column: str = "uid"
    ) -> None:
        """Initialize the CRUD base with database connection.

        Args:
            sql_db: The SQLite interface to use.
            table_name: Name of the table to operate on.
            id_column: Name of the primary key column.
        """

        self.db = sql_db
        self.table_name = table_name
        self.id_column = id_column

    @property
    def is_empty(self) -> bool:
        """Check if the table is empty.

        Returns:
            True if the table is empty, False otherwise.
        """

        query = f"SELECT COUNT(*) FROM {self.table_name}"
        result = self.db.fetch_one(query)
        count = result[0] if result else 0

        return count == 0

    def row_factory(self, cursor: Cursor, row: tuple[Any, ...]) -> T:
        """Convert a row from the database into a dictionary.

        This method can be overridden in subclasses to customize the row
        conversion process. The default implementation returns a dictionary.

        Args:
            cursor: The cursor object.
            row: The row to convert.

        Returns:
            A dictionary with column names as keys.
        """

        return cast(
            T, {column[0]: value for column, value in zip(cursor.description, row)}
        )

    def filter_to_sql(
        self, filter: dict[str, Any], operator: str = "AND"
    ) -> tuple[str, tuple[Any, ...]]:
        """Convert a filter dictionary to SQL WHERE clause and parameters.

        Args:
            filter: Dictionary of column-value pairs to filter by.
            operator: Logical operator to use (AND/OR).

        Returns:
            A tuple containing the SQL WHERE clause and parameters.

        Raises:
            ValueError: If filter is empty.
        """

        if not filter:
            raise ValueError("Filter cannot be empty")

        filter_clause = f" {operator} ".join(f"{k} = ?" for k in filter.keys())
        params = tuple(filter.values())

        return filter_clause, params

    def create(self, **kwargs: Any) -> bool:
        """Create a new record in the database.

        Args:
            **kwargs: Column-value pairs to insert into the table.

        Returns:
            True if record created successfully, False otherwise.

        Raises:
            SQLiteQueryError: If database operation fails.
        """

        columns = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" * len(kwargs))
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        params = tuple(kwargs.values())

        try:
            self.db.execute_sql(query, params)
            log.info(f"Inserted new record into {self.table_name}")
            return True
        except SQLiteQueryError as e:
            log.error(f"Failed to insert record into {self.table_name}: {e}")
            return False

    def read(self, filter: dict[str, Any], filter_operator: str = "AND") -> T | None:
        """Read a record from the database.

        Args:
            filter: Dictionary of column-value pairs to filter by.

        Returns:
            The record as a dictionary or None if not found.

        Raises:
            ValueError: If filter is empty.
            SQLiteQueryError: If database operation fails.
        """

        filter_clause, params = self.filter_to_sql(filter, filter_operator)
        query = f"SELECT * FROM {self.table_name} WHERE {filter_clause}"

        try:
            record = self.db.fetch_one(query, params, self.row_factory)
            return cast(T, record) if record else None
        except SQLiteQueryError as e:
            log.error(f"Failed to fetch record from {self.table_name}: {e}")
            return None

    def update(
        self,
        filter: dict[str, Any],
        updates: dict[str, Any],
        filter_operator: str = "AND",
    ) -> bool:
        """Update records in the database that match the filter.

        Args:
            filter: Dictionary of column-value pairs to filter by.
            updates: Dictionary of column-value pairs to update.
            filter_operator: Logical operator to use (AND/OR).

        Returns:
            True if records updated successfully, False otherwise.

        Raises:
            ValueError: If filter is empty.
            SQLiteQueryError: If database operation fails.
        """

        if not updates:
            return True

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        filter_clause, filter_params = self.filter_to_sql(filter, filter_operator)
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE {filter_clause}"
        params = tuple(updates.values()) + filter_params

        try:
            rows_affected = self.db.execute_sql(query, params)
            if rows_affected == 0:
                log.warning(
                    f"No records updated in {self.table_name} with filter {filter}"
                )
                return False
            log.info(f"Updated {rows_affected} record(s) in {self.table_name}")
            return True
        except SQLiteQueryError as e:
            log.error(f"Failed to update records in {self.table_name}: {e}")
            return False

    def delete(self, filter: dict[str, Any], filter_operator: str = "AND") -> bool:
        """Delete records from the database that match the filter.

        Args:
            filter: Dictionary of column-value pairs to filter by.
            filter_operator: Logical operator to use (AND/OR).

        Returns:
            True if records deleted successfully, False otherwise.

        Raises:
            ValueError: If filter is empty.
            SQLiteQueryError: If database operation fails.
        """

        filter_clause, params = self.filter_to_sql(filter, filter_operator)
        query = f"DELETE FROM {self.table_name} WHERE {filter_clause}"

        try:
            rows_affected = self.db.execute_sql(query, params)
            if rows_affected == 0:
                log.warning(
                    f"No records deleted from {self.table_name} with filter {filter}"
                )
                return False
            log.info(f"Deleted {rows_affected} record(s) from {self.table_name}")
            return True
        except SQLiteQueryError as e:
            log.error(f"Failed to delete records from {self.table_name}: {e}")
            return False
