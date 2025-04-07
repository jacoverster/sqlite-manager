import logging
from sqlite3 import Cursor, Row
from typing import Any, Generic, TypeVar, cast

from sqlite_manager.interface import SQLiteInterface, SQLiteQueryError

log = logging.getLogger(__name__)

T = TypeVar("T", bound=dict[str, Any])


class CRUDBase(Generic[T]):
    """Base class for CRUD operations on a specific database table.

    This class provides standard Create, Read, Update, Delete operations
    with configurable table name and primary key column.

    Attributes:
        db: The SQLiteInterface instance to use for database operations
        table_name: The name of the table this CRUD handler manages
        id_column: The name of the primary key column (defaults to "uid")
    """

    def __init__(
        self, sql_db: SQLiteInterface, table_name: str, id_column: str = "uid"
    ) -> None:
        """Initialize the CRUD base with database connection.

        Args:
            sql_db: The SQLite interface to use
            table_name: Name of the table to operate on
            id_column: Name of the primary key column
        """

        self.db = sql_db
        self.table_name = table_name
        self.id_column = id_column

    def row_factory(self, cursor: Cursor, row: Row) -> dict[str, Any]:
        """Convert a row from the database into a dictionary.

        This method can be overridden in subclasses to customize the row
        conversion process. The default implementation returns a dictionary.

        Args:
            cursor: The cursor object
            row: The row to convert

        Returns:
            A dictionary with column names as keys
        """

        return {column[0]: value for column, value in zip(cursor.description, row)}

    def create(self, **kwargs: Any) -> bool:
        """Create a new record in the database.

        Args:
            **kwargs: Column-value pairs to insert into the table.

        Returns:
            True if record created successfully, False otherwise.

        Raises:
            SQLiteQueryError: If database operation fails
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

    def read(self, filter: dict[str, Any]) -> dict[str, Any] | None:
        """Read a record from the database.

        Args:
            filter: Dictionary of column-value pairs to filter by

        Returns:
            The record as a dictionary or None if not found.

        Raises:
            ValueError: If filter is empty
            SQLiteQueryError: If database operation fails
        """

        if not filter:
            raise ValueError("Filter cannot be empty")

        filter_clause = " AND ".join(f"{k} = ?" for k in filter.keys())
        query = f"SELECT * FROM {self.table_name} WHERE {filter_clause}"
        params = tuple(filter.values())

        try:
            record = self.db.fetch_one(query, params, self.row_factory)
            return cast(T | None, record)
        except SQLiteQueryError as e:
            log.error(f"Failed to fetch record from {self.table_name}: {e}")
            return None

    def update(self, id: int, updates: dict[str, Any]) -> bool:
        """Update a record in the database.

        Args:
            id: The ID of the record to update
            updates: Dictionary of column-value pairs to update.

        Returns:
            True if record updated successfully, False otherwise.

        Raises:
            SQLiteQueryError: If database operation fails
        """

        if not updates:
            return True

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE {self.id_column} = ?"
        params = tuple(updates.values()) + (id,)

        try:
            rows_affected = self.db.execute_sql(query, params)
            if rows_affected == 0:
                log.warning(f"No records updated in {self.table_name} with ID {id}")
                return False
            log.info(f"Updated record in {self.table_name}: ID={id}")
            return True
        except SQLiteQueryError as e:
            log.error(f"Failed to update record in {self.table_name}: {e}")
            return False

    def delete(self, id: int) -> bool:
        """Delete a record from the database.

        Args:
            id: The ID of the record to delete.

        Returns:
            True if record deleted successfully, False otherwise.

        Raises:
            SQLiteQueryError: If database operation fails
        """

        query = f"DELETE FROM {self.table_name} WHERE {self.id_column} = ?"
        params = (id,)

        try:
            rows_affected = self.db.execute_sql(query, params)
            if rows_affected == 0:
                log.warning(f"No records deleted from {self.table_name} with ID {id}")
                return False
            log.info(f"Deleted record from {self.table_name} with ID {id}")
            return True
        except SQLiteQueryError as e:
            log.error(f"Failed to delete record from {self.table_name}: {e}")
            return False
