from pathlib import Path
import sqlite3

import pytest
from sqlite_manager.interface import DEFAULT_PRAGMAS, RowFactory, SQLiteInterface


@pytest.fixture
def dict_row_factory() -> RowFactory:
    """Fixture for creating a row factory that returns rows as dictionaries."""

    def row_factory(cursor, row) -> dict:
        return {col[0]: row[i] for i, col in enumerate(cursor.description)}

    return row_factory


def test_init_sqlite_interface(tmp_path: Path):
    """Test the initialization of the SQLiteInterface."""

    db_path = tmp_path / "test.db"
    interface = SQLiteInterface(db_path)
    assert interface.db_path == db_path
    assert interface.pragmas == DEFAULT_PRAGMAS

    with interface.connection() as con:
        assert isinstance(con, sqlite3.Connection)

    assert db_path.exists()


def test_execute_sql(test_db: SQLiteInterface):
    """Test the execution of SQL queries."""

    test_db.execute_sql("CREATE TABLE test_table (test_column);")
    changes = test_db.execute_sql("INSERT INTO test_table VALUES ('test_value')")
    assert changes == 1

    # Check that the row exists
    (result,) = test_db.fetch_one("SELECT * FROM test_table")  # type: ignore
    assert result == "test_value"

    # Delete the table and check that it no longer exists
    test_db.execute_sql("DROP TABLE test_table;")
    with test_db.connection() as con:
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        assert ("test_table",) not in tables


def test_execute_many(test_db: SQLiteInterface):
    """Test the execution of multiple SQL queries."""

    test_db.execute_sql("CREATE TABLE test_table (test_column);")
    test_db.execute_many(
        "INSERT INTO test_table VALUES (?)", [("test_value_1",), ("test_value_2",)]
    )

    # Check that the rows exist
    results = test_db.fetch_all("SELECT * FROM test_table")
    assert len(results) == 2
    assert ("test_value_1",) in results
    assert ("test_value_2",) in results


def test_fetch_one(test_db: SQLiteInterface, dict_row_factory: RowFactory):
    """Test fetching a single row from the database."""

    with test_db.connection() as con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"
        )
        con.execute("INSERT INTO users (name) VALUES ('Alice')")

    query = "SELECT * FROM users WHERE name = ?"
    params = ("Alice",)

    result = test_db.fetch_one(query, params)
    assert result is not None
    assert result[0] == 1
    assert result[1] == "Alice"

    # Test with sqlite.Row factory
    result = test_db.fetch_one(query, params, row_factory=sqlite3.Row)
    assert result is not None
    assert result["id"] == 1
    assert result["name"] == "Alice"

    # Test with dict factory
    result = test_db.fetch_one(query, params, row_factory=dict_row_factory)
    assert result is not None
    assert result.get("id") == 1
    assert result.get("name") == "Alice"


def test_fetch_all(test_db: SQLiteInterface, dict_row_factory: RowFactory):
    """Test fetching all rows from the database."""

    with test_db.connection() as con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"
        )
        con.execute("INSERT INTO users (name) VALUES ('Alice')")
        con.execute("INSERT INTO users (name) VALUES ('Bob')")

    query = "SELECT * FROM users"

    results = test_db.fetch_all(query)
    assert len(results) == 2
    assert results[0][1] == "Alice"
    assert results[1][1] == "Bob"

    # Test with sqlite.Row factory
    results = test_db.fetch_all(query, row_factory=sqlite3.Row)
    assert results[0]["id"] == 1
    assert results[0]["name"] == "Alice"
    assert results[1]["id"] == 2
    assert results[1]["name"] == "Bob"

    # Test with dict factory
    results = test_db.fetch_all(query, row_factory=dict_row_factory)
    assert results[0].get("name") == "Alice"
    assert results[1].get("name") == "Bob"
