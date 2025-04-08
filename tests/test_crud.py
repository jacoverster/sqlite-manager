import pytest
from sqlite_manager.crud import CRUDBase
from pathlib import Path
from sqlite_manager.interface import SQLiteInterface, SQLiteQueryError
from typing import TypedDict, Optional


@pytest.fixture
def test_db(tmp_path: Path) -> SQLiteInterface:
    """Create a test database with a sample table."""

    db_path = tmp_path / "test_crud.db"
    interface = SQLiteInterface(db_path)

    # Create a test table
    interface.execute_sql(
        """
    CREATE TABLE test_items (
      uid INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      value INTEGER
    );
    """
    )
    # Insert some sample data
    interface.execute_many(
        "INSERT INTO test_items (name, value) VALUES (?, ?)",
        [("item1", 100), ("item2", 200)],
    )
    return interface


@pytest.fixture
def test_crud_handler(test_db: SQLiteInterface) -> CRUDBase:
    """Create a CRUDBase instance for testing."""

    return CRUDBase(test_db, "test_items")


@pytest.fixture
def test_custom_crud_handler(test_db: SQLiteInterface) -> CRUDBase:
    """Test custom CRUDBase handler with TypedDict."""

    class CombinedItem(TypedDict):
        uid: int
        name: str
        value: Optional[int]
        combined: Optional[str]

    class CustomCRUD(CRUDBase[CombinedItem]):
        """Custom CRUDBase with overridden row_factory."""

        def row_factory(self, cursor, row):
            base_dict = super().row_factory(cursor, row)
            # Add a calculated field
            if "name" in base_dict and "value" in base_dict:
                base_dict["combined"] = f"{base_dict['name']}_{base_dict['value']}"
            return base_dict

    return CustomCRUD(test_db, "test_items")


def test_crud_initialization(test_db: SQLiteInterface):
    """Test CRUDBase initialization with different configurations."""

    # Test with default id_column
    crud_default = CRUDBase(test_db, "test_items")
    assert crud_default.table_name == "test_items"
    assert crud_default.id_column == "uid"  # Default value

    # Test with custom id_column
    crud_custom = CRUDBase(test_db, "test_items", id_column="custom_id")
    assert crud_custom.table_name == "test_items"
    assert crud_custom.id_column == "custom_id"


def test_filter_to_sql(test_crud_handler: CRUDBase):
    """Test the filter_to_sql method for generating WHERE clauses."""

    # Test basic single filter
    filter_clause, params = test_crud_handler.filter_to_sql({"name": "item1"})
    assert filter_clause == "name = ?"
    assert params == ("item1",)

    # Test multiple filters with default AND operator
    filter_clause, params = test_crud_handler.filter_to_sql(
        {"name": "item1", "value": 100}
    )
    # Since dictionaries don't guarantee order, check both possibilities
    assert "name = ?" in filter_clause and "value = ?" in filter_clause
    assert "AND" in filter_clause
    assert len(params) == 2
    assert set(params) == {"item1", 100}

    # Test multiple filters with OR operator
    filter_clause, params = test_crud_handler.filter_to_sql(
        {"name": "item1", "value": 100}, operator="OR"
    )
    assert "name = ?" in filter_clause and "value = ?" in filter_clause
    assert "OR" in filter_clause
    assert len(params) == 2
    assert set(params) == {"item1", 100}

    # Test with different value types
    filter_dict = {"name": "test", "active": True, "count": 5}
    filter_clause, params = test_crud_handler.filter_to_sql(filter_dict)
    for key in filter_dict.keys():
        assert f"{key} = ?" in filter_clause
    assert len(params) == 3
    assert "test" in params
    assert True in params
    assert 5 in params

    # Test that empty filter raises ValueError
    with pytest.raises(ValueError, match="Filter cannot be empty"):
        test_crud_handler.filter_to_sql({})


def test_row_factory(test_crud_handler: CRUDBase, test_db: SQLiteInterface):
    """Test the default row_factory method."""

    with test_db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM test_items WHERE name = 'item1'")
        row = cursor.fetchone()

        result = test_crud_handler.row_factory(cursor, row)

        assert result["name"] == "item1"
        assert result["value"] == 100


def test_create_and_read(test_crud_handler: CRUDBase):
    """Test creating and then reading a record."""

    # Create a new record
    success = test_crud_handler.create(name="new_item", value=123)
    assert success is True

    # Read the created record
    record = test_crud_handler.read({"name": "new_item"})
    assert record is not None
    assert record["value"] == 123


def test_read_nonexistent(test_crud_handler: CRUDBase):
    """Test reading a nonexistent record returns None."""

    record = test_crud_handler.read({"name": "does_not_exist"})
    assert record is None


def test_read_empty_filter(test_crud_handler: CRUDBase):
    """Test that reading with an empty filter raises ValueError."""

    with pytest.raises(ValueError, match="Filter cannot be empty"):
        test_crud_handler.read({})


def test_update(test_crud_handler: CRUDBase):
    """Test updating a record."""

    # First read a record to get its ID
    record = test_crud_handler.read({"name": "item1"})
    assert record is not None

    # Update the record
    success = test_crud_handler.update({"uid": record["uid"]}, {"value": 999})
    assert success is True

    # Verify the update
    updated = test_crud_handler.read({"name": "item1"})
    assert updated["value"] == 999


def test_update_nonexistent(test_crud_handler: CRUDBase):
    """Test updating a nonexistent record returns False."""

    success = test_crud_handler.update({"uid": 999}, {"value": 999})
    assert success is False


def test_delete(test_crud_handler: CRUDBase):
    """Test deleting a record."""

    # First read a record to get its ID
    record = test_crud_handler.read({"name": "item2"})
    assert record is not None

    # Delete the record
    success = test_crud_handler.delete({"name": "item2"})
    assert success is True

    # Verify the deletion
    deleted = test_crud_handler.read({"name": "item2"})
    assert deleted is None


def test_delete_nonexistent(test_crud_handler: CRUDBase):
    """Test deleting a nonexistent record returns False."""

    success = test_crud_handler.delete({"uid": 999})
    assert success is False


def test_error_handling(test_crud_handler: CRUDBase, monkeypatch, caplog):
    """Test error handling in CRUD methods."""

    def mock_error(*args, **kwargs):
        raise SQLiteQueryError("Test error")

    # Test create error handling
    monkeypatch.setattr(test_crud_handler.db, "execute_sql", mock_error)
    success = test_crud_handler.create(name="error_test")
    assert success is False
    assert "Failed to insert record into test_items" in caplog.text

    # Clear logs between tests
    caplog.clear()

    # Test read error handling
    monkeypatch.setattr(test_crud_handler.db, "fetch_one", mock_error)
    result = test_crud_handler.read({"name": "test"})
    assert result is None
    assert "Failed to fetch record from test_items" in caplog.text


def test_custom_row_factory(test_custom_crud_handler: CRUDBase):
    """Test a custom row_factory implementation."""

    # Read a record using the custom row_factory
    record = test_custom_crud_handler.read({"name": "item1"})

    assert record is not None
    assert "combined" in record
    assert record["combined"] == "item1_100"
