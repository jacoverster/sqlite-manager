from pathlib import Path
from typing import Generator
import pytest

from examples.user_mamagement.user_manager import UserManager, UserManagerError
from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator


@pytest.fixture
def test_user_manager(tmp_path: Path) -> Generator[UserManager, None, None]:
    """Fixture to create a temporary SQLite database for testing."""

    temp_db_path = tmp_path / "test_users.sqlite3"
    migrations_dir = Path(__file__).parent / "migrations"

    sql_db = SQLiteInterface(temp_db_path)
    migrator = SQLiteMigrator(
        temp_db_path, migrations_dir=migrations_dir, backup_dir=tmp_path / "backups"
    )
    migrator.migrate()

    yield UserManager(sql_db)

    # Clean up the schema file after the test
    (migrations_dir / "schema_v1.sql").unlink(missing_ok=True)


def test_create_users(test_user_manager: UserManager):
    """Test creating new users."""

    admin_created = test_user_manager.create_user("admin", "Pass123!", role="admin")
    admin_user = test_user_manager.read({"username": "admin"})

    assert admin_created is True
    assert admin_user is not None
    assert admin_user["username"] == "admin"
    assert admin_user["role"] == "admin"

    user_created = test_user_manager.create_user("user", "Pass123!")
    user = test_user_manager.read({"username": "user"})

    assert user_created is True
    assert user is not None
    assert user["username"] == "user"
    assert user["role"] == "user"


def test_create_user_invalid_username(test_user_manager: UserManager):
    """Test creating a user with an invalid username raises an error."""

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("invalid username", "Pass123!")

    assert "Invalid username" in str(excinfo.value)


def test_create_user_invalid_password(test_user_manager: UserManager):
    """Test creating a user with an invalid username raises an error."""

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("validusername", "short")

    assert "Password must be at least 8 characters long" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("validusername", "no_uppercase")

    assert "Password must contain at least one uppercase letter" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("validusername", "NO_LOWERCASE")

    assert "Password must contain at least one lowercase letter" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("validusername", "No_Number")
    assert "Password must contain at least one number" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        test_user_manager.create_user("validusername", "NoSpecialChar123")
    assert "Password must contain at least one special character" in str(excinfo.value)


def test_create_duplicate_user(test_user_manager: UserManager):
    """Test creating a duplicate user raises an error."""

    test_user_manager.create_user("admin", "Pass123!", role="admin")

    with pytest.raises(UserManagerError) as excinfo:
        test_user_manager.create_user("admin", "Pass123!")

    assert "already exists" in str(excinfo.value)


def test_update_user(test_user_manager: UserManager):
    """Test updating user details."""

    test_user_manager.create_user("updateuser", "Pass123!")
    updated = test_user_manager.update(
        {"user_id": 1},
        {"username": "updateduser", "password": "NewPass123!", "activated": False},
    )
    user = test_user_manager.read({"username": "updateduser"})

    assert updated is True
    assert user is not None
    assert user["username"] == "updateduser"
    assert user["activated"] == 0


def test_update_user_invalid_id(test_user_manager: UserManager):
    """Test updating a user with an invalid ID raises an error."""

    updated = test_user_manager.update(
        {"user_id": 999}, {"username": "nonexistentuser"}
    )

    assert updated is False


def test_list_users(test_user_manager: UserManager):
    """Test listing all users."""

    test_user_manager.create_user("user1", "Pass123!")
    test_user_manager.create_user("user2", "Pass123!")

    users = test_user_manager.list_users()

    assert len(users) == 2
    assert any(user["username"] == "user1" for user in users)
    assert any(user["username"] == "user2" for user in users)


def test_authenticate_user(test_user_manager: UserManager):
    """Test user authentication."""

    test_user_manager.create_user("authuser", "Pass123!")
    user = test_user_manager.authenticate("authuser", "Pass123!")

    assert user is not None
    assert user["username"] == "authuser"

    assert test_user_manager.authenticate("authuser", "WrongPassword") is None


def test_authenticate_invalid_user(test_user_manager: UserManager):
    """Test authenticating a non-existent user returns None."""

    user = test_user_manager.authenticate("nonexistentuser", "Pass123!")

    assert user is None
