from pathlib import Path
import logging
import os
from sqlite3 import Cursor, Row
import bcrypt
from typing import Any, Dict, Literal, Optional, TypedDict, cast, override
import re

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator
from sqlite_manager.crud import CRUDBase

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s | %(filename)s:%(lineno)d | %(message)s",
)
log = logging.getLogger(__name__)

# Configuration
PROJECT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = PROJECT_DIR / "userdata.db"
MIGRATIONS_DIR = PROJECT_DIR / "migrations"
BACKUP_DIR = PROJECT_DIR / "backups"


def create_database():
    """Creates the user database and applies migrations."""

    sql_db = SQLiteInterface(DB_PATH)
    migrator = SQLiteMigrator(DB_PATH, MIGRATIONS_DIR, BACKUP_DIR)
    migrator.migrate()

    return sql_db


class UserManagerError(Exception):
    """Base exception for user management errors."""


class UserData(TypedDict):
    """Type definition for user data."""

    user_id: int
    username: str
    role: Literal["admin", "user"]
    created_at: str
    last_login: Optional[str]
    activated: bool


class UserManager(CRUDBase[UserData]):
    """User management system using SQLiteManager.

    This class handles user authentication, creation, and management,
    with special handling for the first user (admin).

    Configuration options:
    - MIN_PASSWORD_LENGTH: Minimum required password length
    - USERNAME_PATTERN: Regex pattern for valid usernames
    - BCRYPT_ROUNDS: Work factor for bcrypt password hashing
    - MAX_LOGIN_ATTEMPTS: Maximum failed login attempts before lockout
    - LOCKOUT_MINUTES: Duration of account lockout in minutes
    """

    # Class constants for configuration
    MIN_PASSWORD_LENGTH = 8
    USERNAME_PATTERN = re.compile(r"^\w{3,30}$")

    def __init__(self, sql_db: SQLiteInterface):
        """Initialize the user management system with database connection.

        Args:
            sql_db: SQLiteInterface instance for database operations
        """
        super().__init__(sql_db, "users", "user_id")

        # Ensure required tables exist
        self._initialize_auth_tables()

    @override
    def row_factory(self, cursor: Cursor, row: Row) -> UserData:
        """Convert a row from the database into a UserData dictionary.

        Args:
            cursor: The database cursor
            row: The database row

        Returns:
            UserData dictionary with typed fields
        """
        row_fields = (column[0] for column in cursor.description)
        row_dict = {key: value for key, value in zip(row_fields, row)}
        annotations = UserData.__annotations__
        user_data = {k: v for k, v in row_dict.items() if k in annotations}

        return cast(UserData, user_data)

    def _validate_username(self, username: str) -> bool:
        """Validate username meets required pattern.'

        Args:
            username: The username to validate

        Returns:
            True if username matches pattern, False otherwise"""

        return self.USERNAME_PATTERN.match(username)

    def _validate_password(self, password: str) -> bool:
        """Validate password meets security requirements.

        Args:
            password: The password to validate

        Returns:
            True if password meets requirements, False otherwise
        """
        # Minimum length check
        if len(password) < self.MIN_PASSWORD_LENGTH:
            return False

        # Basic complexity checks
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)

        return has_upper and has_lower and has_digit

    def create_user(self, username: str, password: str, role: str = "user") -> bool:
        """Create a new user in the database.

        Args:
            username: The unique username
            password: Plain text password that will be hashed
            role: The user role (defaults to "user")

        Returns:
            True if user created successfully, False otherwise

        Raises:
            UserExistsError: If username already exists
            ValueError: If username or password is invalid
        """

        if not self._validate_username(username):
            raise ValueError(
                "Invalid username format: must be 3-30 alphanumeric characters or underscores"
            )

        if not self._validate_password(password):
            raise ValueError(
                f"Password must be at least {self.MIN_PASSWORD_LENGTH} characters "
                "with at least one uppercase letter, one lowercase letter, and one digit"
            )

        existing = self.read({"username": username})
        if existing:
            raise UserManagerError(
                f"Failed to create user, '{username}' already exists"
            )

        return self.create(
            username=username,
            role=role,
            hashed_password=bcrypt.hashpw(password.encode(), bcrypt.gensalt()),
        )

    def authenticate(self, username: str, password: str) -> UserData:
        """Authenticate a user with username and password.

        Args:
            username: The username to authenticate
            password: The plain text password

        Returns:
            User data if authenticated

        Raises:
            InvalidCredentialsError: If authentication fails
            AccountLockedError: If account is locked due to too many failed attempts
        """

        user = self.read({"username": username})
        if not user or not user.get("activated"):
            log.warning(
                f"Authentication failed: User '{username}' not found or inactive"
            )
            return None

        if bcrypt.checkpw(password.encode(), user["hashed_password"]):
            self.update(user["user_id"], {"last_login": "CURRENT_TIMESTAMP"})
            log.info(f"User authenticated: {username}")
            return user

        log.warning(f"Authentication failed for '{username}'")
        return None

    def list_users(self) -> list[UserData]:
        """List all users in the system.

        Returns:
            A list of user data dictionaries
        """

        query = query = "SELECT * FROM users ORDER BY username"

        return self.db.fetch_all(query, row_factory=self.row_factory)

    def update_user(
        self,
        user_id: int,
        username: str | None = None,
        password: str | None = None,
        activated: bool | None = None,
    ) -> bool:
        """Update user data.

        Args:
            user_id: ID of the user to update
            username: New username (if changing)
            password: New password (if changing)
            activated: New activation status (if changing)

        Returns:
            True if update was successful, False otherwise

        Raises:
            UserExistsError: If new username already exists
            ValueError: If new username or password is invalid
        """
        updates: Dict[str, Any] = {}

        if username is not None:
            if not self._validate_username(username):
                raise ValueError("Invalid username format")

            existing = self.read({"username": username})
            if existing:
                raise UserManagerError(
                    f"Cannot update username to '{username}', user already exists"
                )
            updates["username"] = username

        if password is not None:
            if not self._validate_password(password):
                raise ValueError(
                    f"Password must be at least {self.MIN_PASSWORD_LENGTH} characters "
                    "with at least one uppercase letter, one lowercase letter, and one digit"
                )
            updates["hashed_password"] = bcrypt.hashpw(
                password.encode(), bcrypt.gensalt()
            )

        if activated is not None:
            updates["activated"] = activated

        if not updates:
            raise ValueError("No updates provided")

        return self.update(user_id, updates)
