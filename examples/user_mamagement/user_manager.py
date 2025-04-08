import logging
from sqlite3 import Cursor, Row
import bcrypt
import sys
from typing import Literal, Optional, TypedDict, cast

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override
import re

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.crud import CRUDBase

log = logging.getLogger(__name__)


class UserManagerError(Exception):
    """Base exception for user management errors."""


class UserData(TypedDict):
    """Type definition for user data.

    This model should match the database schema.
    """

    user_id: int
    username: str
    role: Literal["admin", "user"]
    hashed_password: str
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
    """

    MIN_PASSWORD_LENGTH = 8
    USERNAME_PATTERN = re.compile(r"^\w{3,30}$")

    def __init__(self, sql_db: SQLiteInterface):
        """Initialize the user management system with database connection.

        Args:
            sql_db: SQLiteInterface instance for database operations
        """

        super().__init__(sql_db, "users", "user_id")

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
            True if username matches pattern, False otherwise
        """

        if not self.USERNAME_PATTERN.match(username):
            raise ValueError(
                "Invalid username format: must be 3-30 alphanumeric characters or underscores"
            )

    def _validate_password(self, password: str) -> bool:
        """Validate password meets security requirements.

        Args:
            password: The password to validate

        Returns:
            True if password meets requirements, False otherwise
        """

        if len(password) < self.MIN_PASSWORD_LENGTH:
            raise ValueError(
                f"Password must be at least {self.MIN_PASSWORD_LENGTH} characters long"
            )

        has_upper = any(c.isupper() for c in password)
        if not has_upper:
            raise ValueError("Password must contain at least one uppercase letter")

        has_lower = any(c.islower() for c in password)
        if not has_lower:
            raise ValueError("Password must contain at least one lowercase letter")

        has_digit = any(c.isdigit() for c in password)
        if not has_digit:
            raise ValueError("Password must contain at least one number")

        has_special = any(not c.isalnum() for c in password)
        if not has_special:
            raise ValueError("Password must contain at least one special character")

        return True

    def is_empty(self) -> bool:
        """Check if the user table is empty.

        Returns:
            True if the user table is empty, False otherwise
        """

        query = "SELECT COUNT(*) FROM users"
        count = self.db.fetch_one(query)[0]

        return count == 0

    def create_user(
        self, username: str, password: str, role: str = "user", validate: bool = True
    ) -> bool:
        """Create a new user in the database.

        Args:
            username: The unique username
            password: Plain text password that will be hashed
            role: The user role (defaults to "user")
            validate: Whether to validate username and password (default: True)

        Returns:
            True if user created successfully, False otherwise

        Raises:
            UserExistsError: If username already exists
            ValueError: If username or password is invalid
        """

        if validate:
            self._validate_username(username)
            self._validate_password(password)

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

    def authenticate(self, username: str, password: str) -> UserData | None:
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
            query = "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE user_id = ?"
            self.db.execute_sql(query, (user["user_id"],))
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

        updates = {}
        if username is not None:
            self._validate_username(username)
            existing = self.read({"username": username})
            if existing:
                raise UserManagerError(
                    f"Cannot update username to '{username}', user already exists"
                )
            updates["username"] = username

        if password is not None:
            self._validate_password(password)
            updates["hashed_password"] = bcrypt.hashpw(
                password.encode(), bcrypt.gensalt()
            )

        if activated is not None:
            updates["activated"] = activated

        if not updates:
            raise ValueError("No updates provided")

        return self.update({"user_id": user_id}, updates)
