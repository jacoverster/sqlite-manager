from pathlib import Path
import bcrypt
import logging
import os
from typing import Any, Literal, TypedDict

from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s | %(filename)s:%(lineno)d | %(message)s",
)
logger = logging.getLogger("user_manager")

# Configuration
PROJECT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = PROJECT_DIR / "userdata.db"
MIGRATIONS_DIR = PROJECT_DIR / "migrations"
BACKUP_DIR = PROJECT_DIR / "backups"


class UserData(TypedDict):
    user_id: int
    username: str
    role: Literal["admin", "user"]
    created_at: str
    last_login: str | None
    activated: bool


USER_DATA_FIELDS = ", ".join(UserData.__annotations__.keys())


class UserManager:
    """User management system using SQLiteManager.

    This class handles user authentication, creation, and management,
    with special handling for the first user (admin).
    """

    def __init__(self):
        """Initialize the user management system with database connection and migrations."""

        self.db = SQLiteInterface(DB_PATH)
        self.migrator = SQLiteMigrator(DB_PATH, MIGRATIONS_DIR, BACKUP_DIR)
        self.migrator.migrate()

    def __del__(self):
        """Clean up database resources when object is destroyed."""

        if hasattr(self, "db") and self.db:
            self.db.close()

    def is_first_user(self) -> bool:
        """Check if this is the first run with no users in database."""

        count = self.db.fetch_one("SELECT COUNT(*) FROM users")

        return count[0] == 0

    def create_user(self, username: str, password: str) -> bool:
        """Create a new user in the database.

        Args:
            username: The unique username
            password: Plain text password that will be hashed

        Returns:
            True if user created successfully, False otherwise
        """

        existing = self.db.fetch_one(
            "SELECT username FROM users WHERE username = ?", (username,)
        )
        if existing:
            logger.warning(f"Failed to create user, '{username}' already exists")
            return False

        query = "INSERT INTO users (username, role, hashed_password) VALUES (?, ?, ?)"
        params = (
            username,
            "admin" if self.is_first_user() else "user",
            bcrypt.hashpw(password.encode(), bcrypt.gensalt()),
        )
        try:
            self.db.execute_sql(query, params)
            logger.info(f"Created new '{params[1]}': {username}")
            return True
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return False

    def authenticate(self, username: str, password: str) -> UserData | None:
        """Authenticate a user with username and password.

        Args:
            username: The username to authenticate
            password: The plain text password

        Returns:
            User data dict if authenticated, None otherwise
        """

        query = "SELECT * FROM users WHERE username = ?"
        user: dict = self.db.fetch_one(
            query, (username,), row_factory=self.db.get_dict_factory()
        )
        if not user or not user.get("activated"):
            logger.warning(
                f"Authentication failed: User '{username}' not found or inactive"
            )
            return None

        if bcrypt.checkpw(password.encode(), user["hashed_password"]):
            query = "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE user_id = ?"
            self.db.execute_sql(query, (user["user_id"],))
            del user["hashed_password"]
            logger.info(f"User authenticated: {username})")
            return user

        logger.warning(f"Authentication failed: Invalid password for '{username}'")
        return None

    def get_user(self, user_id: int) -> UserData | None:
        """Get user data by user ID.

        Args:
            user_id: ID of the user to retrieve

        Returns:
            User data dict if found, None otherwise
        """

        query = f"SELECT {USER_DATA_FIELDS} FROM users WHERE user_id = ?"

        return self.db.fetch_one(
            query, (user_id,), row_factory=self.db.get_dict_factory()
        )

    def list_users(self) -> list[UserData]:
        """List all users in the system.

        Returns:
            A list of user data dictionaries
        """

        query = query = f"SELECT {USER_DATA_FIELDS} FROM users ORDER BY username"

        return self.db.fetch_all(query, row_factory=self.db.get_dict_factory())

    def update_user(self, user_id: int, data: dict[str, Any]) -> bool:
        """Update user data (admin only).

        Args:
            user_id: ID of the user to update
            data: Dictionary of fields to update

        Returns:
            True if update was successful, False otherwise
        """

        allowed_fields = {"username", "password", "activated"}
        invalid_fields = {f for f in data.keys() if f not in allowed_fields}
        if invalid_fields:
            raise ValueError(
                f"Invalid fields: {invalid_fields}. Allowed fields: '{allowed_fields}'"
            )

        updates = {k: v for k, v in data.items() if k in allowed_fields}
        if not updates:
            return False

        if "password" in updates:
            password_bytes = str(updates.pop("password")).encode()
            updates["hashed_password"] = bcrypt.hashpw(password_bytes, bcrypt.gensalt())

        if "username" in updates:
            existing = self.db.fetch_one(
                "SELECT username FROM users WHERE username = ?", (updates["username"],)
            )
            if existing:
                raise ValueError(
                    f"Cannot update username to '{updates['username']}', user already exists"
                )

        set_clauses = [f"{field} = ?" for field in updates.keys()]
        query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ?"
        params = list(updates.values()) + [user_id]

        try:
            self.db.execute_sql(query, params)
            logger.info(f"Updated user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            return False
