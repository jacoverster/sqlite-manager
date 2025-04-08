# SQLite Manager

A lightweight, type-safe Python library for managing SQLite databases with migrations and CRUD operations.

[![PyPI version](https://img.shields.io/pypi/v/sqlite-manager.svg)](https://pypi.org/project/sqlite-manager/)
[![Python versions](https://img.shields.io/pypi/pyversions/sqlite-manager.svg)](https://pypi.org/project/sqlite-manager/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Zero-dependency** and lightweight implementation.
- **Type-safe interface** with full typing support for modern Python development
- **Database migrations** for versioned database schema management
- **CRUD abstraction** to simplify database operations
- **Robust error handling** for all database operations
- **Flexible row factories** to control how database rows are returned
- **Optional Pydantic integration** for model validation and serialization
- **Optimized SQLite configuration** with sensible defaults
- **Example integrations** including a fully-featured user management system

## Installation

With pip:

```bash
pip install sqlite-manager
```

With uv (recommended for faster, more reliable dependency resolution):

```bash
uv pip install sqlite-manager
```

For user management functionality (includes bcrypt dependency):

```bash
pip install "sqlite-manager[users]"
# Or with uv
uv pip install "sqlite-manager[users]"
```

## Quick Start

### Basic SQLite Operations

# SQLiteInterface

A Python wrapper for SQLite database operations that simplifies connection management and query execution. It provides safe transaction handling with automatic connection lifecycle management, parameter binding, and flexible row formatting options.

```python
from pathlib import Path
from sqlite_manager.interface import SQLiteInterface

# Create a database interface
db = SQLiteInterface(Path("my_database.db"))

# Execute SQL statements
db.execute_sql("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)")
db.execute_sql("INSERT INTO items (name) VALUES (?)", ("Test Item",))

# Fetch data
items = db.fetch_all("SELECT * FROM items")
item = db.fetch_one("SELECT * FROM items WHERE name = ?", ("Test Item",))
```

### CRUD Operations

The `CRUDBase` class provides standard Create, Read, Update, Delete operations with configurable table name and primary key column. A default dictionary `row_factory` can be overridden to to customize row
conversion.

```python
from sqlite_manager.crud import CRUDBase

# Create a CRUD handler for a table
items_crud = CRUDBase(db, table_name="items", id_column="item_id")

# Create a record
items_crud.create(name="New Item", value=123)

# Read a record
item = items_crud.read({"name": "New Item"})

# Update a record
items_crud.update({"name": "New Item"}, {"value": 456})

# Delete a record
items_crud.delete({"item_id": item["item_id"]})
```

### Database Migrations

A database migrator for SQLite databases. A migrations table is created to version the database and record migrations.

Migrations are SQL scripts that are executed in order to update the database schema.
SQL scripts are stored in the migrations directory and should be named starting with
migration_ and a number, for eg. migration_0001.sql.

The migrator will execute migration scripts with a version number greater than the
current database version in numeric order. A backup of the database is created before migration and is stored in the backups directory.

Includes a [migration template](examples/migration_template.sql) to for creating migration instructions.

See the [user management example](examples/user_mamagement) for an example of how to use the migrator.

```python
from sqlite_manager.migrator import SQLiteMigrator

# First create a migration directory with the initial migration.
# See examples/user_management.py

# Set up the migrator
migrator = SQLiteMigrator(
    Path("my_database.db"),
    migrations_dir=Path("migrations"),
    backup_dir=Path("backups")
)

# Apply all pending migrations
migrator.migrate()

# Get current database version
version = migrator.get_database_version()

# Restore from a backup
migrator.restore(Path("backups/backup_v1_2025-04-07.sqlite3"))
```

## Advanced Usage

### Type-Safe CRUD Operations

```python
from typing import TypedDict, Optional, override
from sqlite_manager.crud import CRUDBase

# Define your data model
class Product(TypedDict):
    id: int
    name: str
    price: float
    description: Optional[str]
    price_with_tax: Optional[float]

# Create a type-safe CRUD handler
class ProductCRUD(CRUDBase[Product]):  # Generic type support
    """Products CRUD with custom row processing"""

    @override
    def row_factory(self, cursor, row):
        base_dict = super().row_factory(cursor, row)
        # Add computed properties or transform data
        if "price" in base_dict:
            base_dict["price_with_tax"] = base_dict["price"] * 1.2
        return base_dict

# Use your typed CRUD class
product_crud = ProductCRUD(db, "products", id_column="id")
```

### Custom Row Factories

```python
def dict_row_factory(cursor, row):
    """Convert rows to dictionaries with lowercase keys"""
    return {col[0].lower(): row[i] for i, col in enumerate(cursor.description)}

# Use with fetch operations
users = db.fetch_all(
    "SELECT * FROM users WHERE active = ?",
    (True,),
    row_factory=dict_row_factory
)
```

## Complete examples

To run the examples install the optional dependencies:

```bash
pip install "sqlite-manager[examples]"
```

### [Use management](examples/user_mamagement)

See the [example notebook](examples/user_mamagement/user_manager_example.ipynb) for a complete demonstration.


The package includes a complete user management system example:

```python
from sqlite_manager.interface import SQLiteInterface
from sqlite_manager.migrator import SQLiteMigrator
from examples.user_mamagement.user_manager import UserManager

# Set up the database
db = SQLiteInterface(Path("users.db"))
migrator = SQLiteMigrator(
    Path("users.db"),
    migrations_dir=Path("migrations"),
    backup_dir=Path("backups")
)
migrator.migrate()

# Initialize user manager
user_manager = UserManager(db)

# Create users
user_manager.create("admin", "SecureP@ss123", role="admin")
user_manager.create("user", "UserP@ss456")

# Authenticate
user = user_manager.authenticate("admin", "SecureP@ss123")

# List all users
all_users = user_manager.list_users()
```

### [Pydantic validation](examples/pydantic_validation)

See the [example notebook](examples/pydantic_validation/pydantic_example.ipynb) for a complete demonstration.

```python
from pydantic import BaseModel, Field, model_validator
from typing import Optional
from sqlite_manager.crud import CRUDBase

# Define your Pydantic model
class Product(BaseModel):
    product_id: int = Field(default=None)
    name: str
    price: float
    description: Optional[str] = None
    price_with_tax: Optional[float] = None

    @model_validator(mode="after")
    def validate_price_with_tax(self) -> "Product":
        """Validate the price with tax calculation"""
        if self.price is not None and self.price_with_tax is None:
            self.price_with_tax = self.price * 1.2
        return self

# Create a CRUD handler with Pydantic model support
class PydanticCRUD(CRUDBase[Product]):  # Generic type support
    """CRUD handler that returns Pydantic models"""

    def __init__(self, sql_db, table_name="products", id_column="product_id"):
        super().__init__(sql_db, table_name, id_column)

    @override
    def row_factory(self, cursor, row) -> Product:
        """Returns a Pydantic model instance from a row"""
        fields = [column[0] for column in cursor.description]

        return Product(**dict(zip(fields, row, strict=True)))

    def create_from_model(self, product: Product) -> Product:
        """Create a new product from a Pydantic model"""

        return super().create(**product.model_dump(exclude_unset=True))

# Use your Pydantic CRUD class
product_crud = PydanticCRUD(db)
product_crud.create(name="Example", price=100.0)

# Create from a model
new_product = Product(name="Example Product 2", price=200.0)
product_crud.create(**new_product.model_dump(exclude_unset=True))

product: Product = product_crud.read({"product_id": 1})  # Returns a Product instance
print(product)
```


## Project Structure

```
sqlite-manager/
├── src/
│   └── sqlite_manager/
│       ├── interface.py      # Core SQLite interface
│       ├── crud.py           # CRUD operations
│       └── migrator.py       # Database migrations
├── examples/                 # Example implementations
│   └── user_mamagement/      # User management example
│   └── pydantic_validation   # Pydantic validation example
├── tests/                    # Test suite
└── pyproject.toml            # Project configuration
```

## Development

Clone the repository and install development dependencies:

```bash
# With pip
git clone https://github.com/your-username/sqlite-manager.git
cd sqlite-manager
pip install ".[dev]"

# With uv (recommended)
git clone https://github.com/your-username/sqlite-manager.git
cd sqlite-manager
uv pip install ".[dev]"
```

Run tests:

```bash
pytest
# Or with uv
uv run pytest
```

## License

MIT License - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request