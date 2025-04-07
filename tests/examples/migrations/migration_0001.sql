-- Migration v0001: Initial users table creation

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  role TEXT CHECK (role IN ('user', 'admin')),
  hashed_password TEXT NOT NULL,
  last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  activated BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create indexes for performance optimization
-- Index on username for fast lookups during authentication
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- Index on role for filtering users by role
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

-- Index on activated status for filtering active/inactive users
CREATE INDEX IF NOT EXISTS idx_users_activated ON users(activated);
