CREATE TABLE IF NOT EXISTS database_metadata (
    database_id TEXT NOT NULL,
    key TEXT NOT NULL,
    str_value TEXT,
    int_value INTEGER,
    float_value REAL,
    bool_value INTEGER,
    PRIMARY KEY (database_id, key),
    FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
);
