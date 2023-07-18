CREATE TABLE embedding_metadata_lists (
    id INTEGER REFERENCES embeddings(id),
    key TEXT NOT NULL,
    string_value TEXT,
    float_value REAL,
    int_value INTEGER,
    FOREIGN KEY (id, key) REFERENCES embedding_metadata(id, key) ON DELETE CASCADE
);
