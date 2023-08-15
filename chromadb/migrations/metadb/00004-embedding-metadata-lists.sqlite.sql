CREATE TABLE embedding_metadata_lists (
    id INTEGER REFERENCES embeddings(id),
    key TEXT NOT NULL,
    list_index INTEGER NOT NULL,
    string_value TEXT,
    float_value REAL,
    int_value INTEGER,
    bool_value BOOLEAN,
    PRIMARY KEY (id, key, list_index),
    FOREIGN KEY (id, key) REFERENCES embedding_metadata(id, key) ON DELETE CASCADE
);
