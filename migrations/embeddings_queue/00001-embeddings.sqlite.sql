CREATE TABLE embeddings_queue (
    seq_id INTEGER PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    topic TEXT NOT NULL,
    id TEXT NOT NULL,
    is_delete INTEGER,
    vector BLOB,
    encoding TEXT,
    metadata TEXT
);
