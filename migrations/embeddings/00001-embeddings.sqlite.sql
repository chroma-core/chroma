CREATE TABLE embeddings (
    seq_id INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    id TEXT NOT NULL,
    vector BLOB NOT NULL,
    UNIQUE (topic, id)
);

CREATE TABLE embeddings_metadata {
    embedding INTEGER NOT NULL,
    string_value TEXT,
    int_value INTEGER,
    float_value REAL,
    FOREIGN KEY (embedding) REFERENCES embeddings (seq_id) ON DELETE CASCADE
}
