CREATE TABLE collections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    topic TEXT NOT NULL
);

CREATE TABLE collection_metadata {
    collection_id TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    text_value TEXT,
    int_value INTEGER,
    float_value REAL,
    PRIMARY KEY (collection_id, key),
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
};
