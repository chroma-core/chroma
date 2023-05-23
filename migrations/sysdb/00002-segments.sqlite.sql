CREATE TABLE segments (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    scope TEXT NOT NULL,
    topic TEXT,
    collection TEXT,
    FOREIGN KEY (collection_id) REFERENCES collection(id)
);

CREATE TABLE segment_metadata {
    segment_id TEXT,
    key TEXT NOT NULL,
    text_value TEXT,
    int_value INTEGER,
    float_value REAL,
    PRIMARY KEY (segment_id, key),
    FOREIGN KEY (segment_id) REFERENCES segments(id) ON DELETE CASCADE
};
