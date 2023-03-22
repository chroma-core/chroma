CREATE TABLE segments (
    id UUID PRIMARY KEY,
    type TEXT,
    scope TEXT,
    embedding_function TEXT
);

CREATE TABLE segment_metadata (
    segment UUID REFERENCES segments(id),
    key TEXT,
    value TEXT,
    PRIMARY KEY (segment, key)
);
