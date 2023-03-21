CREATE TABLE embedding_functions (
    name TEXT PRIMARY KEY,
    dimension INTEGER,
    scalar_type TEXT
);

CREATE TABLE segments (
    id UUID PRIMARY KEY,
    type TEXT,
    embedding_function REFERENCES embedding_functions(name)
);

CREATE TABLE segment_metadata (
    segment REFERENCES segments(id),
    key TEXT,
    value TEXT,
    PRIMARY KEY (segment, key)
);
