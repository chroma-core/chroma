CREATE TABLE segments (
    id UUID PRIMARY KEY,
    type TEXT,
    scope TEXT,
    topic TEXT REFERENCES topics(name),
);

CREATE TABLE segment_metadata (
    segment UUID REFERENCES segments(id),
    key TEXT,
    value TEXT,
    PRIMARY KEY (segment, key)
);
