CREATE TABLE segments (
    id UUID PRIMARY KEY,
    type TEXT,
    scope TEXT,
    topic TEXT REFERENCES topics(name),
    metadata JSON
);
