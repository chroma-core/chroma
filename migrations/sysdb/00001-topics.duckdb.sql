CREATE TYPE scalar_encoding AS ENUM ('INT32', 'FLOAT32');

CREATE TABLE embedding_functions (
    name TEXT PRIMARY KEY,
    dimension INTEGER,
    scalar_encoding TEXT
);

CREATE TABLE topics (
    name TEXT PRIMARY KEY,
    embedding_function TEXT REFERENCES embedding_functions(name)
);

CREATE TABLE topic_metadata (
    topic TEXT REFERENCES topics(name),
    key TEXT,
    value TEXT,
    PRIMARY KEY (topic, key)
);
