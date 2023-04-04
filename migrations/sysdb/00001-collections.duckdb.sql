CREATE TYPE scalar_encoding AS ENUM ('INT32', 'FLOAT32');

CREATE TABLE embedding_functions (
    name TEXT PRIMARY KEY,
    dimension INTEGER NOT NULL,
    scalar_encoding scalar_encoding NOT NULL
);

CREATE TABLE collections (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    embedding_function TEXT REFERENCES embedding_functions(name),
    metadata JSON
);