CREATE TYPE scalar_encoding AS ENUM ('INT32', 'FLOAT32');

CREATE TABLE embedding_functions (
    name TEXT PRIMARY KEY,
    dimension INTEGER NOT NULL,
    scalar_encoding scalar_encoding NOT NULL
);

CREATE TABLE topics (
    name TEXT PRIMARY KEY,
    embedding_function TEXT REFERENCES embedding_functions(name),
    metadata JSON
);