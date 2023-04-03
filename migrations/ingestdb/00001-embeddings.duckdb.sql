CREATE SEQUENCE seq_embedding_ids START 1;

CREATE TYPE embedding_encoding AS ENUM ('INT32', 'FLOAT32');

CREATE TABLE embeddings (
    topic TEXT NOT NULL,
    id TEXT NOT NULL,
    seq BIGINT NOT NULL DEFAULT nextval('seq_embedding_ids'),
    encoding embedding_encoding NOT NULL,
    vector BLOB NOT NULL,
    metadata JSON
);
