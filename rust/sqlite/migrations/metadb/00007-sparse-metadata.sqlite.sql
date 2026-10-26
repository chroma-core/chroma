-- Authoritative sparse metadata, including empty vectors and optional tokens.
-- Local sparse admission remains guarded until native Search is implemented.
CREATE TABLE embedding_metadata_sparse (
    id INTEGER NOT NULL REFERENCES embeddings(id),
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id, key)
);
