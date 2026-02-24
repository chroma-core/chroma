-- Separate table for exploded array metadata values.
-- Each array element gets its own row, enabling efficient $contains queries.
-- The existing embedding_metadata table (with its PRIMARY KEY (id, key))
-- remains untouched and continues to store scalar metadata values.

CREATE TABLE IF NOT EXISTS embedding_metadata_array (
    id INTEGER NOT NULL REFERENCES embeddings(id),
    key TEXT NOT NULL,
    string_value TEXT,
    int_value INTEGER,
    float_value REAL,
    bool_value INTEGER
);

CREATE INDEX IF NOT EXISTS embedding_metadata_array_id_key
    ON embedding_metadata_array (id, key);
CREATE INDEX IF NOT EXISTS embedding_metadata_array_key_string
    ON embedding_metadata_array (key, string_value) WHERE string_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS embedding_metadata_array_key_int
    ON embedding_metadata_array (key, int_value) WHERE int_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS embedding_metadata_array_key_float
    ON embedding_metadata_array (key, float_value) WHERE float_value IS NOT NULL;
