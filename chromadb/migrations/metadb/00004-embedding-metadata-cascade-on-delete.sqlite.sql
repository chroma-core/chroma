-- Rename the old table
ALTER TABLE embedding_metadata RENAME TO old_embedding_metadata;

-- Create the new table with the CASCADE DELETE option
CREATE TABLE embedding_metadata
(
    id INTEGER REFERENCES embeddings ON DELETE CASCADE,
    key          TEXT NOT NULL,
    string_value TEXT,
    int_value    INTEGER,
    float_value  REAL,
    bool_value   INTEGER,
    PRIMARY KEY
    (id, key)
);

    -- Copy the data from the old table to the new one
    INSERT INTO embedding_metadata
    SELECT *
    FROM old_embedding_metadata;

    -- Drop the old table
    DROP TABLE old_embedding_metadata;
