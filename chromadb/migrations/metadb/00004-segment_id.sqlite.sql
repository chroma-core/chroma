-- Add segment_id to embedding_metadata table
ALTER TABLE embedding_metadata ADD COLUMN segment_id TEXT;

-- TODO we can merge this with the table copy below
UPDATE embedding_metadata
SET segment_id = (
    SELECT segment_id
    FROM embeddings
    WHERE embedding_metadata.id = embeddings.id
);

ALTER TABLE embedding_metadata RENAME TO embedding_metadata_backup;

CREATE TABLE embedding_metadata_new (
                                        id INTEGER REFERENCES embeddings(id),
                                        segment_id TEXT NOT NULL REFERENCES embeddings(segment_id),
                                        key TEXT NOT NULL,
                                        string_value TEXT,
                                        int_value INTEGER,
                                        bool_value INTEGER,
                                        float_value REAL,
                                        PRIMARY KEY (id, segment_id, key)
);

INSERT INTO embedding_metadata_new(id, segment_id, key, string_value, int_value, float_value)
SELECT id, segment_id, key, string_value, int_value, float_value FROM embedding_metadata_backup;

DROP TABLE embedding_metadata_backup;

ALTER TABLE embedding_metadata_new RENAME TO embedding_metadata;
