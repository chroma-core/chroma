CREATE TABLE embedding_metadata_temp (
                                    id INTEGER REFERENCES embeddings(id) ON DELETE CASCADE NOT NULL,
                                    key TEXT NOT NULL,
                                    string_value TEXT,
                                    int_value INTEGER,
                                    float_value REAL,
                                    bool_value INTEGER,
                                    PRIMARY KEY (id, key)
);

INSERT INTO embedding_metadata_temp
SELECT id, key, string_value, int_value, float_value, bool_value
FROM embedding_metadata;

DROP TABLE embedding_metadata;

ALTER TABLE embedding_metadata_temp RENAME TO embedding_metadata;
