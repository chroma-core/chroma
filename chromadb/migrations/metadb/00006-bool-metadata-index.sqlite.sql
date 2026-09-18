CREATE INDEX IF NOT EXISTS embedding_metadata_bool_value ON embedding_metadata (key, bool_value) WHERE bool_value IS NOT NULL;
