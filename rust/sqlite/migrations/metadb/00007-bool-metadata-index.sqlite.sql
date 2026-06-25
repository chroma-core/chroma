CREATE INDEX IF NOT EXISTS embedding_metadata_bool_value ON embedding_metadata (key, bool_value) WHERE bool_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS embedding_metadata_array_key_bool ON embedding_metadata_array (key, bool_value) WHERE bool_value IS NOT NULL;
