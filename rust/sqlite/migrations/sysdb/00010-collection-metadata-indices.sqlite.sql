CREATE INDEX IF NOT EXISTS collection_metadata_int_value ON collection_metadata (key, int_value) WHERE int_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS collection_metadata_float_value ON collection_metadata (key, float_value) WHERE float_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS collection_metadata_string_value ON collection_metadata (key, str_value) WHERE str_value IS NOT NULL;
CREATE INDEX IF NOT EXISTS collection_metadata_bool_value ON collection_metadata (key, bool_value) WHERE bool_value IS NOT NULL;
