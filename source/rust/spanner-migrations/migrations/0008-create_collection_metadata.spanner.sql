-- Create collection_metadata table
-- Stores key-value metadata for collections
-- Interleaved in collections for better data locality and query performance

CREATE TABLE IF NOT EXISTS collection_metadata (
    collection_id STRING(MAX) NOT NULL,
    key STRING(MAX) NOT NULL,
    str_value STRING(MAX),
    int_value INT64,
    float_value FLOAT64,
    bool_value BOOL,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (collection_id, key),
  INTERLEAVE IN PARENT collections

