-- Create collection_segments table
-- Tracks segments per collection per region
-- Interleaved in collection_compaction_cursors for better data locality and query performance

CREATE TABLE IF NOT EXISTS collection_segments (
    collection_id STRING(MAX) NOT NULL,
    region STRING(MAX) NOT NULL,
    id STRING(MAX) NOT NULL,
    type STRING(MAX) NOT NULL,
    scope STRING(MAX) NOT NULL,
    is_deleted BOOL DEFAULT (false),
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    file_paths JSON
) PRIMARY KEY (collection_id, region, id),
  INTERLEAVE IN PARENT collection_compaction_cursors

