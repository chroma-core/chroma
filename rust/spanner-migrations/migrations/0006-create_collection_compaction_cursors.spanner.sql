-- Create collection_compaction_cursors table
-- Tracks compaction state per collection per region
-- Interleaved in collections table for better data locality and query performance

CREATE TABLE IF NOT EXISTS collection_compaction_cursors (
    collection_id STRING(MAX) NOT NULL,
    region STRING(MAX) NOT NULL,
    last_compacted_offset INT64,
    version INT64,
    total_records_post_compaction INT64 DEFAULT (0),
    size_bytes_post_compaction INT64 DEFAULT (0),
    num_versions INT64 DEFAULT (0),
    version_file_name STRING(MAX),
    last_compaction_time_secs TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    oldest_version_ts TIMESTAMP,
    index_schema JSON NOT NULL,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (collection_id, region),
  INTERLEAVE IN PARENT collections

