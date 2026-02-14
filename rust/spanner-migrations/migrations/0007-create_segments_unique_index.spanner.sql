-- Create unique index on collection_segments (id) to ensure segment IDs are unique across all collections and regions

CREATE UNIQUE INDEX segment_unique_idx ON collection_segments (id);

