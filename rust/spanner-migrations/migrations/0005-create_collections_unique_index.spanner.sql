-- Create unique index on collections (database_id, name) to ensure collection names are unique within a database
CREATE UNIQUE INDEX collection_unique_idx ON collections (database_id, name);

