-- Create unique index on collections (database_id, name) to ensure collection names are unique within a database

CREATE UNIQUE INDEX idx_collections_database_id_name ON collections (name, database_id);

