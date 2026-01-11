-- Index for listing collections with pagination (ordered by created_at)
CREATE INDEX collections_list_idx ON collections (tenant_id, database_name, created_at);

