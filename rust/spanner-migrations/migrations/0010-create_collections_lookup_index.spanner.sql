-- Index for looking up collections by name within a tenant/database
CREATE INDEX collections_lookup_idx ON collections (tenant_id, database_name, name);

