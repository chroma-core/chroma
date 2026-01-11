-- Unique index for looking up collections by name within a tenant/database
-- Ensures collection names are unique within a database
CREATE UNIQUE INDEX collections_lookup_idx ON collections (tenant_id, database_name, name);

