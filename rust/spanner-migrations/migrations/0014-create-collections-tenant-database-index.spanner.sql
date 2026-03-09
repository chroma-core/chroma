-- Create index to speed tenant/database lookups (e.g. database listing paths)
CREATE INDEX collections_tenant_database_idx ON collections (tenant_id, database_name);
