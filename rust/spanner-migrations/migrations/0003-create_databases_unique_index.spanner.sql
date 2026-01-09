-- Create unique index on databases (name, tenant_id)
-- This ensures that database names are unique within a tenant

CREATE UNIQUE INDEX database_unique_idx ON databases (tenant_id, name);

