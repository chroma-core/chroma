-- Create unique index on databases (name, tenant_id)
-- This ensures that database names are unique within a tenant

CREATE UNIQUE INDEX idx_tenantid_name ON databases (name, tenant_id);

