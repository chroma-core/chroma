-- This adds resource_name to tenants and makes it unique
ALTER TABLE tenants ADD COLUMN resource_name text NULL;
CREATE UNIQUE INDEX idx_resource_name_unique ON tenants (resource_name) WHERE (resource_name IS NOT NULL);
