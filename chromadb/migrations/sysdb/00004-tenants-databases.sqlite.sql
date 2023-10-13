CREATE TABLE tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    UNIQUE (name) -- Maybe not needed since we want to support slug ids
);

CREATE TABLE databases (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    UNIQUE (name)
);

ALTER TABLE collections
    ADD COLUMN database_id TEXT NOT NULL REFERENCES databases(id) DEFAULT 'default'; -- ON DELETE CASCADE not supported by sqlite in ALTER TABLE

-- Create default tenant and database
INSERT INTO tenants (id, name) VALUES ('default', 'default');
INSERT INTO databases (id, name, tenant_id) VALUES ('default', 'default', 'default');
