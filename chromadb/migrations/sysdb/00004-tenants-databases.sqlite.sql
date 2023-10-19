CREATE TABLE tenants ( -- todo: make this idempotent by checking if table exists by using CREATE TABLE IF NOT EXISTS
    id TEXT PRIMARY KEY,
    UNIQUE (id) -- Maybe not needed since we want to support slug ids
);

CREATE TABLE databases (
    id TEXT PRIMARY KEY, -- unique globally
    name TEXT NOT NULL, -- unique per tenant
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, name) -- Ensure that a tenant has only one database with a given name
);

CREATE TABLE collections_tmp (
    id TEXT PRIMARY KEY, -- unique globally
    name TEXT NOT NULL, -- unique per database
    topic TEXT NOT NULL,
    dimension INTEGER,
    database_id TEXT NOT NULL REFERENCES databases(id) ON DELETE CASCADE,
    UNIQUE (name, database_id)
);

-- Create default tenant and database
INSERT INTO tenants (id) VALUES ('default'); -- should ids be uuids?
INSERT INTO databases (id, name, tenant_id) VALUES ('default', 'default', 'default');

INSERT INTO collections_tmp (id, name, topic, dimension, database_id)
    SELECT id, name, topic, dimension, 'default' FROM collections;
DROP TABLE collections;
ALTER TABLE collections_tmp RENAME TO collections;
