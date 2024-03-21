INSERT OR REPLACE INTO tenants (id) VALUES ('default_tenant'); -- The default tenant id is 'default_tenant' others are UUIDs
INSERT OR REPLACE INTO databases (id, name, tenant_id) VALUES ('00000000-0000-0000-0000-000000000000', 'default_database', 'default_tenant');

INSERT OR REPLACE INTO collections_tmp (id, name, topic, dimension, database_id)
    SELECT id, name, topic, dimension, '00000000-0000-0000-0000-000000000000' FROM collections;
DROP TABLE collections;
ALTER TABLE collections_tmp RENAME TO collections;
