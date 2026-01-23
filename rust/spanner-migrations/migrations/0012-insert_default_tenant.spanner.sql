DML:
INSERT INTO tenants (id, created_at, updated_at, last_compaction_time)
SELECT 'default_tenant', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP(), TIMESTAMP '1970-01-01 00:00:00+00'
WHERE NOT EXISTS (SELECT 1 FROM tenants WHERE id = 'default_tenant');
