-- DML:
INSERT OR IGNORE INTO tenants (id, created_at, updated_at, last_compaction_time)
VALUES ('default_tenant', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP(), TIMESTAMP '1970-01-01 00:00:00+00');
