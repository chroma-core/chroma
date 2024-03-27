INSERT INTO
  "public"."tenants" (id, last_compaction_time)
VALUES
  ('default_tenant', 0);

-- The default tenant id is 'default_tenant' others are UUIDs
INSERT INTO
  databases (id, name, tenant_id)
VALUES
  (
    '00000000-0000-0000-0000-000000000000',
    'default_database',
    'default_tenant'
  );