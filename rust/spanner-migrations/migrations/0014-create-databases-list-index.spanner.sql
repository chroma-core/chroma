-- Index for listing databases by tenant with soft-delete filtering and created_at ordering
CREATE INDEX databases_list_idx ON databases (tenant_id, is_deleted, created_at) STORING (name);
