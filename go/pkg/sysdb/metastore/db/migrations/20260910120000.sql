-- Add an index for stable, tenant-scoped database pagination.
CREATE INDEX "idx_databases_list" ON "public"."databases" ("tenant_id", "is_deleted", "created_at", "id");
