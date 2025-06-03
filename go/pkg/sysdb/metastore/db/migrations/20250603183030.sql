-- Modify "tenants" table
ALTER TABLE "public"."tenants" ADD COLUMN "static_name" text NULL;
-- Create index "idx_static_name_not_null" to table: "tenants"
CREATE UNIQUE INDEX "idx_static_name_not_null" ON "public"."tenants" ("static_name") WHERE (static_name IS NOT NULL);
