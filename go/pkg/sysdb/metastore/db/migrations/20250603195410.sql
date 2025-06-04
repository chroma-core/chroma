-- Modify "tenants" table
ALTER TABLE "public"."tenants" ADD COLUMN "resource_name" text NULL;
-- Create index "idx_resource_name_unique" to table: "tenants"
CREATE UNIQUE INDEX "idx_resource_name_unique" ON "public"."tenants" ("resource_name") WHERE (resource_name IS NOT NULL);
