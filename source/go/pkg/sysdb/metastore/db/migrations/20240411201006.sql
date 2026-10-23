-- Drop index "uni_collections_name" from table: "collections"
DROP INDEX "public"."uni_collections_name";
-- Create index "idx_name" to table: "collections"
CREATE UNIQUE INDEX "idx_name" ON "public"."collections" ("name", "database_id");
-- Drop "record_logs" table
DROP TABLE "public"."record_logs";
