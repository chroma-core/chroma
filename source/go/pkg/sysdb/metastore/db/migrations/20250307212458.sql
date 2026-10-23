-- Modify "collections" table
ALTER TABLE "public"."collections" ADD COLUMN "num_versions" integer NULL DEFAULT 0, ADD COLUMN "oldest_version_ts" timestamp NULL;
