-- Modify "collections" table
ALTER TABLE "public"."collections" ALTER COLUMN "name" SET NOT NULL, ALTER COLUMN "database_id" SET NOT NULL;
-- Modify "databases" table
ALTER TABLE "public"."databases" ALTER COLUMN "name" SET NOT NULL, ALTER COLUMN "tenant_id" SET NOT NULL;
-- Modify "segments" table
ALTER TABLE "public"."segments" ADD CONSTRAINT "uni_segments_id" UNIQUE ("id");
