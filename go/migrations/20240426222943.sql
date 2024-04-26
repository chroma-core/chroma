-- Modify "segments" table
ALTER TABLE "public"."segments" ADD COLUMN "log_position" bigint NULL DEFAULT 0, ADD COLUMN "collection_version" integer NULL DEFAULT 0;
