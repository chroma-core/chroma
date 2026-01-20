-- Add compaction_failure_count column to collections table to track compaction failures persistently
ALTER TABLE "public"."collections" ADD COLUMN "compaction_failure_count" integer NOT NULL DEFAULT 0;
