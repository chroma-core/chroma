ALTER TABLE "public"."collections" ADD COLUMN "size_bytes_post_compaction" bigint NULL DEFAULT 0;
ALTER TABLE "public"."collections" ADD COLUMN "last_compaction_time_secs" bigint NULL DEFAULT 0;
