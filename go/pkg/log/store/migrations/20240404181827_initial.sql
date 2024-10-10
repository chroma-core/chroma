-- Create "collection" table
CREATE TABLE "public"."collection" (
  "id" text NOT NULL,
  "record_compaction_offset_position" bigint NOT NULL,
  "record_enumeration_offset_position" bigint NOT NULL,
  PRIMARY KEY ("id")
);
-- Create "record_log" table
CREATE TABLE "public"."record_log" (
  "offset" bigint NOT NULL,
  "collection_id" text NOT NULL,
  "timestamp" bigint NOT NULL,
  "record" bytea NOT NULL,
  PRIMARY KEY ("collection_id", "offset")
);
