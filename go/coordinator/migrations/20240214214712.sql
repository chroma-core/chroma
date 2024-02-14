-- Modify "segments" table
ALTER TABLE "public"."segments" DROP CONSTRAINT "segments_pkey", ALTER COLUMN "collection_id" SET NOT NULL, ADD PRIMARY KEY ("collection_id", "id");
-- Create "record_logs" table
CREATE TABLE "public"."record_logs" (
  "collection_id" text NOT NULL,
  "id" bigserial NOT NULL,
  "timestamp" bigint NULL,
  "record" bytea NULL,
  PRIMARY KEY ("collection_id", "id")
);
