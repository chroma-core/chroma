-- Create "collection_position" table
CREATE TABLE "public"."collection_position" (
  "collection_id" text NOT NULL,
  "record_log_position" bigint NOT NULL,
  PRIMARY KEY ("collection_id")
);
-- Create "record_log" table
CREATE TABLE "public"."record_log" (
  "id" bigint NOT NULL,
  "collection_id" text NOT NULL,
  "timestamp" integer NOT NULL,
  "record" bytea NOT NULL,
  PRIMARY KEY ("collection_id", "id")
);
