-- Create "notifications" table
CREATE TABLE "public"."notifications" (
  "id" bigserial NOT NULL,
  "collection_id" text NULL,
  "type" text NULL,
  "status" text NULL,
  PRIMARY KEY ("id")
);
