-- Modify "record_log" table
ALTER TABLE "public"."record_log" ALTER COLUMN "timestamp" SET DEFAULT (EXTRACT(epoch FROM now()))::integer;
