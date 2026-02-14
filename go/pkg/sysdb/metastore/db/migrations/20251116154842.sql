-- Add is_ready column to attached_functions table to track initialization status
ALTER TABLE "public"."attached_functions" ADD COLUMN "is_ready" boolean NOT NULL DEFAULT false;
