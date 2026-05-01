-- Add is_async column to functions table to support asynchronous function execution
ALTER TABLE "public"."functions" ADD COLUMN "is_async" boolean NOT NULL DEFAULT false;

INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type", "is_async") VALUES (
  '1db3d179-37a7-4c44-a301-687c1da69d7b',
  'dummy_async',
  true,
  '{}',
  true
);
