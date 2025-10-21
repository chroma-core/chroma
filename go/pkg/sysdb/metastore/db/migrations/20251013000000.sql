-- Make next_run NOT NULL
ALTER TABLE "public"."tasks"
ALTER COLUMN "next_run" SET NOT NULL;

-- Add lowest_live_nonce column, initialized to next_nonce
ALTER TABLE "public"."tasks"
ADD COLUMN "lowest_live_nonce" UUID DEFAULT NULL;
