-- All segments should have an associated collection.
ALTER TABLE "public"."segments" ALTER COLUMN "collection" SET NOT NULL;
