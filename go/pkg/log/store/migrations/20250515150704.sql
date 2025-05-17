-- Modify "collection" table
ALTER TABLE "collection" ADD COLUMN "is_sealed" boolean NOT NULL DEFAULT false;
