-- Remove next_run column from attached_functions table as it's no longer needed
ALTER TABLE "public"."attached_functions" DROP COLUMN "next_run";
