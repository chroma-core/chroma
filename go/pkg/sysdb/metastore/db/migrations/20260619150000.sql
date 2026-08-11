ALTER TABLE "public"."attached_functions" DROP CONSTRAINT "attached_functions_pkey";
ALTER TABLE "public"."attached_functions" ADD CONSTRAINT "attached_functions_pkey" PRIMARY KEY ("id", "input_collection_id");
