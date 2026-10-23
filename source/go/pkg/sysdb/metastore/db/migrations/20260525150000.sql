INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type", "is_async") VALUES (
  '2df4342c-5b5a-49aa-8345-c46503e85509',
  'revision_history',
  true,
  '{}',
  false
);

ALTER TABLE "public"."attached_functions" DROP CONSTRAINT "attached_functions_pkey";
ALTER TABLE "public"."attached_functions" ADD CONSTRAINT "attached_functions_pkey" PRIMARY KEY ("id", "input_collection_id");
