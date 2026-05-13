-- Add http_generate function for triggering wiki generation via HTTP POST to Modal service
INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type", "is_async") VALUES (
  '9e3c7540-4ddd-40a2-bbff-ad9cb3f06efc',
  'http_generate',
  false,
  '{}',
  true,
);
