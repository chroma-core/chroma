-- Insert statistics function
INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type") VALUES (
  '304b58ad-a5cb-41dc-b88f-36dd3bf1d401',
  'statistics',
  false,
  '{"type": "object", "properties": {"key": {"type": "string", "description": "Metadata key"}, "type": {"type": "string", "description": "Value type"}, "value": {"type": "string", "description": "Value"}, "count": {"type": "integer", "description": "Frequency count"}}}'
);

-- Setting builtin functions to non incremental until we support incremental functions.
UPDATE "public"."functions" SET "is_incremental" = 'f' WHERE "name" = 'record_counter';
