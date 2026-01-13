-- Create "functions" table (renamed from operators)
CREATE TABLE "public"."functions" (
  "id" uuid NOT NULL,
  "name" text NOT NULL UNIQUE,
  "is_incremental" boolean NOT NULL,
  "return_type" jsonb NOT NULL,
  PRIMARY KEY ("id")
);

-- Create "attached_functions" table (renamed from tasks)
CREATE TABLE "public"."attached_functions" (
  "id" uuid NOT NULL,
  "name" text NOT NULL,
  "tenant_id" text NOT NULL,
  "database_id" text NOT NULL,
  "input_collection_id" text NOT NULL,
  "output_collection_name" text NOT NULL,
  "output_collection_id" text DEFAULT NULL,
  "function_id" uuid NOT NULL,
  "function_params" jsonb NOT NULL,
  "completion_offset" bigint NOT NULL DEFAULT 0,
  "last_run" timestamp NULL DEFAULT NULL,
  "next_run" timestamp NOT NULL,
  "min_records_for_invocation" bigint NOT NULL DEFAULT 100,
  "current_attempts" integer NOT NULL DEFAULT 0,
  "is_alive" boolean NOT NULL DEFAULT true,
  "is_deleted" boolean NOT NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "global_parent" uuid NULL,
  "next_nonce" UUID NOT NULL,
  "oldest_written_nonce" UUID DEFAULT NULL,
  "lowest_live_nonce" UUID DEFAULT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "unique_attached_function_per_collection" UNIQUE ("input_collection_id", "name")
);

-- Create "global_functions" table (renamed from task_templates)
CREATE TABLE "public"."global_functions" (
  "id" uuid NOT NULL,
  "tenant_id" text NOT NULL,
  "database_id" text NOT NULL,
  "name" text NOT NULL,
  "function_id" text NOT NULL,
  "params" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "output_collection_pattern" text NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id"),
  CONSTRAINT "unique_global_function_per_tenant_db" UNIQUE ("tenant_id", "database_id", "name")
);

-- Insert built-in functions
INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type") VALUES (
  'ccf2e3ba-633e-43ba-9394-46b0c54c61e3', -- Randomly generated
  'record_counter',
  true,
  '{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}'
);
