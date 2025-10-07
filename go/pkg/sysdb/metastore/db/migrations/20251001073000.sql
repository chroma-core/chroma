-- Create "operators" table
CREATE TABLE "public"."operators" (
  "operator_id" uuid NOT NULL,
  "operator_name" text NOT NULL UNIQUE,
  "is_incremental" boolean NOT NULL,
  "return_type" jsonb NOT NULL,
  PRIMARY KEY ("operator_id")
);

-- Insert sample operator: record counter
INSERT INTO "public"."operators" ("operator_id", "operator_name", "is_incremental", "return_type")
VALUES (
  'ccf2e3ba-633e-43ba-9394-46b0c54c61e3', -- Randomly generated
  'record_counter',
  true,
  '{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}'
);

-- Create "tasks" table
CREATE TABLE "public"."tasks" (
  "task_id" uuid NOT NULL,
  "task_name" text NOT NULL,
  "tenant_id" text NOT NULL,
  "database_id" text NOT NULL,
  "input_collection_id" text NOT NULL, -- Keeping these as text instead of UUID until collections.id becomes a UUID
  "output_collection_name" text NOT NULL,
  "output_collection_id" text DEFAULT NULL, -- Lazily filled in after output collection is created
  "operator_id" uuid NOT NULL,
  "operator_params" jsonb NOT NULL,
  "completion_offset" bigint NOT NULL DEFAULT 0,
  "last_run" timestamp NULL DEFAULT NULL,
  "next_run" timestamp NULL DEFAULT NULL,
  "min_records_for_task" bigint NOT NULL DEFAULT 100,
  "current_attempts" integer NOT NULL DEFAULT 0,
  "is_alive" boolean NOT NULL DEFAULT true,
  "is_deleted" boolean NOT NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "task_template_parent" uuid NULL,
  "next_nonce" UUID NOT NULL, -- UUIDv7
  "oldest_written_nonce" UUID DEFAULT NULL, -- UUIDv7
  PRIMARY KEY ("task_id"),
  CONSTRAINT "unique_task_per_collection" UNIQUE ("input_collection_id", "task_name")
);

-- Create "task_templates" table
CREATE TABLE "public"."task_templates" (
  "template_id" uuid NOT NULL,
  "tenant_id" text NOT NULL,
  "database_id" text NOT NULL,
  "template_name" text NOT NULL,
  "operator_id" text NOT NULL,
  "params" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "output_collection_pattern" text NOT NULL,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("template_id"),
  CONSTRAINT "unique_template_per_tenant_db" UNIQUE ("tenant_id", "database_id", "template_name")
);
