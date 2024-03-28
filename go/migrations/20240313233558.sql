-- Create "collection_metadata" table
CREATE TABLE "public"."collection_metadata" (
  "collection_id" text NOT NULL,
  "key" text NOT NULL,
  "str_value" text NULL,
  "int_value" bigint NULL,
  "float_value" numeric NULL,
  "ts" bigint NULL DEFAULT 0,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("collection_id", "key")
);

-- Create "collections" table
CREATE TABLE "public"."collections" (
  "id" text NOT NULL,
  "name" text NULL,
  "topic" text NULL,
  "dimension" integer NULL,
  "database_id" text NULL,
  "ts" bigint NULL DEFAULT 0,
  "is_deleted" boolean NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "log_position" bigint NULL DEFAULT 0,
  "version" integer NULL DEFAULT 0,
  PRIMARY KEY ("id")
);

-- Create index "uni_collections_name" to table: "collections"
CREATE UNIQUE INDEX "uni_collections_name" ON "public"."collections" ("name");

-- Create "databases" table
CREATE TABLE "public"."databases" (
  "id" text NOT NULL,
  "name" character varying(128) NULL,
  "tenant_id" character varying(128) NULL,
  "ts" bigint NULL DEFAULT 0,
  "is_deleted" boolean NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("id")
);

-- Create index "idx_tenantid_name" to table: "databases"
CREATE UNIQUE INDEX "idx_tenantid_name" ON "public"."databases" ("name", "tenant_id");

-- Create "notifications" table
CREATE TABLE "public"."notifications" (
  "id" bigserial NOT NULL,
  "collection_id" text NULL,
  "type" text NULL,
  "status" text NULL,
  PRIMARY KEY ("id")
);

-- Create "record_logs" table
CREATE TABLE "public"."record_logs" (
  "collection_id" text NOT NULL,
  "id" bigint NOT NULL,
  "timestamp" bigint NULL,
  "record" bytea NULL,
  PRIMARY KEY ("collection_id", "id")
);

-- Create "segment_metadata" table
CREATE TABLE "public"."segment_metadata" (
  "segment_id" text NOT NULL,
  "key" text NOT NULL,
  "str_value" text NULL,
  "int_value" bigint NULL,
  "float_value" numeric NULL,
  "ts" bigint NULL DEFAULT 0,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("segment_id", "key")
);

-- Create "segments" table
CREATE TABLE "public"."segments" (
  "collection_id" text NOT NULL,
  "id" text NOT NULL,
  "type" text NOT NULL,
  "scope" text NULL,
  "topic" text NULL,
  "ts" bigint NULL DEFAULT 0,
  "is_deleted" boolean NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "file_paths" text NULL DEFAULT '{}',
  PRIMARY KEY ("collection_id", "id")
);

-- Create "tenants" table
CREATE TABLE "public"."tenants" (
  "id" text NOT NULL,
  "ts" bigint NULL DEFAULT 0,
  "is_deleted" boolean NULL DEFAULT false,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "last_compaction_time" bigint NOT NULL,
  PRIMARY KEY ("id")
);