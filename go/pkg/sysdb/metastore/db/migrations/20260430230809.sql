-- Create "database_metadata" table
CREATE TABLE "public"."database_metadata" (
  "database_id" text NOT NULL,
  "key" text NOT NULL,
  "str_value" text NULL,
  "int_value" bigint NULL,
  "float_value" numeric NULL,
  "bool_value" boolean NULL,
  "ts" bigint NULL DEFAULT 0,
  "created_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY ("database_id", "key"),
  CONSTRAINT "fk_database_metadata_database" FOREIGN KEY ("database_id") REFERENCES "public"."databases"("id") ON DELETE CASCADE
);

-- Create index for faster lookups by database_id
CREATE INDEX "idx_database_metadata_database_id" ON "public"."database_metadata" ("database_id");
