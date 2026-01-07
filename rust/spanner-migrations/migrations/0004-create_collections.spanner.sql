-- Create collections table

CREATE TABLE IF NOT EXISTS collections (
    collection_id STRING(MAX) NOT NULL,
    name STRING(MAX) NOT NULL,
    dimension INT64,
    database_id STRING(MAX) NOT NULL,
    database_name STRING(MAX) NOT NULL,
    tenant_id STRING(MAX) NOT NULL,
    is_deleted BOOL DEFAULT (false),
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (collection_id)

