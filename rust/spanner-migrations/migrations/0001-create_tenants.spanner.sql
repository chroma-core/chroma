-- Create tenants table

CREATE TABLE IF NOT EXISTS tenants (
    id STRING(MAX) NOT NULL,
    is_deleted BOOL DEFAULT (false),
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    last_compaction_time TIMESTAMP NOT NULL,
    resource_name STRING(MAX)
) PRIMARY KEY (id)
