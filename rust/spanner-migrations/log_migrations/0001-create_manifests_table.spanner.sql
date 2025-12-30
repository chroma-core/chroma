CREATE TABLE IF NOT EXISTS manifests (
    log_id STRING(36) NOT NULL,
    setsum STRING(64) NOT NULL,
    collected STRING(64) NOT NULL,
    acc_bytes INT64 NOT NULL,
    writer STRING(64) NOT NULL,
    enumeration_offset INT64 NOT NULL,
) PRIMARY KEY (log_id);
