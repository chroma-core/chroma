CREATE TABLE IF NOT EXISTS fragments (
    log_id STRING(36) NOT NULL,
    ident STRING(36) NOT NULL,
    path STRING(64) NOT NULL,
    position_start INT64 NOT NULL,
    position_limit INT64 NOT NULL,
    num_bytes INT64 NOT NULL,
    setsum STRING(64) NOT NULL,
) PRIMARY KEY (log_id, ident);
