CREATE TABLE IF NOT EXISTS fragment_regions (
    log_id STRING(36) NOT NULL,
    ident STRING(36) NOT NULL,
    region STRING(32) NOT NULL,
) PRIMARY KEY (log_id, ident, region);
