CREATE TABLE IF NOT EXISTS manifest_regions (
    log_id STRING(36) NOT NULL,
    region STRING(32) NOT NULL,
    collected STRING(64) NOT NULL,
) PRIMARY KEY (log_id, region),
    INTERLEAVE IN PARENT manifests ON DELETE NO ACTION;
