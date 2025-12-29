CREATE TABLE IF NOT EXISTS fragment_regions (
    collection_id STRING(36) NOT NULL,
    ident UUID NOT NULL,
    region STRING(32) NOT NULL,
) PRIMARY KEY (id, ident);
