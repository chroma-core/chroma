CREATE TABLE IF NOT EXISTS fragments (
    collection_id STRING(36) NOT NULL,
    ident UUID NOT NULL,
    path STRING(64) NOT NULL,
    start INT64 NOT NULL,
    limit INT64 NOT NULL,
    num_bytes INT64 NOT NULL,
    setsum STRING(64) NOT NULL,
) PRIMARY KEY (id, ident);
