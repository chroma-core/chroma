CREATE TABLE IF NOT EXISTS manifests (
    collection_id STRING(36) NOT NULL,
    setsum STRING(64) NOT NULL,
    collected STRING(64) NOT NULL,
    acc_bytes INT64 NOT NULL,
    writer STRING(64) NOT NULL,
    initial_offset INT64 NOT NULL,
    initial_seq_no INT64 NOT NULL,
) PRIMARY KEY (id);
