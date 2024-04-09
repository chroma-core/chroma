CREATE TABLE record_log (
                        "offset" BIGINT NOT NULL,
                        collection_id text NOT NULL,
                        timestamp BIGINT NOT NULL,
                        record bytea NOT NULL,
                        PRIMARY KEY(collection_id, "offset")
);

