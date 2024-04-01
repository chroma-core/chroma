CREATE TABLE record_log (
                        "offset" BIGINT NOT NULL,
                        collection_id text NOT NULL,
                        timestamp int NOT NULL default extract(epoch from now())::int,
                        record bytea NOT NULL,
                        PRIMARY KEY(collection_id, "offset")
);

