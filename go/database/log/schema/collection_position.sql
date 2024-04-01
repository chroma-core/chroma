CREATE TABLE log (
                        id   BIGINT PRIMARY KEY,
                        collection_id text NOT NULL,
                        timestamp int NOT NULL,
                        record bytea NOT NULL
);

