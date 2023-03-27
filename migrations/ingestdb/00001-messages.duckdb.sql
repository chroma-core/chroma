CREATE SEQUENCE seq_messages START 1;

CREATE TABLE messages (
    topic TEXT NOT NULL,
    id TEXT NOT NULL,
    seq BIGINT NOT NULL DEFAULT nextval('seq_messages'),
    message BLOB
);