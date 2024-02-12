CREATE TABLE segments_temp (
                          id TEXT PRIMARY KEY,
                          type TEXT NOT NULL,
                          scope TEXT NOT NULL,
                          topic TEXT,
                          collection TEXT REFERENCES collections(id)
);

INSERT INTO segments_temp SELECT * FROM segments;

DROP TABLE segments;

ALTER TABLE segments_temp RENAME TO segments;
