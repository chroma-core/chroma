ALTER TABLE segments RENAME TO old_segments;


CREATE TABLE segments (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    scope TEXT NOT NULL,
    topic TEXT,
    collection TEXT REFERENCES collections(id) 
);


INSERT INTO segments
SELECT * FROM old_segments;


DROP TABLE old_segments;
