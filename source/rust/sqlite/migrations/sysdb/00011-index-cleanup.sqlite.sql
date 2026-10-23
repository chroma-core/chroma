-- Record intentional deletion in the same transaction as the segment removal.
-- Absence from the catalog alone must never authorize deleting index files.
CREATE TABLE index_cleanup (
    segment_id TEXT PRIMARY KEY NOT NULL
);
