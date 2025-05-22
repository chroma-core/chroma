CREATE TABLE collection (
                        id text PRIMARY KEY,
                        record_compaction_offset_position bigint NOT NULL,
                        record_enumeration_offset_position bigint NOT NULL,
                        is_sealed boolean NOT NULL DEFAULT false
                        );

-- The `record_compaction_offset_position` column indicates the offset position of the latest compaction, offsets are 1-indexed.
-- The `record_enenumeration_offset_position` column denotes the incremental offset for the most recent record in a collection.
