-- Drop the segment unique index as it prevents multi-region segment replication
-- Segments need to exist in multiple regions with the same ID for MCMR support

DROP INDEX segment_unique_idx;

