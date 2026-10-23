-- Denormalize database_id into collection_compaction_cursors for CDC/billing.
-- NOT NULL with empty-string default; existing rows will be backfilled manually.

ALTER TABLE collection_compaction_cursors ADD COLUMN database_id STRING(MAX) NOT NULL DEFAULT ('')
