-- Add failure_count column to attached_functions table
ALTER TABLE attached_functions ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;
