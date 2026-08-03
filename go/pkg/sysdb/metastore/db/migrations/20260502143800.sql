-- Add heap_entry_pending column to attached_functions table
ALTER TABLE attached_functions ADD COLUMN heap_entry_pending BOOLEAN NOT NULL DEFAULT FALSE;
