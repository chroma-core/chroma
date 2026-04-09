CREATE INDEX IF NOT EXISTS fragments_by_position_limit
ON fragments(log_id, position_limit)
STORING (path, position_start, num_bytes, setsum);
