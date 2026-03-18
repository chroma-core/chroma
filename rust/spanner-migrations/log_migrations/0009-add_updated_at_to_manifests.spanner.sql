ALTER TABLE manifests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true);
