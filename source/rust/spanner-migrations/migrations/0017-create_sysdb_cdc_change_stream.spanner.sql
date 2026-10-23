CREATE CHANGE STREAM mcmr_sysdb_ccc_stream
  FOR collection_compaction_cursors
  OPTIONS (retention_period = '7d', value_capture_type = 'OLD_AND_NEW_VALUES');
