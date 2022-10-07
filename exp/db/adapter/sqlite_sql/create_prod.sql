CREATE TABLE prod_embeddings (
  id          INTEGER NOT NULL, 
  created_at  DATETIME DEFAULT (CURRENT_TIMESTAMP), 
  updated_at  DATETIME DEFAULT (CURRENT_TIMESTAMP), 
  data        TEXT,
  inferences  TEXT,
  labels      TEXT,
  project     TEXT,
  model       TEXT,
  layer       TEXT,
  input_uri   TEXT,
  PRIMARY KEY (id)
);
