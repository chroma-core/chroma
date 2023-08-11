CREATE VIRTUAL TABLE embedding_fulltext_search USING fts5(id, string_value, tokenize='trigram');
INSERT INTO embedding_fulltext (id, string_value) SELECT id, string_value FROM embedding_metadata;
DROP TABLE embedding_fulltext;
