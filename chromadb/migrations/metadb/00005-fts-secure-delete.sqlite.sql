-- Ref: https://www.sqlite.org/fts5.html#the_secure_delete_configuration_option
INSERT INTO embedding_fulltext_search(string_value, rank) VALUES('secure-delete', 1);
