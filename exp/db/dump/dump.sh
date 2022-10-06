#!/bin/sh

SQL=`cat dump.sql`

sqlite3 -json /tmp/chroma.db "$SQL" > dump.out
#head dump.out
./flatten_sql_embeddings.py < dump.out
