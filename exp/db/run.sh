#!/bin/sh

time ./db_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
	    --scratch=/tmp \
	    --db=pythondb
time ./db_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
	    --scratch=/tmp \
	    --db=sqlite



# Milvus deferred until we test nearest-vector searches
# time ./db_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
# 	    --scratch=/tmp \
# 	    --db=milvus
