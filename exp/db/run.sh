#!/bin/sh

./db_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
	    --scratch=/tmp \
	    --db=pythondb
