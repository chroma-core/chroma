#!/bin/sh

time ./ann_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
	    --scratch=/tmp \
	    --sink=annoy
time ./ann_run.py --train_input=mnist_train.jsonl.bz2 --prod_input=mnist_test.jsonl.bz2 \
	    --scratch=/tmp \
	    --sink=hnsw

