#!/bin/bash
# Sanity check to ensure package is built correctly.
# First argument is the directory to find wheels in.

# pip install -v --no-index --find-links $1 --extra-index-url https://pypi.org/simple chromadb
pip install target/wheels/chromadb-0.1.0.tar.gz

python -c "import chromadb; api = chromadb.Client(); print(api.heartbeat())"
