#!/usr/scripts/env bash

docker run --rm -it -p 8000:8000 -e ALLOW_RESET=TRUE chromadb/chroma:latest
