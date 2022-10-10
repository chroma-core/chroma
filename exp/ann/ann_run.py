#!/usr/bin/env python3

import argparse
import bz2
import json
import numpy as np
import sys

from collections import defaultdict

from embedding import Embedding
from sink.annoy import Annoy

sinks = {
    "annoy": Annoy,
}


def get_args():
    parser = argparse.ArgumentParser(description="ANN Test Run")
    parser.add_argument("--train_input", required=True, help="Path to training jsonl")
    parser.add_argument("--prod_input", required=True, help="Path to prod jsonl")
    parser.add_argument("--scratch", required=True, help="Path to scratch files")
    parser.add_argument("--sink", required=True, help="ANN to test")
    args = parser.parse_args()
    return args


def stream_json(filename):
    with bz2.open(filename, "r") as stream:
        for line in stream:
            data = json.loads(line)
            yield data


def stream_embedding(filename, mode:Embedding.Mode):
    for data in stream_json(filename):
        yield Embedding(data, mode)


def ingest_training(sink, filename):
    for embedding in stream_embedding(filename, Embedding.Mode.TRAIN):
        sink.ingest_training(embedding)
    sink.commit()


def ingest_prod(sink, filename):
    ingested = 0
    distances = []
    for embedding in stream_embedding(filename, Embedding.Mode.PROD):
        keys, key_dists = sink.ingest_prod(embedding)
        distances.append(key_dists[0])
        ingested += 1
    distances.sort()
    print(f"Ingested {ingested} prod embeddings dists: {distances[0]}-{distances[-1]}")


def main():
    args = get_args()
    try:
        sink = sinks[args.sink]()
    except:
        print(f"Available adapters: {', '.join(sorted(sinks.keys()))}")
        sys.exit(1)
    print(f"Start with {args.sink}")
    sink.init_sink(args.scratch)
    ingest_training(sink, args.train_input)
    ingest_prod(sink, args.prod_input)


if __name__ == "__main__":
    main()
