#!/usr/bin/env python3

import argparse
import bz2
from collections import defaultdict
import json
import numpy as np
import sys

from adapter.milvus import Milvus
from adapter.parquet import Parquet
from adapter.pythondb import Pythondb
from adapter.sqlite import SQLitedb

from embedding import Embedding
from ovoids import Ovoid, OvoidTooSmall, OvoidSingularCovariance, OvoidNegativeSquared

db_adapters = {
    "parquet": Parquet,
    "pythondb": Pythondb,
    "sqlite": SQLitedb,
    "milvus": Milvus,
}


def get_args():
    parser = argparse.ArgumentParser(description="DB Test Run")
    parser.add_argument("--train_input", required=True, help="Path to training jsonl")
    parser.add_argument("--prod_input", required=True, help="Path to prod jsonl")
    parser.add_argument("--scratch", required=True, help="Path to scratch files")
    parser.add_argument("--db", required=True, help="Database to test")
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


def ingest_training(database, filename):
    for embedding in stream_embedding(filename, Embedding.Mode.TRAIN):
        database.ingest_training(embedding)
    database.commit()


def ingest_prod(database, filename, ovoids):
    ingested = 0
    for embedding in stream_embedding(filename, Embedding.Mode.PROD):
        if embedding.inference in ovoids:
            ovoid = ovoids[embedding.inference]
            distance = ovoid.distance(embedding.data)
            # print(f"Prod: {distance}")
        database.ingest_prod(embedding)
        ingested += 1
    print(f"Ingested {ingested} prod embeddings")


def build_ovoids(database):
    ovoids = {}
    for category in database.categories():
        embeddings = database.embeddings_for_category(category)
        width = len(embeddings[0])
        empty = np.empty((0, width))
        full = np.append(empty, embeddings, axis=0)
        try:
            ovoids[category] = Ovoid(category, full)
        except OvoidTooSmall:
            pass
        except OvoidSingularCovariance:
            pass
    return ovoids


def main():
    args = get_args()
    try:
        database = db_adapters[args.db]()
    except:
        print(f"Available adapters: {', '.join(sorted(db_adapters.keys()))}")
        sys.exit(1)
    print(f"Start with {args.db}")
    database.init_db(args.scratch)
    ingest_training(database, args.train_input)
    print(f"Counts: {database.training_counts()}")
    ovoids = build_ovoids(database)
    ingest_prod(database, args.prod_input, ovoids)


if __name__ == "__main__":
    main()
