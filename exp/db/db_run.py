#!/usr/bin/env python3

import argparse
import bz2
from collections import defaultdict
import json
import numpy as np
import sys

from adapter.numpydb import Numpy
from adapter.pythondb import Pythondb
from ovoids import Ovoid, OvoidTooSmall, OvoidSingularCovariance, OvoidNegativeSquared

db_adapters = {
    "pythondb": Pythondb,
    "numpydb": Numpy,
}


def get_args():
    parser = argparse.ArgumentParser(description="DB Test Run")
    parser.add_argument("--train_input", required=True, help="Path to training jsonl")
    parser.add_argument("--prod_input", required=True, help="Path to prod jsonl")
    parser.add_argument("--db", required=True, help="Database to test")
    args = parser.parse_args()
    return args


def stream_json(filename):
    with bz2.open(filename, "r") as stream:
        for line in stream:
            data = json.loads(line)
            yield data


def ingest_training(database, filename):
    for data in stream_json(filename):
        database.ingest_training(data)


def ingest_prod(database, filename, ovoids):
    ingested = 0
    for data in stream_json(filename):
        for inference in data["inferences"]:
            if inference in ovoids:
                ovoid = ovoids[inference]
                distance = ovoid.distance(data)
                # print(f"Prod: {distance}")
        database.ingest_prod(data)
        ingested += 1
    print(f"Ingested {ingested} prod embeddings")


def build_ovoids(database):
    ovoids = {}
    for category in database.categories():
        embeddings = database.embeddings_for_category(category)
        vectors = [e["embeddings"] for e in embeddings]
        empty = np.empty((0, len(vectors[0])))
        full = np.append(empty, vectors, axis=0)
        try:
            ovoids[category] = Ovoid(category, full)
        except ovoids.OvoidTooSmall:
            pass
        except ovoids.OvoidSingularCovariance:
            pass
    return ovoids


def main():
    args = get_args()
    try:
        database = db_adapters[args.db]()
    except:
        print(f"Available adapters: {', '.join(sorted(db_adapters.keys()))}")
        sys.exit(1)
    ingest_training(database, args.train_input)
    print(f"Counts: {database.training_counts()}")
    ovoids = build_ovoids(database)
    ingest_prod(database, args.prod_input, ovoids)


if __name__ == "__main__":
    main()
