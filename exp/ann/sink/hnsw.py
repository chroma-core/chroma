#!/usr/bin/env python3

import pickle
import random
import sys

from collections import defaultdict
from pathlib import Path

import hnswlib
from annoy import AnnoyIndex
from embedding import Embedding



class Hnsw:
    stage = defaultdict(list)
    indexes = {}

    def index_for(self, model_key, width):
        if model_key not in self.indexes:
            self.indexes[model_key] = hnswlib.Index(space = 'l2', dim=width) # TODO: pass type from commandline
        return self.indexes[model_key]

    @property
    def index_path(self):
        return self.scratch_path / "hnsw"

    def path_for(self, model_key):
        return str((self.index_path / model_key).with_suffix(".hnsw"))

    def init_sink(self, scratch_path):
        self.scratch_path = Path(scratch_path)
        self.index_path.mkdir(parents=True, exist_ok=True)

    def ingest_prod(self, embedding):
        index = self.index_for(embedding.model, embedding.width)
        num_to_return = 1 # TODO: command line
        labels, distances = index.knn_query([embedding.data], k=num_to_return)
        return (labels[0], distances[0])

    def ingest_training(self, embedding:Embedding):
        self.stage[embedding.key].append(embedding)

    def commit(self):
        stage = self.stage
        self.stage = {}
        for key, embeddings in stage.items():
            width = embeddings[0].width
            model_key = embeddings[0].model
            index = self.index_for(model_key, width)
            print("Stage key", key, len(embeddings), width)

            # TODO: knobs?
            index.init_index(max_elements = len(embeddings), ef_construction = 200, M = 16)
            data = [e.data for e in embeddings]
            # Simple index for identity will have to map back to UUID
            identities = range(len(data))
            index.add_items(data, identities)
            index.set_ef(50) # ef should always be > k
            serialized = pickle.dumps(index)
            #save to (self.path_for(key[0]))
            

def main():
    pass

if __name__ == "__main__":
    main()
