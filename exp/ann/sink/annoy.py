#!/usr/bin/env python3

import random
import sys

from collections import defaultdict
from pathlib import Path

from annoy import AnnoyIndex
from embedding import Embedding

class Annoy:
    stage = defaultdict(list)
    indexes = {}

    def index_for(self, model_key, width):
        if model_key not in self.indexes:
            self.indexes[model_key] = AnnoyIndex(width, 'angular') # TODO: pass type from commandline
        return self.indexes[model_key]        

    @property
    def index_path(self):
        return self.scratch_path / "annoy"

    def path_for(self, model_key):
        return str((self.index_path / model_key).with_suffix(".annoy"))

    def init_sink(self, scratch_path):
        self.scratch_path = Path(scratch_path)
        self.index_path.mkdir(parents=True, exist_ok=True)

    def ingest_prod(self, embedding):
        index = self.index_for(embedding.model, embedding.width)
        num_to_return = 1
        search_k = -1 # TODO: command line
        result = index.get_nns_by_vector(embedding.data, num_to_return, search_k, include_distances=True)
        return result

    def ingest_training(self, embedding:Embedding):
        self.stage[embedding.key].append(embedding)

    def commit(self):
        stage = self.stage
        self.stage = {}
        for key, embeddings in stage.items():
            width = embeddings[0].width
            model_key = embeddings[0].model
            # index = AnnoyIndex(width, 'angular') # TODO: pass type from commandline
            index = self.index_for(model_key, width)
            print("Stage key", key, len(embeddings), width)
            # Simple index for identity will have to map back to UUID
            for identity, embedding in enumerate(embeddings):
                index.add_item(identity, embedding.data)
            index.build(10) # TODO: pass this from commandline
            index.save(self.path_for(key[0]))
            

def main():
    pass

def sample():
    f = 40  # Length of item vector that will be indexed

    t = AnnoyIndex(f, 'angular')
    for i in range(1000):
        v = [random.gauss(0, 1) for z in range(f)]
        t.add_item(i, v)

    t.build(10) # 10 trees
    t.save('test.ann')

    # ...

    u = AnnoyIndex(f, 'angular')
    u.load('test.ann') # super fast, will just mmap the file
    print(u.get_nns_by_item(0, 1000)) # will find the 1000 nearest neighbors

if __name__ == "__main__":
    main()
