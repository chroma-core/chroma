import numpy

from collections import defaultdict


class Pythondb:
    training_embeddings = defaultdict(list)
    prod_embeddings = []

    def init_db(self, scratch_path):
        pass

    def ingest_prod(self, embedding):
        self.prod_embeddings.append(embedding)

    def ingest_training(self, embedding):
        for category in embedding.inferences:
            self.training_embeddings[category].append(embedding)

    def training_counts(self):
        return [(cat, len(embeds)) for cat, embeds in self.training_embeddings.items()]

    def categories(self):
        return self.training_embeddings.keys()

    def embeddings_for_category(self, category):
        return [e.data for e in self.training_embeddings[category]]
