import numpy

from collections import defaultdict


class Pythondb:
    stage = defaultdict(list)
    training_embeddings = defaultdict(list)
    prod_embeddings = []

    def init_db(self, scratch_path):
        pass

    def ingest_prod(self, embedding):
        self.prod_embeddings.append(embedding)

    def ingest_training(self, embedding):
        self.stage[embedding.key].append(embedding)
        # self.training_embeddings[embedding.inference].append(embedding)

    def training_counts(self):
        return [(cat, len(embeds)) for cat, embeds in self.training_embeddings.items()]

    def categories(self):
        return self.training_embeddings.keys()

    def embeddings_for_category(self, category):
        return [e.data for e in self.training_embeddings[category]]

    def commit(self):
        stage = self.stage
        self.stage = {}

        for key, embeddings in stage.items():
            model, mode = key
            for embedding in embeddings:
                self.training_embeddings[model].append(embedding)
