from enum import Enum

class Embedding:
    class Mode:
        TRAIN = "train"
        PROD = "prod"

    def __init__(self, data:dict, mode:Mode):
        self.data = data["embeddings"]
        self.width = len(self.data)
        self.inference = data["inferences"][0]
        self.labels = data["labels"]
        self.metadata = data["metadata"]
        self.resource_uri = data["resource_uri"]
        self.model = "model1"
        self.mode = mode

    @property
    def key(self):
        return (self.model, self.mode)

    def __repr__(self):
        return f"Embedding<{self.resource_uri}, {self.data[:3]}... ({len(self.data)})>"
