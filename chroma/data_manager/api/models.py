from cProfile import label
import json

from chroma.data_manager.api import db

# Number of embeddings we can return in a single request without it timing out 
EMBEDDING_PAGE_SIZE = 10000

class Embedding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.Text)
    input_identifier = db.Column(db.Text)
    inference_identifier = db.Column(db.Text)
    label = db.Column(db.Text)

    def to_dict(self):
        deserialized_data = json.loads(self.data)
        return {
            "id": self.id,
            "data": deserialized_data,
            "input_identifier": self.input_identifier,
            "inference_identifier": self.inference_identifier,
            "label": self.label,
        }
