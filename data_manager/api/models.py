import json 
from data_manager.api import db

class Embedding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.Text)

    def to_dict(self):
        deserialized_data = json.loads(self.data)
        return {
            "id": self.id,
            "data": deserialized_data
        }