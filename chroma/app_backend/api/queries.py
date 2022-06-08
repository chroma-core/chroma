import json

from api import chroma_data_manager

def resolve_datapoints(obj, info):
     raw_embeddings = chroma_data_manager.get_embeddings()
     print(len(raw_embeddings["embeddings"]["embeddings"]))

     payload = {
         'data': json.dumps([
                        [
                            -0.36499756446397,
                            -0.9535619122565171,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],])
     }
     return payload