import json

from matplotlib import projections

from api import chroma_data_manager
from utils import umap_project

def resolve_datapoints(obj, info):
     raw_embeddings = chroma_data_manager.get_embeddings()
     vectors = [emb["data"] for emb in raw_embeddings["embeddings"]["embeddings"]]

     projections = umap_project(vectors)

     data = [[proj[0], proj[1], { "class": "forest", "type": "production","ml_model_version": "v2"}] for proj in projections]

     payload = {
         'data': json.dumps(str(data))
     }
     return payload