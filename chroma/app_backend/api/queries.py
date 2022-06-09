import json

from matplotlib import projections

from api import chroma_data_manager
from utils import umap_project

def resolve_datapoints(obj, info):
     raw_embeddings = chroma_data_manager.get_embeddings()
     vectors = [emb["data"] for emb in raw_embeddings["embeddings"]["embeddings"]]

     projections = umap_project(vectors)

     datapoints = [{"x": proj[0], "y": proj[1], "metadata": json.dumps({ "class": "forest", "type": "production","ml_model_version": "v2"})} for proj in projections]

     payload = {
         "success": True,
         "datapoints": datapoints
     }
     return payload