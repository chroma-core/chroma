import json
import umap

from chroma.data_manager import data_manager

# Project high-dimensional vectors to 2-D via UMAP (https://umap-learn.readthedocs.io)
def umap_project(vectors):
    reducer = umap.UMAP()
    reducer.fit(vectors)
    projection = reducer.transform(vectors)
    return projection


def fetch_datapoints():
    chroma_data_manager = data_manager.ChromaDataManager()
    print(" * app utils: fetching embeddings from data_manager", flush=True)
    raw_embeddings = chroma_data_manager.get_embeddings()
    vectors = [emb["data"] for emb in raw_embeddings["embeddings"]["embeddings"]]

    print(" * app utils: projecting to datapoints", flush=True)
    projections = umap_project(vectors)

    print(" * app utils: packing datapoints", flush=True)
    datapoints = [
        {
            "x": proj[0],
            "y": proj[1],
            "metadata": json.dumps(
                {"class": "forest", "type": "production", "ml_model_version": "v2"}
            ),
        }
        for proj in projections
    ]
    print(" * app utils: finished processing datapoints", flush=True)
    return datapoints
