import umap

# Project high-dimensional vectors to 2-D via UMAP (https://umap-learn.readthedocs.io)
def umap_project(vectors):
    reducer = umap.UMAP()
    reducer.fit(vectors)
    projection = reducer.transform(vectors)
    return projection
