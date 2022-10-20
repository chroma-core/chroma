import matplotlib.pyplot as plt
import numpy as np
import umap

def umap_and_project(embedding_data):
    def umap_project(vectors):
        reducer = umap.UMAP()#random_state=42)
        reducer.fit(vectors)
        projection = reducer.transform(vectors)
        return projection

    projections = umap_project(list(embedding_data))

    data = np.random.rand(len(projections), 4)
    plt.scatter(projections[:,0], projections[:,1], c=data)
    plt.title('UMAP embedding of random colours');
    plt.savefig('plot.png')

    return