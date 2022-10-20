import matplotlib.pyplot as plt
import numpy as np
import umap
import matplotlib

def umap_and_project(embedding_data, distances):
    def umap_project(vectors):
        reducer = umap.UMAP()#random_state=42)
        reducer.fit(vectors)
        projection = reducer.transform(vectors)
        return projection

    projections = umap_project(list(embedding_data))

    data = np.random.rand(len(projections), 4)
    cm = plt.cm.get_cmap('RdYlBu')
    plt.scatter(projections[:,0], projections[:,1], c=distances, s=0.3, cmap=cm, norm=matplotlib.colors.LogNorm())
    plt.gcf().set_size_inches(10, 10)
    plt.title('UMAP embedding of random colours');
    plt.savefig('plot.png', dpi=600)

    return