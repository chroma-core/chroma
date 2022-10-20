import matplotlib.pyplot as plt
import numpy as np
import umap
import matplotlib

def umap_and_project(embedding_data, distances):
    def umap_project(vectors):
        reducer = umap.UMAP()#random_state=42) # add random_state to make it reproducible
        reducer.fit(vectors)
        projection = reducer.transform(vectors)
        return projection

    projections = umap_project(list(embedding_data))

    cm = plt.cm.get_cmap('RdYlBu') # different color scales: https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
    # norm helps color nicely even with crazy outliers
    # s = size of the points 
    plt.scatter(projections[:,0], projections[:,1], c=distances, s=0.3, cmap=cm, norm=matplotlib.colors.LogNorm())
    # make the plot larger
    plt.gcf().set_size_inches(10, 10)
    plt.title('UMAP embedding with color representing distance')
    # output the plot at high dpi
    plt.savefig('plot.png', dpi=600)

    return