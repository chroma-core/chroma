import hnswlib
import numpy as np
from chroma_server.index.abstract import Index
from chroma_server.utils import logger

class Hnswlib(Index):

    _index = None

    def __init__(self):
        pass

    def run(self, embedding_data):
        # more comments available at the source: https://github.com/nmslib/hnswlib

        # We split the data in two batches:
        data1 = embedding_data['embedding_data'].to_numpy().tolist()
        dim = len(data1[0])
        num_elements = len(data1) 
        # logger.debug("dimensionality is:", dim)
        # logger.debug("total number of elements is:", num_elements)
        # logger.debug("max elements", num_elements//2)

        concatted_data = data1 
        # logger.debug("concatted_data", len(concatted_data))
        
        p = hnswlib.Index(space='l2', dim=dim)  # # Declaring index, possible options are l2, cosine or ip
        p.init_index(max_elements=len(data1), ef_construction=100, M=16) # Initing index
        p.set_ef(10)  # Controlling the recall by setting ef:
        p.set_num_threads(4) # Set number of threads used during batch search/construction

        # logger.debug("Adding first batch of elements", (len(data1)))
        p.add_items(data1, embedding_data["id"])

        # Query the elements for themselves and measure recall:
        database_ids, distances = p.knn_query(data1, k=1)
        # logger.debug("database_ids", database_ids)
        # logger.debug("distances", distances)
        # logger.debug(len(distances))
        logger.debug("Recall for the first batch:" + str(np.mean(database_ids.reshape(-1) == np.arange(len(data1)))))

        self._index = p

    def fetch(self, query):
       raise NotImplementedError

    def delete_batch(self, batch):
        raise NotImplementedError

    def persist(self):
        if self._index is None:
            return
        self._index.save_index(".chroma/index.bin")
        logger.debug('Index saved to .chroma/index.bin')

    def load(self, elements, dimensionality):
        p = hnswlib.Index(space='l2', dim= dimensionality)
        self._index = p
        self._index.load_index(".chroma/index.bin", max_elements= elements)

    # do knn_query on hnswlib to get nearest neighbors
    def get_nearest_neighbors(self, query, k):
        database_ids, distances = self._index.knn_query(query, k=k)
        return database_ids, distances
