import hnswlib
import time
import numpy as np
from chroma_server.index.abstract import Index
from chroma_server.utils import logger

class Hnswlib(Index):

    _index = None

    # these data structures enable us to map between uuids and ids
    # - our uuids are strings (clickhouse doesnt do autoincrementing ids for performance)
    # - but hnswlib uses integers only as ids
    # - so this is a temporary bandaid. 
    # TODO: this should get written to disk somehow or we the index will be come useless after a restart
    _id_to_uuid = {}
    _uuid_to_id = {}

    def __init__(self):
        pass

    def run(self, embedding_data):
        # more comments available at the source: https://github.com/nmslib/hnswlib

        s1 = time.time()
        uuids = []
        embeddings = []
        ids = []
        i = 0
        for embedding in embedding_data:
            uuids.append(str(embedding[0]))
            embeddings.append((embedding[1]))
            ids.append(i)
            self._id_to_uuid[i] = str(embedding[0])
            self._uuid_to_id[str(embedding[0])] = i
            i += 1
        
        print('time to create uuids and embeddings: ', time.time() - s1)

        # We split the data in two batches:
        data1 = embeddings
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
        s2= time.time()
        p.add_items(data1, ids)
        print('time to add the items: ', time.time() - s2)

        # Query the elements for themselves and measure recall:
        # database_ids, distances = p.knn_query(data1, k=1)
        # logger.debug("database_ids", database_ids)
        # logger.debug("distances", distances)
        # logger.debug(len(distances))
        # logger.debug("Recall for the first batch:" + str(np.mean(database_ids.reshape(-1) == np.arange(len(data1)))))

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
    def get_nearest_neighbors(self, query, k, uuids=None):

        s2= time.time()
        # get ids from uuids
        ids = []
        for uuid in uuids:
            ids.append(self._uuid_to_id[uuid])

        filter_function = None
        if not ids is None:
            filter_function = lambda id: id in ids

        if len(ids) < k:
            k = len(ids)
        print('time to pre process our knn query: ', time.time() - s2)

        s3= time.time()
        database_ids, distances = self._index.knn_query(query, k=k, filter=filter_function)
        print('time to run knn query: ', time.time() - s3)

        # get uuids from ids    
        uuids = []
        for id in database_ids[0]:
            uuids.append(self._id_to_uuid[id])
        
        return uuids, distances
