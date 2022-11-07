import hnswlib
import pickle
import time
import os
import numpy as np
from chroma_server.index.abstract import Index
from chroma_server.utils import logger

class Hnswlib(Index):

    # we cache the index and mappers for the latest space_key
    _space_key = None
    _index = None
    _index_metadata = {
        'dimensionality': None,
        'elements': None,
        'time_created': None,
    }
    # these data structures enable us to map between uuids and ids
    # - our uuids are strings (clickhouse doesnt do autoincrementing ids for performance)
    # - but hnswlib uses integers only as ids
    # - so this is a bandaid. 
    _id_to_uuid = {}
    _uuid_to_id = {}

    def __init__(self):
        pass

    def run(self, space_key, uuids, embedding_data):
        # more comments available at the source: https://github.com/nmslib/hnswlib

        self._space_key = space_key

        s1 = time.time()
        embeddings = embedding_data
        ids = []
        i = 0

        for uuid in uuids:
            ids.append(i)
            self._id_to_uuid[i] = str(uuid)
            self._uuid_to_id[str(uuid)] = i
            i += 1
        
        data1 = embeddings
        dim = len(data1[0])
        num_elements = len(data1) 
        # logger.debug("dimensionality is:", dim)
        # logger.debug("total number of elements is:", num_elements)

        p = hnswlib.Index(space='l2', dim=dim)  # # Declaring index, possible options are l2, cosine or ip
        p.init_index(max_elements=len(data1), ef_construction=100, M=16) # Initing index
        p.set_ef(10)  # Controlling the recall by setting ef:
        p.set_num_threads(4) # Set number of threads used during batch search/construction

        # logger.debug("Adding first batch of elements", (len(data1)))
        s2= time.time()
        p.add_items(data1, ids)

        # Query the elements for themselves and measure recall:
        # database_ids, distances = p.knn_query(data1, k=1)
        # logger.debug("database_ids", database_ids)
        # logger.debug("distances", distances)
        # logger.debug(len(distances))
        # logger.debug("Recall for the first batch:" + str(np.mean(database_ids.reshape(-1) == np.arange(len(data1)))))

        self._index = p

        self._index_metadata = {
            'dimensionality': dim,
            'elements': num_elements,
            'time_created': time.time(),
        }

        self.save()

    def save(self):
        if self._index is None:
            return
        self._index.save_index(f"/index_data/index_{self._space_key}.bin")

        # pickle the mappers
        with open(f"/index_data/id_to_uuid_{self._space_key}.pkl", 'wb') as f:
            pickle.dump(self._id_to_uuid, f, pickle.HIGHEST_PROTOCOL)
        with open(f"/index_data/uuid_to_id_{self._space_key}.pkl", 'wb') as f:
            pickle.dump(self._uuid_to_id, f, pickle.HIGHEST_PROTOCOL)
        with open(f"/index_data/index_metadata_{self._space_key}.pkl", 'wb') as f:
            pickle.dump(self._index_metadata, f, pickle.HIGHEST_PROTOCOL)

        logger.debug('Index saved to /index_data/index.bin')

    def load(self, space_key):
        # unpickle the mappers
        with open(f"/index_data/id_to_uuid_{space_key}.pkl", 'rb') as f:
            self._id_to_uuid = pickle.load(f)
        with open(f"/index_data/uuid_to_id_{space_key}.pkl", 'rb') as f:
            self._uuid_to_id = pickle.load(f)
        with open(f"/index_data/index_metadata_{space_key}.pkl", 'rb') as f:
            self._index_metadata = pickle.load(f)

        p = hnswlib.Index(space='l2', dim= self._index_metadata['dimensionality'])
        self._index = p
        self._index.load_index(f"/index_data/index_{space_key}.bin", max_elements= self._index_metadata['elements'])

        self._space_key = space_key

    # do knn_query on hnswlib to get nearest neighbors
    def get_nearest_neighbors(self, space_key, query, k, uuids=None):

        if self._space_key != space_key:
            self.load(space_key)

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

    def reset(self):
        for f in os.listdir('/index_data'):
            os.remove(os.path.join('/index_data', f))
