from index.abstract import Index
import hnswlib
import numpy as np

class Hnswlib(Index):

    _index = None

    def __init__(self):
        # TODO: implement
        pass

    def run(self, embedding_data):
        # We split the data in two batches:
        data1 = embedding_data['embedding_data'].to_numpy().tolist()
        dim = len(data1[0])
        num_elements = len(data1) 
        print("dimensionality is:", dim)
        print("total number of elements is:", num_elements)
        print("max elements", num_elements//2)

        concatted_data = data1 
        print("concatted_data", len(concatted_data))

        # Declaring index
        p = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip

        # Initing index
        # max_elements - the maximum number of elements (capacity). Will throw an exception if exceeded
        # during insertion of an element.
        # The capacity can be increased by saving/loading the index, see below.
        #
        # ef_construction - controls index search speed/build speed tradeoff
        #
        # M - is tightly connected with internal dimensionality of the data. Strongly affects the memory consumption (~M)
        # Higher M leads to higher accuracy/run_time at fixed ef/efConstruction
        p.init_index(max_elements=len(data1), ef_construction=100, M=16)

        # Controlling the recall by setting ef:
        # higher ef leads to better accuracy, but slower search
        p.set_ef(10)

        # Set number of threads used during batch search/construction
        # By default using all available cores
        p.set_num_threads(4)

        print("Adding first batch of %d elements" % (len(data1)))
        p.add_items(data1)

        # Query the elements for themselves and measure recall:
        labels, distances = p.knn_query(data1, k=1)
        print(len(distances))
        print("Recall for the first batch:", np.mean(labels.reshape(-1) == np.arange(len(data1))), "\n")

        self._index = p

    def fetch(self, query):
        # TODO: implement - do something with the index
        pass

    def delete_batch(self, batch):
        # TODO: implement
        pass

    def persist(self):
        print('running hnswlib persist')
        print(self._index)
        self._index.save_index(".chroma/index.bin")
        print('Index saved to .chroma/index.bin')

    def load(self):
        # TODO: dont hard code maxelements obv
        self._index.load_index(".chroma/index.bin", max_elements = 1000000)



