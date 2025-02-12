import os
import hnswlib
import numpy as np
import unittest


class RandomSelfTestCase(unittest.TestCase):
    def testRandomSelf(self):
        dim = 32
        num_elements = 100000
        k = 10
        num_queries = 20

        recall_threshold = 0.95

        # Generating sample data
        data = np.float32(np.random.random((num_elements, dim)))

        # Declaring index
        hnsw_index = hnswlib.Index(
            space="l2", dim=dim
        )  # possible options are l2, cosine or ip
        bf_index = hnswlib.BFIndex(space="l2", dim=dim)

        # Initing both hnsw and brute force indices
        # max_elements - the maximum number of elements (capacity). Will throw an exception if exceeded
        # during insertion of an element.
        # The capacity can be increased by saving/loading the index, see below.
        #
        # hnsw construction params:
        # ef_construction - controls index search speed/build speed tradeoff
        #
        # M - is tightly connected with internal dimensionality of the data. Strongly affects the memory consumption (~M)
        # Higher M leads to higher accuracy/run_time at fixed ef/efConstruction

        hnsw_index.init_index(max_elements=num_elements, ef_construction=200, M=16)
        bf_index.init_index(max_elements=num_elements)

        # Controlling the recall for hnsw by setting ef:
        # higher ef leads to better accuracy, but slower search
        hnsw_index.set_ef(200)

        # Set number of threads used during batch search/construction in hnsw
        # By default using all available cores
        hnsw_index.set_num_threads(4)

        print("Adding batch of %d elements" % (len(data)))
        hnsw_index.add_items(data)
        bf_index.add_items(data)

        print("Indices built")

        # Generating query data
        query_data = np.float32(np.random.random((num_queries, dim)))

        # Query the elements and measure recall:
        labels_hnsw, distances_hnsw = hnsw_index.knn_query(query_data, k)
        labels_bf, distances_bf = bf_index.knn_query(query_data, k)

        # Measure recall
        correct = 0
        for i in range(num_queries):
            for label in labels_hnsw[i]:
                for correct_label in labels_bf[i]:
                    if label == correct_label:
                        correct += 1
                        break

        recall_before = float(correct) / (k * num_queries)
        print("recall is :", recall_before)
        self.assertGreater(recall_before, recall_threshold)

        # test serializing  the brute force index
        index_path = "bf_index.bin"
        print("Saving index to '%s'" % index_path)
        bf_index.save_index(index_path)
        del bf_index

        # Re-initiating, loading the index
        bf_index = hnswlib.BFIndex(space="l2", dim=dim)

        print("\nLoading index from '%s'\n" % index_path)
        bf_index.load_index(index_path)

        # Query the brute force index again to verify that we get the same results
        labels_bf, distances_bf = bf_index.knn_query(query_data, k)

        # Measure recall
        correct = 0
        for i in range(num_queries):
            for label in labels_hnsw[i]:
                for correct_label in labels_bf[i]:
                    if label == correct_label:
                        correct += 1
                        break

        recall_after = float(correct) / (k * num_queries)
        print("recall after reloading is :", recall_after)

        self.assertEqual(recall_before, recall_after)

        os.remove(index_path)
