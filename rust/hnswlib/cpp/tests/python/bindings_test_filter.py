import os
import unittest

import numpy as np

import hnswlib


class RandomSelfTestCase(unittest.TestCase):
    def testRandomSelf(self):
        dim = 16
        num_elements = 10000

        # Generating sample data
        data = np.float32(np.random.random((num_elements, dim)))

        # Declaring index
        hnsw_index = hnswlib.Index(
            space="l2", dim=dim
        )  # possible options are l2, cosine or ip
        bf_index = hnswlib.BFIndex(space="l2", dim=dim)

        # Initiating index
        # max_elements - the maximum number of elements, should be known beforehand
        #     (probably will be made optional in the future)
        #
        # ef_construction - controls index search speed/build speed tradeoff
        # M - is tightly connected with internal dimensionality of the data
        #     strongly affects the memory consumption

        hnsw_index.init_index(max_elements=num_elements, ef_construction=100, M=16)
        bf_index.init_index(max_elements=num_elements)

        # Controlling the recall by setting ef:
        # higher ef leads to better accuracy, but slower search
        hnsw_index.set_ef(10)

        hnsw_index.set_num_threads(4)  # by default using all available cores

        print("Adding %d elements" % (len(data)))
        hnsw_index.add_items(data)
        bf_index.add_items(data)

        # Query the elements for themselves and measure recall:
        labels, distances = hnsw_index.knn_query(data, k=1)
        self.assertAlmostEqual(
            np.mean(labels.reshape(-1) == np.arange(len(data))), 1.0, 3
        )

        print("Querying only even elements")
        # Query the even elements for themselves and measure recall:
        filter_function = lambda id: id % 2 == 0
        # Warning: search with a filter works slow in python in multithreaded mode, therefore we set num_threads=1
        labels, distances = hnsw_index.knn_query(
            data, k=1, num_threads=1, filter=filter_function
        )
        self.assertAlmostEqual(
            np.mean(labels.reshape(-1) == np.arange(len(data))), 0.5, 3
        )
        # Verify that there are only even elements:
        self.assertTrue(np.max(np.mod(labels, 2)) == 0)

        labels, distances = bf_index.knn_query(data, k=1, filter=filter_function)
        self.assertEqual(np.mean(labels.reshape(-1) == np.arange(len(data))), 0.5)
