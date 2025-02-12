import unittest
import numpy as np
import hnswlib
import os


class RandomSelfTestCase(unittest.TestCase):
    def testPersistentIndex(self):
        print("\n**** Using a persistent index test ****\n")

        dim = 16
        num_elements = 10000

        # Generating sample data
        data = np.float32(np.random.random((num_elements, dim)))
        labels = np.arange(0, num_elements)

        # Declaring index
        p = hnswlib.Index(space="l2", dim=dim)

        # Initiating index
        # Make test dir if it doesn't exist
        if not os.path.exists("test_dir"):
            os.makedirs("test_dir")
        p.init_index(
            max_elements=num_elements,
            ef_construction=100,
            M=16,
            is_persistent_index=True,
            persistence_location="test_dir",
        )
        p.set_num_threads(4)

        print("Adding all elements (%d)" % (len(data)))
        p.add_items(data, labels)
        p.persist_dirty()

        # Load a persisted index
        p2 = hnswlib.Index(space="l2", dim=dim)
        p2.load_index("test_dir", is_persistent_index=True)
        returned_items = p2.get_items(labels)
        self.assertTrue(np.allclose(data, returned_items, atol=1e-6))

        # Test that the query results are the same between the two indices
        query = np.float32(np.random.random((1, dim)))
        labels, distances = p.knn_query(query, k=10)
        labels2, distances2 = p2.knn_query(query, k=10)
        # Check if numpy labels are the same
        self.assertTrue((labels == labels2).all())
        self.assertTrue(np.allclose(distances, distances2, atol=1e-6))
