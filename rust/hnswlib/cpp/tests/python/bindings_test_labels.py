import os
import unittest

import numpy as np

import hnswlib


class RandomSelfTestCase(unittest.TestCase):
    def testRandomSelf(self):
        for idx in range(2):
            print("\n**** Index save-load test ****\n")

            np.random.seed(idx)
            dim = 16
            num_elements = 10000

            # Generating sample data
            data = np.float32(np.random.random((num_elements, dim)))

            # Declaring index
            p = hnswlib.Index(
                space="l2", dim=dim
            )  # possible options are l2, cosine or ip

            # Initiating index
            # max_elements - the maximum number of elements, should be known beforehand
            #     (probably will be made optional in the future)
            #
            # ef_construction - controls index search speed/build speed tradeoff
            # M - is tightly connected with internal dimensionality of the data
            #     strongly affects the memory consumption

            p.init_index(max_elements=num_elements, ef_construction=100, M=16)

            # Controlling the recall by setting ef:
            # higher ef leads to better accuracy, but slower search
            p.set_ef(100)

            p.set_num_threads(4)  # by default using all available cores

            # We split the data in two batches:
            data1 = data[: num_elements // 2]
            data2 = data[num_elements // 2 :]

            print("Adding first batch of %d elements" % (len(data1)))
            p.add_items(data1)

            # Query the elements for themselves and measure recall:
            labels, distances = p.knn_query(data1, k=1)

            items = p.get_items(labels)

            # Check the recall:
            self.assertAlmostEqual(
                np.mean(labels.reshape(-1) == np.arange(len(data1))), 1.0, 3
            )

            # Check that the returned element data is correct:
            diff_with_gt_labels = np.mean(np.abs(data1 - items))
            self.assertAlmostEqual(diff_with_gt_labels, 0, delta=1e-4)

            # Serializing and deleting the index.
            # We need the part to check that serialization is working properly.

            index_path = "first_half.bin"
            print("Saving index to '%s'" % index_path)
            p.save_index(index_path)
            print("Saved. Deleting...")
            del p
            print("Deleted")

            print("\n**** Mark delete test ****\n")
            # Re-initiating, loading the index
            print("Re-initiating")
            p = hnswlib.Index(space="l2", dim=dim)

            print("\nLoading index from '%s'\n" % index_path)
            p.load_index(index_path)
            p.set_ef(100)

            print("Adding the second batch of %d elements" % (len(data2)))
            p.add_items(data2)

            # Query the elements for themselves and measure recall:
            labels, distances = p.knn_query(data, k=1)
            items = p.get_items(labels)

            # Check the recall:
            self.assertAlmostEqual(
                np.mean(labels.reshape(-1) == np.arange(len(data))), 1.0, 3
            )

            # Check that the returned element data is correct:
            diff_with_gt_labels = np.mean(np.abs(data - items))
            self.assertAlmostEqual(
                diff_with_gt_labels, 0, delta=1e-4
            )  # deleting index.

            # Checking that all labels are returned correctly:
            sorted_labels = sorted(p.get_ids_list())
            self.assertEqual(
                np.sum(~np.asarray(sorted_labels) == np.asarray(range(num_elements))), 0
            )

            # Delete data1
            labels1_deleted, _ = p.knn_query(data1, k=1)
            # delete probable duplicates from nearest neighbors
            labels1_deleted_no_dup = set(labels1_deleted.flatten())
            for l in labels1_deleted_no_dup:
                p.mark_deleted(l)
            labels2, _ = p.knn_query(data2, k=1)
            items = p.get_items(labels2)
            diff_with_gt_labels = np.mean(np.abs(data2 - items))
            self.assertAlmostEqual(diff_with_gt_labels, 0, delta=1e-3)

            labels1_after, _ = p.knn_query(data1, k=1)
            for la in labels1_after:
                if la[0] in labels1_deleted_no_dup:
                    print(f"Found deleted label {la[0]} during knn search")
                    self.assertTrue(False)
            print("All the data in data1 are removed")

            # Checking saving/loading index with elements marked as deleted
            del_index_path = "with_deleted.bin"
            p.save_index(del_index_path)
            p = hnswlib.Index(space="l2", dim=dim)
            p.load_index(del_index_path)
            p.set_ef(100)

            labels1_after, _ = p.knn_query(data1, k=1)
            for la in labels1_after:
                if la[0] in labels1_deleted_no_dup:
                    print(
                        f"Found deleted label {la[0]} during knn search after index loading"
                    )
                    self.assertTrue(False)

            # Unmark deleted data
            for l in labels1_deleted_no_dup:
                p.unmark_deleted(l)
            labels_restored, _ = p.knn_query(data1, k=1)
            self.assertAlmostEqual(
                np.mean(labels_restored.reshape(-1) == np.arange(len(data1))), 1.0, 3
            )
            print("All the data in data1 are restored")

        os.remove(index_path)
        os.remove(del_index_path)
