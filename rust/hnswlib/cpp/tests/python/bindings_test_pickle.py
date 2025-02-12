import pickle
import unittest

import numpy as np

import hnswlib


def get_dist(metric, pt1, pt2):
    if metric == "l2":
        return np.sum((pt1 - pt2) ** 2)
    elif metric == "ip":
        return 1.0 - np.sum(np.multiply(pt1, pt2))
    elif metric == "cosine":
        return (
            1.0
            - np.sum(np.multiply(pt1, pt2))
            / (np.sum(pt1**2) * np.sum(pt2**2)) ** 0.5
        )


def brute_force_distances(metric, items, query_items, k):
    dists = np.zeros((query_items.shape[0], items.shape[0]))
    for ii in range(items.shape[0]):
        for jj in range(query_items.shape[0]):
            dists[jj, ii] = get_dist(metric, items[ii, :], query_items[jj, :])

    labels = np.argsort(
        dists, axis=1
    )  # equivalent, but faster: np.argpartition(dists, range(k), axis=1)
    dists = np.sort(
        dists, axis=1
    )  # equivalent, but faster: np.partition(dists, range(k), axis=1)

    return labels[:, :k], dists[:, :k]


def check_ann_results(
    self,
    metric,
    items,
    query_items,
    k,
    ann_l,
    ann_d,
    err_thresh=0,
    total_thresh=0,
    dists_thresh=0,
):
    brute_l, brute_d = brute_force_distances(metric, items, query_items, k)
    err_total = 0
    for jj in range(query_items.shape[0]):
        err = np.sum(np.isin(brute_l[jj, :], ann_l[jj, :], invert=True))
        if err > 0:
            print(
                f"Warning: {err} labels are missing from ann results (k={k}, err_thresh={err_thresh})"
            )

        if err > err_thresh:
            err_total += 1

    self.assertLessEqual(
        err_total,
        total_thresh,
        f"Error: knn_query returned incorrect labels for {err_total} items (k={k})",
    )

    wrong_dists = np.sum(((brute_d - ann_d) ** 2.0) > 1e-3)
    if wrong_dists > 0:
        dists_count = brute_d.shape[0] * brute_d.shape[1]
        print(
            f"Warning: {wrong_dists} ann distance values are different from brute-force values (total # of values={dists_count}, dists_thresh={dists_thresh})"
        )

    self.assertLessEqual(
        wrong_dists,
        dists_thresh,
        msg=f"Error: {wrong_dists} ann distance values are different from brute-force values",
    )


def test_space_main(self, space, dim):
    # Generating sample data
    data = np.float32(np.random.random((self.num_elements, dim)))
    test_data = np.float32(np.random.random((self.num_test_elements, dim)))

    # Declaring index
    p = hnswlib.Index(space=space, dim=dim)  # possible options are l2, cosine or ip
    print(f"Running pickle tests for {p}")

    p.num_threads = self.num_threads  # by default using all available cores

    p0 = pickle.loads(pickle.dumps(p))  # pickle un-initialized Index
    p.init_index(
        max_elements=self.num_elements, ef_construction=self.ef_construction, M=self.M
    )
    p0.init_index(
        max_elements=self.num_elements, ef_construction=self.ef_construction, M=self.M
    )

    p.ef = self.ef
    p0.ef = self.ef

    p1 = pickle.loads(pickle.dumps(p))  # pickle Index before adding items

    # add items to ann index p,p0,p1
    p.add_items(data)
    p1.add_items(data)
    p0.add_items(data)

    p2 = pickle.loads(pickle.dumps(p))  # pickle Index before adding items

    self.assertTrue(
        np.allclose(p.get_items(), p0.get_items()), "items for p and p0 must be same"
    )
    self.assertTrue(
        np.allclose(p0.get_items(), p1.get_items()), "items for p0 and p1 must be same"
    )
    self.assertTrue(
        np.allclose(p1.get_items(), p2.get_items()), "items for p1 and p2 must be same"
    )

    # Test if returned distances are same
    l, d = p.knn_query(test_data, k=self.k)
    l0, d0 = p0.knn_query(test_data, k=self.k)
    l1, d1 = p1.knn_query(test_data, k=self.k)
    l2, d2 = p2.knn_query(test_data, k=self.k)

    self.assertLessEqual(
        np.sum(((d - d0) ** 2.0) > 1e-3),
        self.dists_err_thresh,
        msg=f"knn distances returned by p and p0 must match",
    )
    self.assertLessEqual(
        np.sum(((d0 - d1) ** 2.0) > 1e-3),
        self.dists_err_thresh,
        msg=f"knn distances returned by p0 and p1 must match",
    )
    self.assertLessEqual(
        np.sum(((d1 - d2) ** 2.0) > 1e-3),
        self.dists_err_thresh,
        msg=f"knn distances returned by p1 and p2 must match",
    )

    # check if ann results match brute-force search
    #   allow for 2 labels to be missing from ann results
    check_ann_results(
        self,
        space,
        data,
        test_data,
        self.k,
        l,
        d,
        err_thresh=self.label_err_thresh,
        total_thresh=self.item_err_thresh,
        dists_thresh=self.dists_err_thresh,
    )

    check_ann_results(
        self,
        space,
        data,
        test_data,
        self.k,
        l2,
        d2,
        err_thresh=self.label_err_thresh,
        total_thresh=self.item_err_thresh,
        dists_thresh=self.dists_err_thresh,
    )

    # Check ef parameter value
    self.assertEqual(p.ef, self.ef, "incorrect value of p.ef")
    self.assertEqual(p0.ef, self.ef, "incorrect value of p0.ef")
    self.assertEqual(p2.ef, self.ef, "incorrect value of p2.ef")
    self.assertEqual(p1.ef, self.ef, "incorrect value of p1.ef")

    # Check M parameter value
    self.assertEqual(p.M, self.M, "incorrect value of p.M")
    self.assertEqual(p0.M, self.M, "incorrect value of p0.M")
    self.assertEqual(p1.M, self.M, "incorrect value of p1.M")
    self.assertEqual(p2.M, self.M, "incorrect value of p2.M")

    # Check ef_construction parameter value
    self.assertEqual(
        p.ef_construction, self.ef_construction, "incorrect value of p.ef_construction"
    )
    self.assertEqual(
        p0.ef_construction,
        self.ef_construction,
        "incorrect value of p0.ef_construction",
    )
    self.assertEqual(
        p1.ef_construction,
        self.ef_construction,
        "incorrect value of p1.ef_construction",
    )
    self.assertEqual(
        p2.ef_construction,
        self.ef_construction,
        "incorrect value of p2.ef_construction",
    )


class PickleUnitTests(unittest.TestCase):
    def setUp(self):
        self.ef_construction = 200
        self.M = 32
        self.ef = 400

        self.num_elements = 1000
        self.num_test_elements = 100

        self.num_threads = 4
        self.k = 25

        self.label_err_thresh = 5  # max number of missing labels allowed per test item
        self.item_err_thresh = 5  # max number of items allowed with incorrect labels

        self.dists_err_thresh = (
            50  # for two matrices, d1 and d2, dists_err_thresh controls max
        )
        # number of value pairs that are allowed to be different in d1 and d2
        # i.e., number of values that are (d1-d2)**2>1e-3

    def test_inner_product_space(self):
        test_space_main(self, "ip", 16)

    def test_l2_space(self):
        test_space_main(self, "l2", 53)

    def test_cosine_space(self):
        test_space_main(self, "cosine", 32)
