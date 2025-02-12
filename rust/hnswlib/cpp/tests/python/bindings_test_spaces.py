import unittest

import numpy as np

import hnswlib


class RandomSelfTestCase(unittest.TestCase):
    def testRandomSelf(self):
        data1 = np.asarray(
            [
                [1, 0, 0],
                [0, 1, 0],
                [0, 0, 1],
                [1, 0, 1],
                [1, 1, 1],
            ]
        )

        for space, expected_distances in [
            ("l2", [[0.0, 1.0, 2.0, 2.0, 2.0]]),
            ("ip", [[-2.0, -1.0, 0.0, 0.0, 0.0]]),
            ("cosine", [[0, 1.835e-01, 4.23e-01, 4.23e-01, 4.23e-01]]),
        ]:
            for rightdim in range(1, 128, 3):
                for leftdim in range(1, 32, 5):
                    data2 = np.concatenate(
                        [
                            np.zeros([data1.shape[0], leftdim]),
                            data1,
                            np.zeros([data1.shape[0], rightdim]),
                        ],
                        axis=1,
                    )
                    dim = data2.shape[1]
                    p = hnswlib.Index(space=space, dim=dim)
                    p.init_index(max_elements=5, ef_construction=100, M=16)

                    p.set_ef(10)

                    p.add_items(data2)

                    # Query the elements for themselves and measure recall:
                    labels, distances = p.knn_query(np.asarray(data2[-1:]), k=5)

                    diff = np.mean(np.abs(distances - expected_distances))
                    self.assertAlmostEqual(diff, 0, delta=1e-3)
