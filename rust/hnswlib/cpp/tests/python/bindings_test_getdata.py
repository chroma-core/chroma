import unittest

import numpy as np

import hnswlib


class RandomSelfTestCase(unittest.TestCase):
    def testGettingItems(self):
        print("\n**** Getting the data by label test ****\n")

        dim = 16
        num_elements = 10000

        # Generating sample data
        data = np.float32(np.random.random((num_elements, dim)))
        labels = np.arange(0, num_elements)

        for space in ["l2", "ip", "cosine"]:
            # Declaring index
            p = hnswlib.Index(
                space=space, dim=dim
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

            # Before adding anything, getting any labels should fail
            self.assertRaises(Exception, lambda: p.get_items(labels))

            print("Adding all elements (%d)" % (len(data)))
            p.add_items(data, labels)

            # Getting data by label should raise an exception if a scalar is passed:
            self.assertRaises(ValueError, lambda: p.get_items(labels[0]))

            # After adding them, all labels should be retrievable
            returned_items = p.get_items(labels)
            self.assertTrue(np.allclose(data, returned_items, atol=1e-6))
