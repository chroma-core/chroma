import pulsar
import _pulsar
import random

# Tests to ensure that MessageIDs can be serialized to strongs and compared properly


def assert_compare(pair1, pair2):
    """Assert that the two pairs of values always compare in the same way"""

    a, b = pair1
    c, d = pair2

    try:
        assert (a > b) == (c > d)
        assert (a >= b) == (c >= d)
        assert (a < b) == (c < d)
        assert (a <= b) == (c <= d)
        assert (a == b) == (c == d)
    except AssertionError:
        print(f"Failed to compare {a} and {b} with {c} and {d}")
        raise


def rand_messageid():
    """Generate a random message ID"""
    return _pulsar.MessageId(
        0,
        random.randint(0, 2**31 - 1),
        random.randint(0, 2**31 - 1),
        random.randint(0, 2**31 - 1),
    )


def test_message_ids():

    for i in range(10000):

        m1 = rand_messageid()
        m2 = rand_messageid()

        # Construction order is partitions, ledger, entry, batch
        # Compare order is ledger, entry, batch
        t1 = (m1.ledger_id(), m1.entry_id(), m1.batch_index())
        t2 = (m2.ledger_id(), m2.entry_id(), m2.batch_index())

        assert_compare((t1, t2), (m1, m2))
        # assert_compare((t1, t2), (t1, t2))
