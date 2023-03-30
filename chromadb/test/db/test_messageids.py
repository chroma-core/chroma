import chromadb.utils.messageid as mid
import random
import pulsar


def random_id():
    """Generate a random message id, with increased likelihood of generating boundary values"""

    partition = 0
    ledger = random.randint(0, 2**63 - 1)
    entry = random.randint(0, 2**63 - 1)
    batch = random.randint(0, 2**31 - 1)

    if random.random() < 0.1:
        ledger = 2**63 - 1
    elif random.random() < 0.1:
        ledger = 0

    if random.random() < 0.1:
        entry = 2**63 - 1
    elif random.random() < 0.1:
        entry = 0

    if random.random() < 0.1:
        batch = 2**31 - 1
    elif random.random() < 0.1:
        batch = 0

    return pulsar.MessageId(partition, ledger, entry, batch)


def test_roundtrip_formats():

    for i in range(10000):

        id1 = random_id()
        int1 = mid.pulsar_to_int(id1)
        str1 = mid.int_to_str(int1)
        b1 = mid.int_to_bytes(int1)
        assert int1 == mid.str_to_int(str1)
        assert int1 == mid.bytes_to_int(b1)

        id2 = mid.int_to_pulsar(int1)
        assert id1.ledger_id() == id2.ledger_id()
        assert id1.entry_id() == id2.entry_id()
        assert id1.batch_index() == id2.batch_index()


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


def test_byte_comparison():

    for i in range(10000):
        n1 = random.randint(0, 2**160 - 1)
        n2 = random.randint(0, 2**160 - 1)

        b1 = mid.int_to_bytes(n1)
        b2 = mid.int_to_bytes(n2)

        assert_compare((n1, n2), (b1, b2))


def test_str_comparison():

    for i in range(10000):

        id1 = random_id()
        int1 = mid.pulsar_to_int(id1)
        str1 = mid.int_to_str(int1)

        id2 = random_id()
        int2 = mid.pulsar_to_int(id2)
        str2 = mid.int_to_str(int2)

        assert_compare((int1, int2), (str1, str2))
