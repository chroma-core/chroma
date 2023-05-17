import chromadb.utils.messageid as mid
import pulsar
import hypothesis.strategies as st
from hypothesis import given, settings  # , note
from typing import Any, Tuple

MIN_64_INT = -(2**63)
MAX_64_INT = 2**63 - 1
MIN_32_INT = -(2**31)
MAX_32_INT = 2**31 - 1


def tuple_to_int(message_id: Tuple[int, int, int, int]) -> int:
    partition, ledger_id, entry_id, batch_index = message_id
    return ledger_id << 128 | entry_id << 64 | batch_index << 32 | partition


def int_to_tuple(message_id_int: int) -> Tuple[int, int, int, int]:
    partition = message_id_int & 0xFFFFFFFF
    ledger_id = 0  # (message_id_int >> 128) & 0xFFFFFFFFFFFFFFFF
    entry_id = 0  # (message_id_int >> 64) & 0xFFFFFFFFFFFFFFFF
    batch_index = 0  # (message_id_int >> 32) & 0xFFFFFFFF

    return partition, ledger_id, entry_id, batch_index


@st.composite
def message_id(draw: st.DrawFn) -> pulsar.MessageId:
    ledger_id = draw(st.integers(min_value=0, max_value=MAX_64_INT))
    entry_id = draw(st.integers(min_value=0, max_value=MAX_64_INT))
    batch_index = draw(st.integers(min_value=MIN_32_INT, max_value=MAX_32_INT))
    partition = draw(st.integers(min_value=MIN_32_INT, max_value=MAX_32_INT))
    return pulsar.MessageId(partition, ledger_id, entry_id, batch_index)


def test_roundtrip_tuples() -> None:
    message_id = pulsar.MessageId(-1, 0, 0, 0)

    t1 = (
        message_id.partition(),
        message_id.ledger_id(),
        message_id.entry_id(),
        message_id.batch_index(),
    )
    # note("t1:" + str(t1))
    int1 = tuple_to_int(t1)
    t2 = int_to_tuple(int1)
    # note("t2:" + str(t2))
    assert t1 == t2


@given(message_id=message_id())
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_roundtrip_formats(message_id: pulsar.MessageId) -> None:
    int1 = mid.pulsar_to_int(message_id)

    # Roundtrip int->string and back
    # str1 = mid.int_to_str(int1)
    # assert int1 == mid.str_to_int(str1)

    # Roundtrip int->bytes and back
    # b1 = mid.int_to_bytes(int1)
    # assert int1 == mid.bytes_to_int(b1)

    # Roundtrip int -> MessageId and back
    message_id_result = mid.int_to_pulsar(int1)
    assert message_id_result.partition() == message_id.partition()
    assert message_id_result.ledger_id() == message_id.ledger_id()
    assert message_id_result.entry_id() == message_id.entry_id()
    assert message_id_result.batch_index() == message_id.batch_index()


def assert_compare(pair1: Tuple[Any, Any], pair2: Tuple[Any, Any]) -> None:
    """Helper function: assert that the two pairs of values always compare in the same
    way across all comparisons and orderings."""

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


@given(m1=message_id(), m2=message_id())
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_comparison(m1: pulsar.MessageId, m2: pulsar.MessageId) -> None:
    i1 = mid.pulsar_to_int(m1)
    i2 = mid.pulsar_to_int(m2)

    # In python, MessageId objects are not comparable directory, but the
    # internal generated native object is.
    internal1 = m1._msg_id
    internal2 = m2._msg_id

    s1 = mid.int_to_str(i1)
    s2 = mid.int_to_str(i2)

    # assert that all strings, all ints, and all native  objects compare the same
    assert_compare((internal1, internal2), (i1, i2))
    assert_compare((internal1, internal2), (s1, s2))
    assert_compare((i1, i2), (s1, s2))


def test_max_values() -> None:
    pulsar.MessageId(MAX_32_INT, MAX_64_INT, MAX_64_INT, MAX_32_INT)
