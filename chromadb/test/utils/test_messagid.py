import chromadb.utils.messageid as mid
import pulsar
import hypothesis.strategies as st
from hypothesis import given, settings, note
from typing import Any, Tuple


@st.composite
def message_id(draw: st.DrawFn) -> pulsar.MessageId:
    ledger_id = draw(st.integers(min_value=0, max_value=2**63 - 1))
    entry_id = draw(st.integers(min_value=0, max_value=2**63 - 1))
    batch_index = draw(st.integers(min_value=(2**31 - 1) * -1, max_value=2**31 - 1))
    partition = draw(st.integers(min_value=(2**31 - 1) * -1, max_value=2**31 - 1))
    return pulsar.MessageId(partition, ledger_id, entry_id, batch_index)


@given(message_id=message_id())
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_roundtrip_formats(message_id: pulsar.MessageId) -> None:
    int1 = mid.pulsar_to_int(message_id)

    # Roundtrip int->string and back
    str1 = mid.int_to_str(int1)
    assert int1 == mid.str_to_int(str1)

    # Roundtrip int->bytes and back
    b1 = mid.int_to_bytes(int1)
    assert int1 == mid.bytes_to_int(b1)

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
        note(f"Failed to compare {a} and {b} with {c} and {d}")
        note(f"type: {type(a)}")
        raise


@given(m1=message_id(), m2=message_id())
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_messageid_comparison(m1: pulsar.MessageId, m2: pulsar.MessageId) -> None:
    # MessageID comparison is broken in the Pulsar Python & CPP libraries:
    # The partition field is not taken into account, and two MessageIDs with different
    # partitions will compare inconsistently (m1 > m2 AND m2 > m1)
    # To avoid this, we zero-out the partition field before testing.
    m1 = pulsar.MessageId(0, m1.ledger_id(), m1.entry_id(), m1.batch_index())
    m2 = pulsar.MessageId(0, m2.ledger_id(), m2.entry_id(), m2.batch_index())

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


def test_max_values() -> None:
    pulsar.MessageId(2**31 - 1, 2**63 - 1, 2**63 - 1, 2**31 - 1)


@given(
    i1=st.integers(min_value=0, max_value=2**192 - 1),
    i2=st.integers(min_value=0, max_value=2**192 - 1),
)
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_string_comparison(i1: int, i2: int) -> None:
    assert_compare((i1, i2), (mid.int_to_str(i1), mid.int_to_str(i2)))
