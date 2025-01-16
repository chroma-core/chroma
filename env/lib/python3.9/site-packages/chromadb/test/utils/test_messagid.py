import chromadb.utils.messageid as mid
import hypothesis.strategies as st
from hypothesis import given, settings


@st.composite
def message_id(draw: st.DrawFn) -> int:
    offset_id = draw(st.integers(min_value=0, max_value=2**63 - 1))
    return offset_id


@given(message_id=message_id())
@settings(max_examples=10000)  # these are very fast and we want good coverage
def test_roundtrip_formats(message_id: int) -> None:
    int1 = message_id

    # Roundtrip int->bytes and back
    b1 = mid.int_to_bytes(int1)
    assert int1 == mid.bytes_to_int(b1)
