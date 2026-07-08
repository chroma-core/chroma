import pytest

from chromadb.utils.messageid import bytes_to_int, int_to_bytes


@pytest.mark.parametrize("value", [0, 1, 255, 256, 123456789, 2**191])
def test_round_trip(value):
    assert bytes_to_int(int_to_bytes(value)) == value


def test_encoding_is_always_24_bytes():
    assert len(int_to_bytes(0)) == 24
    assert len(int_to_bytes(2**190)) == 24


def test_zero_is_all_zero_bytes():
    assert int_to_bytes(0) == b"\x00" * 24


def test_big_endian_ordering():
    # The least-significant byte is last in big-endian order.
    assert int_to_bytes(1)[-1] == 1
    assert int_to_bytes(1)[0] == 0


def test_negative_values_are_rejected():
    with pytest.raises(OverflowError):
        int_to_bytes(-1)


def test_values_too_large_to_fit_are_rejected():
    # 24 bytes hold values up to 256**24 - 1.
    with pytest.raises(OverflowError):
        int_to_bytes(256**24)
