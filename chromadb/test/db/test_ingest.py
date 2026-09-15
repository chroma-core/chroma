import numpy as np
import pytest

from chromadb.ingest import decode_vector, encode_vector
from chromadb.types import ScalarEncoding


@pytest.mark.parametrize("encoding", [ScalarEncoding.FLOAT32, ScalarEncoding.INT32])
def test_encode_decode_roundtrip(encoding: ScalarEncoding) -> None:
    dtype = np.int32 if encoding == ScalarEncoding.INT32 else np.float32
    vector = np.array([1, 2, 3], dtype=dtype)
    decoded = decode_vector(encode_vector(vector, encoding), encoding)
    np.testing.assert_array_equal(decoded, vector)
    assert decoded.dtype == vector.dtype


def test_decode_int32_preserves_dtype() -> None:
    encoded = np.array([7, -3, 11], dtype=np.int32).tobytes()
    decoded = decode_vector(encoded, ScalarEncoding.INT32)
    assert decoded.dtype == np.int32
    np.testing.assert_array_equal(decoded, np.array([7, -3, 11], dtype=np.int32))
