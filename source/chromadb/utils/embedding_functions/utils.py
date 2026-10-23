import base64
import numpy as np
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from chromadb.api.shared_system_client import SharedSystemClient


def _get_shared_system_client() -> "type[SharedSystemClient]":
    """Lazy import of SharedSystemClient to avoid circular imports."""
    from chromadb.api.shared_system_client import SharedSystemClient

    return SharedSystemClient


def decode_embedding(b64_string: str):
    """Decode a base64-encoded int8 embedding."""
    return np.frombuffer(base64.b64decode(b64_string), dtype=np.int8).astype(np.float32)
