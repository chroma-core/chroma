import base64
import os
import numpy as np
from typing import TYPE_CHECKING

DEFAULT_CHROMA_EMBED_URL = "https://embed.trychroma.com"


def get_chroma_embed_url() -> str:
    """Return embed base URL from CHROMA_EMBED_URL or default without trailing slash."""
    return (os.environ.get("CHROMA_EMBED_URL") or DEFAULT_CHROMA_EMBED_URL).rstrip("/")

if TYPE_CHECKING:
    from chromadb.api.shared_system_client import SharedSystemClient


def _get_shared_system_client() -> "type[SharedSystemClient]":
    """Lazy import of SharedSystemClient to avoid circular imports."""
    from chromadb.api.shared_system_client import SharedSystemClient

    return SharedSystemClient


def decode_embedding(b64_string: str):
    """Decode a base64-encoded int8 embedding."""
    return np.frombuffer(base64.b64decode(b64_string), dtype=np.int8).astype(np.float32)
