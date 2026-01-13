from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from chromadb.api.shared_system_client import SharedSystemClient


def _get_shared_system_client() -> "type[SharedSystemClient]":
    """Lazy import of SharedSystemClient to avoid circular imports."""
    from chromadb.api.shared_system_client import SharedSystemClient

    return SharedSystemClient
