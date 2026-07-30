from typing import ClassVar, Dict, Optional
import logging
import threading
import uuid
from weakref import WeakSet
from chromadb.api import ServerAPI
from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.config import Settings, System
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.telemetry.product.events import ClientStartEvent

logger = logging.getLogger(__name__)


class SharedSystemClient:
    _identifier_to_system: ClassVar[Dict[str, System]] = {}
    _identifier_to_refcount: ClassVar[Dict[str, int]] = {}
    _released_systems: ClassVar[WeakSet[System]] = WeakSet()
    _refcount_lock: ClassVar[threading.Lock] = threading.Lock()
    _identifier: str

    def __init__(
        self,
        settings: Settings = Settings(),
        *,
        _system: Optional[System] = None,
    ) -> None:
        if _system is None:
            self._identifier = SharedSystemClient._create_and_retain_system(settings)
        else:
            self._identifier = SharedSystemClient._retain_system(_system)

    @classmethod
    def _create_and_retain_system(cls, settings: Settings) -> str:
        """Create or reuse a system and retain it as one atomic operation."""
        identifier = cls._get_identifier_from_settings(settings)
        with cls._refcount_lock:
            if identifier not in cls._identifier_to_system:
                new_system = System(settings)
                try:
                    new_system.instance(ProductTelemetryClient)
                    new_system.instance(ServerAPI)
                    new_system.start()
                except Exception:
                    try:
                        new_system.stop()
                    except Exception:
                        logger.exception(
                            "Failed to stop Chroma system after initialization error"
                        )
                    raise
                cls._identifier_to_system[identifier] = new_system
            else:
                previous_system = cls._identifier_to_system[identifier]

                # For now, the settings must match
                if previous_system.settings != settings:
                    raise ValueError(
                        f"An instance of Chroma already exists for {identifier} with different settings"
                    )

            cls._identifier_to_refcount[identifier] = (
                cls._identifier_to_refcount.get(identifier, 0) + 1
            )
            return identifier

    @staticmethod
    def _get_identifier_from_settings(settings: Settings) -> str:
        identifier = ""
        api_impl = settings.chroma_api_impl

        if api_impl is None:
            raise ValueError("Chroma API implementation must be set in settings")
        elif api_impl in [
            "chromadb.api.segment.SegmentAPI",
            "chromadb.api.rust.RustBindingsAPI",
        ]:
            if settings.is_persistent:
                identifier = settings.persist_directory
            else:
                identifier = (
                    "ephemeral"  # TODO: support pathing and  multiple ephemeral clients
                )
        elif api_impl in [
            "chromadb.api.fastapi.FastAPI",
            "chromadb.api.async_fastapi.AsyncFastAPI",
        ]:
            # FastAPI clients can all use unique system identifiers since their configurations can be independent, e.g. different auth tokens
            identifier = str(uuid.uuid4())
        else:
            raise ValueError(f"Unsupported Chroma API implementation {api_impl}")

        return identifier

    @classmethod
    def from_system(cls, system: System) -> "SharedSystemClient":
        """Create a client from an existing system. This is useful for testing and debugging."""

        instance = cls(system.settings, _system=system)
        return instance

    @classmethod
    def _retain_system(cls, system: System) -> str:
        """Retain an exact System instance and return its cache identifier."""
        with cls._refcount_lock:
            if system in cls._released_systems:
                raise ValueError(
                    "Cannot retain a Chroma System after its final reference was released"
                )

            identifier = next(
                (
                    identifier
                    for identifier, cached_system in cls._identifier_to_system.items()
                    if cached_system is system
                ),
                None,
            )
            if identifier is None:
                identifier = cls._get_identifier_from_settings(system.settings)
                if identifier not in cls._identifier_to_system:
                    cls._identifier_to_system[identifier] = system
                elif system.settings.chroma_api_impl in [
                    "chromadb.api.fastapi.FastAPI",
                    "chromadb.api.async_fastapi.AsyncFastAPI",
                ]:
                    while identifier in cls._identifier_to_system:
                        identifier = str(uuid.uuid4())
                    cls._identifier_to_system[identifier] = system

            cls._identifier_to_refcount[identifier] = (
                cls._identifier_to_refcount.get(identifier, 0) + 1
            )
            return identifier

    @classmethod
    def _release_system(cls, identifier: str) -> None:
        """Decrement refcount and stop the system if this was the last reference.

        This consolidates the "decrement + conditional stop" pattern used in
        both Client.close() and the Client.__init__ exception handler.
        """
        system = None
        with cls._refcount_lock:
            refcount = cls._identifier_to_refcount.get(identifier, 0) - 1
            if refcount > 0:
                cls._identifier_to_refcount[identifier] = refcount
            else:
                cls._identifier_to_refcount.pop(identifier, None)
                system = cls._identifier_to_system.pop(identifier, None)
                if system is not None:
                    cls._released_systems.add(system)

        if system is not None:
            system.stop()

    @classmethod
    def _release_system_on_error(cls, identifier: str) -> None:
        """Release a system during rollback without masking the original error."""
        try:
            cls._release_system(identifier)
        except Exception:
            logger.exception("Failed to stop Chroma system during client rollback")

    @staticmethod
    def clear_system_cache() -> None:
        with SharedSystemClient._refcount_lock:
            SharedSystemClient._identifier_to_system = {}
            SharedSystemClient._identifier_to_refcount = {}
            SharedSystemClient._released_systems = WeakSet()

    @property
    def _system(self) -> System:
        return SharedSystemClient._identifier_to_system[self._identifier]

    def _submit_client_start_event(self) -> None:
        telemetry_client = self._system.instance(ProductTelemetryClient)
        telemetry_client.capture(ClientStartEvent())

    @staticmethod
    def get_chroma_cloud_api_key_from_clients() -> Optional[str]:
        """
        Try to extract api key from existing client instances by checking httpx session headers.

        Requirements to pull api key:
        - must be a BaseHTTPClient instance (ignore RustBindingsAPI and SegmentAPI)
        - must have "api.trychroma.com" or "gcp.trychroma.com" in the _api_url (ignore local/self-hosted instances)
        - must have "x-chroma-token" or "X-Chroma-Token" in the headers

        Returns:
            The first api key found, or None if no client instances have api keys set.
        """

        api_keys: list[str] = []
        systems_snapshot = list(SharedSystemClient._identifier_to_system.values())
        for system in systems_snapshot:
            try:
                server_api = system.instance(ServerAPI)

                if not isinstance(server_api, BaseHTTPClient):
                    # RustBindingsAPI and SegmentAPI don't have HTTP headers
                    continue

                # Only pull api key if the url contains the chroma cloud url
                api_url = server_api.get_api_url()
                if (
                    "api.trychroma.com" not in api_url
                    and "gcp.trychroma.com" not in api_url
                ):
                    continue

                headers = server_api.get_request_headers()
                api_key = None
                for key, value in headers.items():
                    if key.lower() == "x-chroma-token":
                        api_key = value
                        break

                if api_key:
                    api_keys.append(api_key)
            except Exception:
                # If we can't access the ServerAPI instance, continue to the next
                continue

        if not api_keys:
            return None

        # log if multiple viable api keys found
        if len(api_keys) > 1:
            logger.info(
                f"Multiple Chroma Cloud clients found, using API key starting with {api_keys[0][:8]}..."
            )

        return api_keys[0]
