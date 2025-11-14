from typing import ClassVar, Dict
import threading
import uuid

from chromadb.api import ServerAPI
from chromadb.config import Settings, System
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.telemetry.product.events import ClientStartEvent


class SharedSystemClient:
    _identifier_to_system: ClassVar[Dict[str, System]] = {}
    _identifier_to_clientcount: ClassVar[Dict[str, int]] = {}
    _count_lock: ClassVar[threading.Lock] = threading.Lock()
    _identifier: str
    _closed: bool

    def __init__(
        self,
        settings: Settings = Settings(),
    ) -> None:
        self._identifier = SharedSystemClient._get_identifier_from_settings(settings)
        SharedSystemClient._create_system_if_not_exists(self._identifier, settings)
        with SharedSystemClient._count_lock:
            SharedSystemClient._identifier_to_clientcount[self._identifier] = (
                SharedSystemClient._identifier_to_clientcount.get(self._identifier, 0)
                + 1
            )
        self._closed = False

    @classmethod
    def _create_system_if_not_exists(
        cls, identifier: str, settings: Settings
    ) -> System:
        with cls._count_lock:
            if identifier not in cls._identifier_to_system:
                new_system = System(settings)
                cls._identifier_to_system[identifier] = new_system
                cls._identifier_to_clientcount[identifier] = 0

                new_system.instance(ProductTelemetryClient)
                new_system.instance(ServerAPI)

                new_system.start()
            else:
                previous_system = cls._identifier_to_system[identifier]

                # For now, the settings must match
                if previous_system.settings != settings:
                    raise ValueError(
                        f"An instance of Chroma already exists for {identifier} with different settings"
                    )

            return cls._identifier_to_system[identifier]

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

    @staticmethod
    def _populate_data_from_system(system: System) -> str:
        identifier = SharedSystemClient._get_identifier_from_settings(system.settings)
        with SharedSystemClient._count_lock:
            SharedSystemClient._identifier_to_system[identifier] = system
        return identifier

    @classmethod
    def from_system(cls, system: System) -> "SharedSystemClient":
        """Create a client from an existing system. This is useful for testing and debugging."""

        SharedSystemClient._populate_data_from_system(system)
        instance = cls(system.settings)
        return instance

    def close(self) -> None:
        """Explicitly cleanup this client's system reference."""
        if hasattr(self, "_identifier") and not self._closed:
            identifier = self._identifier
            SharedSystemClient._decrement_refcount(identifier)
            self._closed = True
            # Prevent double-cleanup by removing identifier
            delattr(self, "_identifier")

    def __enter__(self) -> "SharedSystemClient":
        return self

    def __exit__(self, *args) -> None:  # type: ignore
        self.close()

    def __del__(self) -> None:
        """Fallback cleanup - prefer using close() or context manager."""
        try:
            if hasattr(self, "_identifier") and not getattr(self, "_closed", False):
                SharedSystemClient._decrement_refcount(self._identifier)
        except Exception:
            pass

    @classmethod
    def _decrement_refcount(cls, identifier: str) -> None:
        """Decrement reference count for a System and cleanup if no clients remain."""
        with cls._count_lock:
            if identifier not in cls._identifier_to_clientcount:
                return

            cls._identifier_to_clientcount[identifier] -= 1

            if cls._identifier_to_clientcount[identifier] <= 0:
                # since no more client using this system, can stop it and remove from cache
                if identifier in cls._identifier_to_system:
                    system = cls._identifier_to_system[identifier]
                    system.stop()
                    del cls._identifier_to_system[identifier]
                del cls._identifier_to_clientcount[identifier]

    @staticmethod
    def clear_system_cache() -> None:
        """Clear the system cache so that new systems can be created for an existing path.
        This should only be used for testing purposes."""
        with SharedSystemClient._count_lock:
            for system in SharedSystemClient._identifier_to_system.values():
                system.stop()
            SharedSystemClient._identifier_to_system = {}
            SharedSystemClient._identifier_to_clientcount = {}

    @property
    def _system(self) -> System:
        with SharedSystemClient._count_lock:
            return SharedSystemClient._identifier_to_system[self._identifier]

    def _submit_client_start_event(self) -> None:
        telemetry_client = self._system.instance(ProductTelemetryClient)
        telemetry_client.capture(ClientStartEvent())
