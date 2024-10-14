from typing import ClassVar, Dict, Optional, Tuple, List
import uuid

from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT
from chromadb.config import Settings, System
from chromadb.telemetry.product import ProductTelemetryClient
from chromadb.telemetry.product.events import ClientStartEvent


class SharedSystemClient:
    _identifier_to_system: ClassVar[Dict[str, System]] = {}
    _identifier: str

    def __init__(
        self,
        settings: Settings = Settings(),
    ) -> None:
        self._identifier = SharedSystemClient._get_identifier_from_settings(settings)
        SharedSystemClient._create_system_if_not_exists(self._identifier, settings)

    @classmethod
    def _create_system_if_not_exists(
        cls, identifier: str, settings: Settings
    ) -> System:
        if identifier not in cls._identifier_to_system:
            new_system = System(settings)
            cls._identifier_to_system[identifier] = new_system

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
        elif api_impl == "chromadb.api.segment.SegmentAPI":
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
        SharedSystemClient._identifier_to_system[identifier] = system
        return identifier

    @classmethod
    def from_system(cls, system: System) -> "SharedSystemClient":
        """Create a client from an existing system. This is useful for testing and debugging."""

        SharedSystemClient._populate_data_from_system(system)
        instance = cls(system.settings)
        return instance

    @staticmethod
    def clear_system_cache() -> None:
        SharedSystemClient._identifier_to_system = {}

    @property
    def _system(self) -> System:
        return SharedSystemClient._identifier_to_system[self._identifier]

    def _submit_client_start_event(self) -> None:
        telemetry_client = self._system.instance(ProductTelemetryClient)
        telemetry_client.capture(ClientStartEvent())

    @staticmethod
    def _singleton_tenant_database_if_applicable(
        overwrite_singleton_tenant_database_access_from_auth: bool,
        user_tenant: Optional[str],
        user_databases: Optional[List[str]],
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        If settings.chroma_overwrite_singleton_tenant_database_access_from_auth
        is False, this function always returns (None, None).

        If settings.chroma_overwrite_singleton_tenant_database_access_from_auth
        is True, follows the following logic:
        - If the user only has access to a single tenant, this function will
          return that tenant as its first return value.
        - If the user only has access to a single database, this function will
          return that database as its second return value. If the user has
          access to multiple tenants and/or databases, including "*", this
          function will return None for the corresponding value(s).
        - If the user has access to multiple tenants and/or databases this
          function will return None for the corresponding value(s).
        """
        if not overwrite_singleton_tenant_database_access_from_auth:
            return None, None
        tenant = None
        database = None
        if user_tenant and user_tenant != "*":
            tenant = user_tenant
        if user_databases and len(user_databases) == 1 and user_databases[0] != "*":
            database = user_databases[0]
        return tenant, database

    @staticmethod
    def maybe_set_tenant_and_database(
        overwrite_singleton_tenant_database_access_from_auth: bool,
        tenant: Optional[str] = None,
        database: Optional[str] = None,
        user_tenant: Optional[str] = None,
        user_databases: Optional[List[str]] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        (
            new_tenant,
            new_database,
        ) = SharedSystemClient._singleton_tenant_database_if_applicable(
            overwrite_singleton_tenant_database_access_from_auth,
            user_tenant,
            user_databases,
        )

        if (not tenant or tenant == DEFAULT_TENANT) and new_tenant:
            tenant = new_tenant
        if (not database or database == DEFAULT_DATABASE) and new_database:
            database = new_database

        return tenant, database
