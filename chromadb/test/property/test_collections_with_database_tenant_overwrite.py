from typing import Dict, Optional

from hypothesis.stateful import (
    Bundle,
    initialize,
    run_state_machine_as_test,
)
import logging
import pytest

import chromadb.api.types as types
from chromadb.api.client import AdminClient, Client
from chromadb.config import Settings, System
from chromadb.test.conftest import (
  fastapi_fixture_admin_and_singleton_tenant_db_user
)
from chromadb.test.property.test_collections_with_database_tenant import (
  TenantDatabaseCollectionStateMachine,
)


# See conftest.py
SINGLETON_TENANT = 'singleton_tenant'
SINGLETON_DATABASE = 'singleton_database'


class SingletonTenantDatabaseCollectionStateMachine(
    TenantDatabaseCollectionStateMachine
):
    clients: Bundle[Client]

    singleton_client: Client
    singleton_admin_client: AdminClient
    root_client: Client
    root_admin_client: AdminClient

    def __init__(self, singleton_client: Client, root_client: Client) -> None:
        super().__init__(singleton_client)

        self.singleton_client = singleton_client
        self.singleton_admin_client = AdminClient.from_system(singleton_client._system)
        self.tenant_to_database_to_model[SINGLETON_TENANT] = {}
        self.tenant_to_database_to_model[SINGLETON_TENANT][SINGLETON_DATABASE] = {}

        self.root_client = root_client
        self.root_admin_client = AdminClient.from_system(root_client._system)

    @initialize()
    def initialize(self) -> None:
        self.api = self.root_client
        self.admin_client = self.root_admin_client
        super().initialize()

    def get_tenant_model(
        self,
        tenant: str
    ) -> Dict[str, Dict[str, Optional[types.CollectionMetadata]]]:
        if self.api == self.singleton_client:
            tenant = SINGLETON_TENANT
        return self.tenant_to_database_to_model[tenant]

    def set_tenant_model(
        self,
        tenant: str,
        model: Dict[str, Dict[str, Optional[types.CollectionMetadata]]]
    ) -> None:
        if self.api == self.singleton_client:
            # This never happens because we never actually issue a
            # create_tenant call on singleton_tenant:
            # thanks to the above overriding of get_tenant_model(),
            # the underlying state machine test should always expect an error
            # when it sends the request, so shouldn't try to update the model.
            raise ValueError('trying to overwrite the model for singleton??')
        self.tenant_to_database_to_model[tenant] = model

    def has_database_for_tenant(self, tenant: str, database: str) -> bool:
        if self.api == self.singleton_client:
            tenant = SINGLETON_TENANT
            database = SINGLETON_DATABASE
        return database in self.tenant_to_database_to_model[tenant]

    def set_database_model_for_tenant(
        self,
        tenant: str,
        database: str,
        database_model: Dict[str, Optional[types.CollectionMetadata]]
    ) -> None:
        if self.api == self.singleton_client:
            # This never happens because we never actually issue a
            # create_database call on (singleton_tenant, singleton_database):
            # thanks to the above overriding of has_database_for_tenant(),
            # the underlying state machine test should always expect an error
            # when it sends the request, so shouldn't try to update the model.
            raise ValueError('trying to overwrite the model for singleton??')
        self.tenant_to_database_to_model[tenant][database] = database_model


def test_collections_with_tenant_database_overwrite(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)

    api_fixture = fastapi_fixture_admin_and_singleton_tenant_db_user()
    sys: System = next(api_fixture)
    sys.reset_state()
    root_client = Client.from_system(sys)
    _root_admin_client = AdminClient.from_system(sys)

    # This is a little awkward but we have to create the tenant and DB
    # before we can instantiate a Client which connects to them. This also
    # means we need to manually populate state in the state machine.
    _root_admin_client.create_tenant(SINGLETON_TENANT)
    _root_admin_client.create_database(SINGLETON_DATABASE, SINGLETON_TENANT)

    singleton_settings = Settings(**dict(sys.settings))
    singleton_settings.chroma_client_auth_credentials = "singleton-token"
    singleton_system = System(singleton_settings)
    singleton_system.start()
    singleton_client = Client.from_system(singleton_system)

    run_state_machine_as_test(
        lambda: SingletonTenantDatabaseCollectionStateMachine(
            singleton_client,
            root_client,
        )
    )  # type: ignore
