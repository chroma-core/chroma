from hypothesis import given, settings
from overrides import override
from starlette.datastructures import Headers
from typing import Any, Dict, List, Optional

from hypothesis.stateful import (
    Bundle,
    rule,
    initialize,
    multiple,
    run_state_machine_as_test,
    MultipleResults,
)
import hypothesis.strategies as st
import logging
import pytest
import string

from chromadb.auth import (
    ServerAuthenticationProvider,
    UserIdentity
)
from chromadb.api import ServerAPI
from chromadb.api.client import Client
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.test.conftest import (
  fastapi_fixture_admin_and_singleton_tenant_db_user
)
from chromadb.test.property.test_collections_with_database_tenant import (
  TenantDatabaseCollectionStateMachine,
)


# This test reuses the state machines from test_collections.py and
# test_collections_with_database_tenant.py. However instead of always using the
# test's default api and admin_api (which have full permissions over all
# tenants and dbs), we sometimes use api clients which only have access to
# a single tenant and database. When this happens, we expect all requests
# to be routed to that tenant and database regardless of which tenant and
# database the request specifies.


class SingletonTenantDatabaseCollectionStateMachine(
    TenantDatabaseCollectionStateMachine
):
    def __init__(self, singleton_client: Client, root_client: Client) -> None:
        super().__init__(root_client)

    @initialize()
    def initialize(self) -> None:
        super().initialize()


def test_collections_with_tenant_database_overwrite(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)

    api_fixture = fastapi_fixture_admin_and_singleton_tenant_db_user()
    sys: System = next(api_fixture)
    sys.reset_state()
    client = Client.from_system(sys)

    root_settings = Settings(**dict(sys.settings))
    root_settings.chroma_client_auth_credentials = "admin-token"
    system = System(root_settings)
    system.start()
    root_client = Client.from_system(system)

    run_state_machine_as_test(
        lambda: SingletonTenantDatabaseCollectionStateMachine(
            client,
            root_client,
        )
    )  # type: ignore
