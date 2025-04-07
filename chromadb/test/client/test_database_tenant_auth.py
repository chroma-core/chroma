import os
from typing import Dict, cast
from fastapi import HTTPException
from overrides import override
import chromadb
from chromadb.api import ServerAPI
from chromadb.auth import (
    AuthzAction,
    AuthzResource,
    ServerAuthenticationProvider,
    ServerAuthorizationProvider,
    UserIdentity,
)
from chromadb.config import Settings, System
from chromadb.test.conftest import _fastapi_fixture
from hypothesis.stateful import (
    run_state_machine_as_test,
)
from chromadb.test.property.test_embeddings import EmbeddingStateMachine


class ExampleAuthenticationProvider(ServerAuthenticationProvider):
    """In practice the tenant would likely be resolved from some other opaque value (e.g. key/token). Here, it's just passed directly as a header for simplicity."""

    @override
    def authenticate_or_raise(self, headers: Dict[str, str]) -> UserIdentity:
        return UserIdentity(
            user_id="test",
            tenant=headers.get("x-tenant", None),
        )


class ExampleAuthorizationProvider(ServerAuthorizationProvider):
    """A simple authz provider that asserts the user's tenant matches the resource's tenant."""

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings

    @override
    def authorize_or_raise(
        self, user: UserIdentity, action: AuthzAction, resource: AuthzResource
    ) -> None:
        if user.tenant is None:
            return

        if action == AuthzAction.RESET:
            return

        if user.tenant != resource.tenant:
            raise HTTPException(status_code=403, detail="Unauthorized")


def test_tenant_and_database_passed_from_client() -> None:
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        host = os.environ.get("CHROMA_SERVER_HOST", "localhost")
        port = int(os.environ.get("CHROMA_SERVER_HTTP_PORT", 0))

        settings = Settings()
        settings.chroma_api_impl = "chromadb.api.fastapi.FastAPI"
        settings.chroma_server_http_port = port
        settings.chroma_server_host = host
        admin_client = chromadb.AdminClient(settings)
        admin_client.create_tenant("test_tenant")
        admin_client.create_database("test_database", "test_tenant")
    else:
        api_fixture = _fastapi_fixture(
            chroma_server_authn_provider="chromadb.test.client.test_database_tenant_auth.ExampleAuthenticationProvider",
            chroma_server_authz_provider="chromadb.test.client.test_database_tenant_auth.ExampleAuthorizationProvider",
        )
        sys: System = next(api_fixture)
        sys.reset_state()

        server = sys.require(ServerAPI)
        server.create_tenant("test_tenant")
        server.create_database("test_database", "test_tenant")
        host = cast(str, sys.settings.chroma_server_host)
        port = cast(int, sys.settings.chroma_server_http_port)

    client = chromadb.HttpClient(
        host=host,
        port=port,
        headers={"x-tenant": "test_tenant"},
        tenant="test_tenant",
        database="test_database",
    )

    run_state_machine_as_test(
        lambda: EmbeddingStateMachine(client),
    )  # type: ignore
