from typing import Dict
from fastapi import HTTPException
from overrides import override
from chromadb.auth import (
    AuthzAction,
    AuthzResource,
    ServerAuthenticationProvider,
    ServerAuthorizationProvider,
    UserIdentity,
)
from chromadb.config import System
from chromadb.test.conftest import ClientFactories
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


def test_tenant_and_database_passed_from_client(
    client_factories: ClientFactories,
) -> None:
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()
    admin_client.create_tenant("test_tenant")
    admin_client.create_database("test_database", "test_tenant")

    client.set_tenant(tenant="test_tenant", database="test_database")

    run_state_machine_as_test(
        lambda: EmbeddingStateMachine(client),
    )  # type: ignore
