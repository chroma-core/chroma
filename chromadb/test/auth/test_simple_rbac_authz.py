from hypothesis import given, Phase, settings
import hypothesis.strategies as st
import pytest
from typing import Any, Dict, Optional

from chromadb.api import ServerAPI
from chromadb.config import Settings, System
from chromadb.test.auth.rbac_test_executors import api_executors
from chromadb.test.auth.strategies import (
    random_token_transport_header,
    rbac_test_conf,
    unauthorized_actions,
)
from chromadb.test.conftest import _fastapi_fixture


def test_basic_authn_rbac_authz_unit_test(api_with_authn_rbac_authz: ServerAPI) -> None:
    api_with_authn_rbac_authz.reset()
    api_with_authn_rbac_authz.create_collection("test_collection")


@settings(max_examples=10, phases=[Phase.generate, Phase.target], deadline=None)
@given(rbac_test_conf(), st.booleans(), random_token_transport_header(), st.data())
def test_token_authn_rbac_authz(
    rbac_conf: Dict[str, Any], persistence: bool, header: Optional[str], data: Any
) -> None:
    for user in rbac_conf["users"]:
        if user["id"] == "__root__":
            break

        token_index = data.draw(
            st.integers(min_value=0, max_value=len(user["tokens"]) - 1)
        )
        token = user["tokens"][token_index]

        api_fixture = _fastapi_fixture(
            is_persistent=persistence,
            chroma_auth_token_transport_header=header,
            chroma_client_auth_credentials=token,
            chroma_client_auth_provider="chromadb.auth."
            "token_authn.TokenAuthClientProvider",
            chroma_server_authn_provider="chromadb.auth.token_authn."
            "TokenAuthenticationServerProvider",
            chroma_server_authn_credentials_file=rbac_conf["filename"],
            chroma_server_authz_provider="chromadb.auth.simple_rbac_authz."
            "SimpleRBACAuthorizationProvider",
            chroma_server_authz_config_file=rbac_conf["filename"],
        )
        sys: System = next(api_fixture)
        sys.reset_state()
        api = sys.instance(ServerAPI)
        api.heartbeat()

        root_settings = Settings(**dict(sys.settings))
        root_users = [user for user in rbac_conf["users"] if user["id"] == "__root__"]
        assert len(root_users) == 1
        root_user = root_users[0]
        root_settings.chroma_client_auth_credentials = root_user["tokens"][0]
        system = System(root_settings)
        root_api = system.instance(ServerAPI)
        system.start()

        role_matches = [r for r in rbac_conf["roles"] if r["id"] == user["role"]]
        assert len(role_matches) == 1
        role = role_matches[0]

        for action in role["actions"]:
            api_executors[action](
                api,
                root_api,
                data.draw,
            )
            root_api.reset()

        for unauthorized_action in unauthorized_actions(role["actions"]):
            with pytest.raises(Exception) as ex:
                api_executors[unauthorized_action](
                    api,
                    root_api,
                    data.draw,
                )
                assert "Unauthorized" in str(ex) or "Forbidden" in str(ex)
