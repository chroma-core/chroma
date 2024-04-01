import json
import random
import string
from typing import Dict, Any, Tuple
import uuid
import hypothesis.strategies as st
import pytest
from hypothesis import given, settings
from chromadb import AdminClient

from chromadb.api import AdminAPI, ServerAPI
from chromadb.api.models.Collection import Collection
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.test.conftest import _fastapi_fixture


valid_action_space = [
    "tenant:create_tenant",
    "tenant:get_tenant",
    "db:create_database",
    "db:get_database",
    "db:reset",
    "db:list_collections",
    "collection:get_collection",
    "db:create_collection",
    "collection:delete_collection",
    "collection:update_collection",
    "collection:add",
    "collection:delete",
    "collection:get",
    "collection:query",
    "collection:peek",
    "collection:update",
    "collection:upsert",
    "collection:count",
]

role_name = st.text(alphabet=string.ascii_letters, min_size=1, max_size=20)

user_name = st.text(alphabet=string.ascii_letters, min_size=1, max_size=20)

actions = st.lists(
    st.sampled_from(valid_action_space), min_size=1, max_size=len(valid_action_space)
)


@st.composite
def master_user(draw: st.DrawFn) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return {
        "role": "__master_role__",
        "id": "__master__",
        "tenant": DEFAULT_TENANT,
        "tokens": [
            {
                "token": f"{random.randint(1,1000000)}_"
                + draw(
                    st.text(
                        alphabet=string.ascii_letters + string.digits,
                        min_size=1,
                        max_size=25,
                    )
                )
            }
            for _ in range(2)
        ],
    }, {
        "__master_role__": {
            "actions": valid_action_space,
            "unauthorized_actions": [],
        }
    }


@st.composite
def user_role_config(draw: st.DrawFn) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    role = draw(role_name)
    user = draw(user_name)
    actions_list = draw(actions)
    if any(
        action in actions_list
        for action in [
            "collection:add",
            "collection:delete",
            "collection:get",
            "collection:query",
            "collection:peek",
            "collection:update",
            "collection:upsert",
            "collection:count",
        ]
    ):
        actions_list.append("collection:get_collection")
    if any(
        action in actions_list
        for action in [
            "collection:peek",
        ]
    ):
        actions_list.append("collection:get")
    actions_list.extend(
        [
            "tenant:get_tenant",
            "db:get_database",
        ]
    )
    unauthorized_actions = set(valid_action_space) - set(actions_list)
    _role_config = {
        f"{role}": {
            "actions": actions_list,
            "unauthorized_actions": list(unauthorized_actions),
        }
    }

    return {
        "role": role,
        "id": user,
        "tenant": DEFAULT_TENANT,
        "tokens": [
            {
                "token": f"{random.randint(1,1000000)}_"
                + draw(
                    st.text(
                        alphabet=string.ascii_letters + string.digits,
                        min_size=1,
                        max_size=25,
                    )
                )
            }
            for _ in range(2)
        ],
    }, _role_config


@st.composite
def rbac_config(draw: st.DrawFn) -> Dict[str, Any]:
    user_roles = draw(
        st.lists(user_role_config().filter(lambda t: t[0]), min_size=1, max_size=10)
    )
    muser_role = draw(st.lists(master_user(), min_size=1, max_size=1))
    users = []
    roles = []
    for user, role in user_roles:
        users.append(user)
        roles.append(role)

    for muser, mrole in muser_role:
        users.append(muser)
        roles.append(mrole)
    roles_mapping = {}
    for role in roles:
        roles_mapping.update(role)
    _rbac_config = {
        "roles_mapping": roles_mapping,
        "users": users,
    }
    return _rbac_config


@st.composite
def token_config(draw: st.DrawFn) -> Dict[str, Any]:
    token_header = draw(st.sampled_from(["AUTHORIZATION", "X_CHROMA_TOKEN", None]))
    server_provider = draw(
        st.sampled_from(["token", "chromadb.auth.token.TokenAuthServerProvider"])
    )
    client_provider = draw(
        st.sampled_from(["token", "chromadb.auth.token.TokenAuthClientProvider"])
    )
    server_authz_provider = draw(
        st.sampled_from(["chromadb.auth.authz.SimpleRBACAuthorizationProvider"])
    )
    server_credentials_provider = draw(st.sampled_from(["user_token_config"]))
    # _rbac_config = draw(rbac_config())
    persistence = draw(st.booleans())
    return {
        "token_transport_header": token_header,
        "chroma_server_auth_credentials_file": None,
        "chroma_server_auth_provider": server_provider,
        "chroma_client_auth_provider": client_provider,
        "chroma_server_authz_config_file": None,
        "chroma_server_auth_credentials_provider": server_credentials_provider,
        "chroma_server_authz_provider": server_authz_provider,
        "is_persistent": persistence,
    }


api_executors = {
    "db:create_database": lambda api, mapi, aapi: (
        aapi.create_database(f"test-{uuid.uuid4()}")
    ),
    "db:get_database": lambda api, mapi, aapi: (aapi.get_database(DEFAULT_DATABASE),),
    "tenant:create_tenant": lambda api, mapi, aapi: (
        aapi.create_tenant(f"test-{uuid.uuid4()}")
    ),
    "tenant:get_tenant": lambda api, mapi, aapi: (aapi.get_tenant(DEFAULT_TENANT),),
    "db:reset": lambda api, mapi, _: api.reset(),
    "db:list_collections": lambda api, mapi, _: api.list_collections(),
    "collection:get_collection": lambda api, mapi, _: (
        # pre-condition
        mcol := mapi.create_collection(f"test-get-{uuid.uuid4()}"),
        api.get_collection(f"{mcol.name}"),
    ),
    "db:create_collection": lambda api, mapi, _: (
        api.create_collection(f"test-create-{uuid.uuid4()}"),
    ),
    "db:get_or_create_collection": lambda api, mapi, _: (
        api.get_or_create_collection(f"test-get-or-create-{uuid.uuid4()}")
    ),
    "collection:delete_collection": lambda api, mapi, _: (
        # pre-condition
        mcol := mapi.create_collection(f"test-delete-col-{uuid.uuid4()}"),
        api.delete_collection(f"{mcol.name}"),
    ),
    "collection:update_collection": lambda api, mapi, _: (
        # pre-condition
        mcol := mapi.create_collection(f"test-modify-col-{uuid.uuid4()}"),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.modify(metadata={"test": "test"}),
    ),
    "collection:add": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-add-doc-{uuid.uuid4()}"),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.add(documents=["test"], ids=["1"]),
    ),
    "collection:delete": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-delete-doc-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(client=api, name=f"{mcol.name}", id=mcol.id),
        col.delete(ids=["1"]),
    ),
    "collection:get": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-get-doc-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.get(ids=["1"]),
    ),
    "collection:query": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-query-doc-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.query(query_texts=["test"]),
    ),
    "collection:peek": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-peek-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.peek(),
    ),
    "collection:update": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-update-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.update(ids=["1"], documents=["test1"]),
    ),
    "collection:upsert": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-upsert-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.upsert(ids=["1"], documents=["test1"]),
    ),
    "collection:count": lambda api, mapi, _: (
        mcol := mapi.create_collection(f"test-count-{uuid.uuid4()}"),
        mcol.add(documents=["test"], ids=["1"]),
        col := Collection(api, f"{mcol.name}", mcol.id),
        col.count(),
    ),
}


def master_api(_settings: Settings) -> Tuple[ServerAPI, AdminAPI]:
    system = System(_settings)
    api = system.instance(ServerAPI)
    admin_api = AdminClient(api.get_settings())
    system.start()
    return api, admin_api


@settings(max_examples=10)
@given(token_config=token_config(), rbac_config=rbac_config())
def test_authz(token_config: Dict[str, Any], rbac_config: Dict[str, Any]) -> None:
    authz_config = rbac_config
    token_config["chroma_server_authz_config"] = rbac_config
    token_config["chroma_server_auth_credentials"] = json.dumps(rbac_config["users"])
    random_user = random.choice(
        [user for user in authz_config["users"] if user["id"] != "__master__"]
    )
    _master_user = [
        user for user in authz_config["users"] if user["id"] == "__master__"
    ][0]
    random_token = random.choice(random_user["tokens"])["token"]
    api = _fastapi_fixture(
        is_persistent=token_config["is_persistent"],
        chroma_server_auth_provider=token_config["chroma_server_auth_provider"],
        chroma_server_auth_credentials_provider=token_config[
            "chroma_server_auth_credentials_provider"
        ],
        chroma_server_auth_credentials=token_config["chroma_server_auth_credentials"],
        chroma_client_auth_provider=token_config["chroma_client_auth_provider"],
        chroma_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_server_authz_provider=token_config["chroma_server_authz_provider"],
        chroma_server_authz_config=token_config["chroma_server_authz_config"],
        chroma_client_auth_credentials=random_token,
    )
    _sys: System = next(api)
    _sys.reset_state()
    _master_settings = Settings(**dict(_sys.settings))
    _master_settings.chroma_client_auth_credentials = _master_user["tokens"][0]["token"]
    _master_api, admin_api = master_api(_master_settings)
    _api = _sys.instance(ServerAPI)
    _api.heartbeat()
    for action in authz_config["roles_mapping"][random_user["role"]]["actions"]:
        api_executors[action](_api, _master_api, admin_api)  # type: ignore
    for unauthorized_action in authz_config["roles_mapping"][random_user["role"]][
        "unauthorized_actions"
    ]:
        with pytest.raises(Exception) as ex:
            api_executors[unauthorized_action](
                _api, _master_api, admin_api
            )  # type: ignore
            assert "Unauthorized" in str(ex) or "Forbidden" in str(ex)
