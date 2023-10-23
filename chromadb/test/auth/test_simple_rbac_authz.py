import os
import random
import shutil
import string
from typing import IO, Dict, Any, Generator
import uuid
import hypothesis.strategies as st
import pytest
from hypothesis import given
import tempfile

import yaml
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.config import System
from chromadb.test.conftest import _fastapi_fixture


valid_action_space = [
    "db:reset",
    "db:list_collections",
    "db:get_collection",
    "db:create_collection",
    "db:get_or_create_collection",
    "db:delete_collection",
    "db:update_collection",
    "collection:add",
    "collection:delete",
    "collection:get",
    "collection:query",
    "collection:peek",
    "collection:update",
    "collection:upsert",
    "collection:count",
]


@st.composite
def random_token(draw: st.DrawFn) -> str:
    return draw(
        st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=25)
    )


@st.composite
def generate_uniq_token(draw: st.DrawFn) -> str:
    return str(st.uuids())


@st.composite
def invalid_token(draw: st.DrawFn) -> str:
    opposite_alphabet = set(string.printable) - set(
        string.digits + string.ascii_letters + string.punctuation
    )
    token = draw(st.text(alphabet=list(opposite_alphabet), min_size=1, max_size=50))
    return token


role_name = st.text(alphabet=string.ascii_letters, min_size=1, max_size=20)

user_name = st.text(alphabet=string.ascii_letters, min_size=1, max_size=20)

actions = st.lists(
    st.sampled_from(valid_action_space), min_size=1, max_size=len(valid_action_space)
)


unique_tokens = st.lists(
    st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=25),
    unique=True,
)


@st.composite
def user_role_config(draw: st.DrawFn) -> tuple[Dict[str, Any], Dict[str, Any]]:
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
        actions_list.append("db:get_collection")
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


@pytest.fixture(scope="function")
def get_temp_file() -> Generator[IO[bytes], None, None]:
    with tempfile.NamedTemporaryFile(delete=False) as f:
        yield f
    if os.path.exists(f.name):
        shutil.rmtree(f.name)


@st.composite
def rbac_config(draw: st.DrawFn) -> Dict[str, Any]:
    user_roles = draw(
        st.lists(user_role_config().filter(lambda t: t[0]), min_size=1, max_size=10)
    )
    users = []
    roles = []
    for user, role in user_roles:
        users.append(user)
        roles.append(role)
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
    "db:reset": lambda api: api.reset(),
    "db:list_collections": lambda api: api.list_collections(),
    "db:get_collection": lambda api: (api.get_collection(f"test-get-{uuid.uuid4()}")),
    "db:create_collection": lambda api: (
        api.create_collection(f"test-create-{uuid.uuid4()}")
    ),
    "db:get_or_create_collection": lambda api: (
        api.get_or_create_collection(f"test-get-or-create-{uuid.uuid4()}")
    ),
    "db:delete_collection": lambda api: (
        api.delete_collection(f"test-delete-col-{uuid.uuid4()}")
    ),
    "db:update_collection": lambda api: (
        col := Collection(api, f"test-update-{uuid.uuid4()}", uuid.uuid4()),
        col.modify(metadata={"test": "test"}),  # type: ignore
    ),
    "collection:add": lambda api: (
        col := Collection(api, f"test-add-doc-{uuid.uuid4()}", uuid.uuid4()),
        col.add(documents=["test"], ids=["1"]),  # type: ignore
    ),
    "collection:delete": lambda api: (
        col := Collection(api, f"test-delete-doc-{uuid.uuid4()}", uuid.uuid4()),
        col.delete(ids=["1"]),  # type: ignore
    ),
    "collection:get": lambda api: (
        col := Collection(api, f"test-get-docs-{uuid.uuid4()}", uuid.uuid4()),
        col.get(ids=["1"]),
    ),
    "collection:query": lambda api: (
        col := Collection(api, f"test-query-{uuid.uuid4()}", uuid.uuid4()),
        col.query(query_texts=["test"]),
    ),
    "collection:peek": lambda api: (
        col := Collection(api, f"test-peek-{uuid.uuid4()}", uuid.uuid4()),
        col.peek(),
    ),
    "collection:update": lambda api: (
        col := Collection(api, f"test-update-docs-{uuid.uuid4()}", uuid.uuid4()),
        col.update(ids=["1"], documents=["test1"]),  # type: ignore
    ),
    "collection:upsert": lambda api: (
        col := Collection(api, f"test-upsert-{uuid.uuid4()}", uuid.uuid4()),
        col.upsert(ids=["1"], documents=["test1"]),  # type: ignore
    ),
    "collection:count": lambda api: (
        col := Collection(api, f"test-count-{uuid.uuid4()}", uuid.uuid4()),
        col.count(),
    ),
}


@given(token_config=token_config(), rbac_config=rbac_config())
def test_authz(
    token_config: Dict[str, Any], rbac_config: Dict[str, Any], get_temp_file: IO[bytes]
) -> None:
    authz_config = rbac_config
    token_config["chroma_server_authz_config_file"] = get_temp_file.name
    token_config["chroma_server_auth_credentials_file"] = get_temp_file.name
    with open(get_temp_file.name, "w") as f:
        yaml.dump(authz_config, f)
    random_user = random.choice(authz_config["users"])
    random_token = random.choice(random_user["tokens"])["token"]
    api = _fastapi_fixture(
        is_persistent=token_config["is_persistent"],
        chroma_server_auth_provider=token_config["chroma_server_auth_provider"],
        chroma_server_auth_credentials_provider=token_config[
            "chroma_server_auth_credentials_provider"
        ],
        chroma_server_auth_credentials_file=token_config[
            "chroma_server_auth_credentials_file"
        ],
        chroma_client_auth_provider=token_config["chroma_client_auth_provider"],
        chroma_client_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_server_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_server_authz_provider=token_config["chroma_server_authz_provider"],
        chroma_server_authz_config_file=token_config["chroma_server_authz_config_file"],
        chroma_client_auth_credentials=random_token,
    )
    _sys: System = next(api)
    _sys.reset_state()
    _api = _sys.instance(API)
    _api.heartbeat()
    for actions in authz_config["roles_mapping"][random_user["role"]]["actions"]:
        try:
            # print(actions, get_temp_file.name)
            api_executors[actions](_api)  # type: ignore
        except Exception as e:
            # we want to ignore errors such as collection not found or 400 client error
            # cause by the lack of data on the server side. The reason is that Authz
            # makes sense in client/server mode but we don't yet have means to provision
            # the server as part of the tests.
            assert (
                "not exist" in str(e)
                or (
                    "400 Client Error" in str(e)
                    and actions in ["collection:upsert", "collection:update"]
                )
                or "StopIteration" in str(e)
            )
    for unauthorized_action in authz_config["roles_mapping"][random_user["role"]][
        "unauthorized_actions"
    ]:
        with pytest.raises(Exception) as ex:
            # print(unauthorized_action)
            api_executors[unauthorized_action](_api)  # type: ignore
            assert "Unauthorized" in str(ex) or "Forbidden" in str(ex)
