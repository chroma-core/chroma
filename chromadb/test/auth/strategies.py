import hypothesis.strategies as st
import tempfile
from typing import Any, Dict, List, Optional
import yaml

import string

from chromadb import TokenTransportHeader
from chromadb.test.property.strategies import collection_name


@st.composite
def many_unique_names(draw: st.DrawFn) -> List[str]:
    # This is ugly -- is there a better way to guarantee ("guarantee")
    # that we have enough unique names?

    # 10 tests, <= 10 users, 2 iterations through the space, <= 2 names per action
    length = len(valid_action_space) * 10 * 10 * 2 * 2
    names = draw(
        st.lists(
            collection_name(),
            min_size=length,
            max_size=length,
        )
    )
    seen = set()
    unseen = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        unseen.append(name)
    return unseen


@st.composite
def random_token(draw: st.DrawFn) -> str:
    return draw(
        st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=50)
    )


@st.composite
def random_token_transport_header(draw: st.DrawFn) -> Optional[str]:
    return draw(
        st.sampled_from(
            [
                TokenTransportHeader.AUTHORIZATION,
                TokenTransportHeader.X_CHROMA_TOKEN,
                None,
            ]
        )
    )


@st.composite
def random_user_name(draw: st.DrawFn) -> str:
    return draw(st.text(alphabet=string.ascii_letters, min_size=1, max_size=20))


@st.composite
def random_users_with_tokens(draw: st.DrawFn) -> List[Dict[str, Any]]:
    users = draw(
        st.lists(
            st.fixed_dictionaries(
                {
                    "id": random_user_name(),
                    "tokens": st.lists(random_token(), min_size=3, max_size=10),
                }
            ),
            min_size=1,
            max_size=1,
        )
    )
    unseen_users = []
    seen_users = set()
    seen_tokens = set()
    for user in users:
        if user["id"] in seen_users:
            break
        for token in user["tokens"]:
            if token in seen_tokens:
                break
    else:
        seen_users.add(user["id"])
        for token in user["tokens"]:
            seen_tokens.add(token)

        unseen_users.append(user)
    return unseen_users


@st.composite
def token_test_conf(draw: st.DrawFn) -> Dict[str, Any]:
    users = draw(random_users_with_tokens())
    filename = _dump_to_tmpfile({"users": users})
    return {"users": users, "filename": filename}


valid_action_space = [
    "system:reset",
    "tenant:create_tenant",
    "tenant:get_tenant",
    "db:create_database",
    "db:get_database",
    "db:list_collections",
    "db:create_collection",
    "db:get_or_create_collection",
    "collection:get_collection",
    "collection:delete_collection",
    "collection:update_collection",
    "collection:add",
    "collection:delete",
    "collection:get",
    "collection:query",
    "collection:peek",
    "collection:count",
    "collection:update",
    "collection:upsert",
]


def unauthorized_actions(authorized_actions: List[str]) -> List[str]:
    return [action for action in valid_action_space if action not in authorized_actions]


@st.composite
def random_role_name(draw: st.DrawFn) -> str:
    return draw(st.text(alphabet=string.ascii_letters, min_size=1, max_size=20))


@st.composite
def random_action(draw: st.DrawFn) -> str:
    return draw(st.sampled_from(valid_action_space))


@st.composite
def random_allowed_actions_for_role(draw: st.DrawFn) -> List[str]:
    actions = draw(st.sets(random_action(), min_size=1, max_size=10))

    if any(
        action in actions
        for action in [
            "collection:add",
            "collection:delete",
            "collection:get",
            "collection:query",
            "collection:peek",
            "collection:update",
            "collection:upsert",
            "collection:count",
            "collection:update_collection",
            "collection:delete_collection",
        ]
    ):
        actions.add("collection:get_collection")

    if "collection:peek" in actions:
        actions.add("collection:get")

    actions.add("tenant:get_tenant")
    actions.add("db:get_database")
    return list(actions)


@st.composite
def random_roles(draw: st.DrawFn) -> List[Dict[str, Any]]:
    roles = draw(
        st.lists(
            st.fixed_dictionaries(
                {"id": random_role_name(), "actions": random_allowed_actions_for_role()}
            ),
            min_size=1,
            max_size=10,
        ),
    )
    unseen_roles = []
    seen = set()
    for role in roles:
        if role["id"] in seen:
            continue
        seen.add(role["id"])
        unseen_roles.append(role)
    return unseen_roles


def _transform_roles_for_flush(roles: List[Dict[str, Any]]) -> Dict[str, Any]:
    roles_mapping = {}
    for role in roles:
        roles_mapping.update({role["id"]: {"actions": role["actions"]}})
    return roles_mapping


@st.composite
def random_users_and_roles(draw: st.DrawFn) -> Dict[str, Any]:
    users = draw(random_users_with_tokens())
    roles = draw(random_roles())
    for user in users:
        role_index = draw(st.integers(min_value=0, max_value=len(roles) - 1))
        user["role"] = roles[role_index]["id"]
    return {"users": users, "roles": roles}


def _root_user_and_role() -> Dict[str, Any]:
    return {
        "users": [{"id": "__root__", "tokens": ["__root__"], "role": "__root__"}],
        "roles": [{"id": "__root__", "actions": valid_action_space}],
    }


@st.composite
def rbac_test_conf(draw: st.DrawFn) -> Dict[str, Any]:
    users_and_roles = draw(random_users_and_roles())
    root_user_and_role = _root_user_and_role()
    users_and_roles["users"].extend(root_user_and_role["users"])
    users_and_roles["roles"].extend(root_user_and_role["roles"])

    filename = _dump_to_tmpfile(
        {
            "users": users_and_roles["users"],
            "roles_mapping": _transform_roles_for_flush(users_and_roles["roles"]),
        }
    )
    return {
        "users": users_and_roles["users"],
        "roles": users_and_roles["roles"],
        "filename": filename,
    }


def _dump_to_tmpfile(data: Any) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open(tmp.name, "w") as f:
        yaml.dump(data, f)
    return tmp.name
