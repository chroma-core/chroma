import hypothesis.strategies as st
import tempfile
from typing import Any, Dict
import yaml

import string


# valid_action_space = [
#     "tenant:create_tenant",
#     "tenant:get_tenant",
#     "db:create_database",
#     "db:get_database",
#     "db:reset",
#     "db:list_collections",
#     "collection:get_collection",
#     "db:create_collection",
#     "collection:delete_collection",
#     "collection:update_collection",
#     "collection:add",
#     "collection:delete",
#     "collection:get",
#     "collection:query",
#     "collection:peek",
#     "collection:update",
#     "collection:upsert",
#     "collection:count",
# ]


@st.composite
def random_token(draw: st.DrawFn) -> str:
    return draw(
        st.text(
            alphabet=string.ascii_letters + string.digits,
            min_size=1,
            max_size=50
        )
    )


@st.composite
def random_token_transport_header(draw: st.DrawFn) -> str | None:
    return draw(
        st.sampled_from(
            [
                "AUTHORIZATION",
                "X_CHROMA_TOKEN",
                None
            ]
        )
    )


@st.composite
def random_user_name(draw: st.DrawFn) -> str:
    return draw(
        st.text(
            alphabet=string.ascii_letters,
            min_size=1,
            max_size=20
        )
    )


@st.composite
def users_with_tokens_and_valid_user(draw: st.DrawFn) -> Dict[str, Any]:
    users = draw(
        st.lists(
            st.fixed_dictionaries(
                {
                    "id": random_user_name(),
                    "tokens": st.lists(
                        random_token(),
                        min_size=1,
                        max_size=10
                    )
                }
            ),
            min_size=1,
            max_size=10
        )
    )
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open(tmp.name, "w") as f:
        yaml.dump({"users": users}, f)
    return {
        "users": users,
        "filename": tmp.name
    }


# @st.composite
# def random_role_name(draw: st.DrawFn) -> str:
#     return draw(
#         st.text(
#             alphabet=string.ascii_letters,
#             min_size=1,
#             max_size=20
#         )
#     )


# @st.composite
# def random_action(draw: st.DrawFn) -> str:
#     return draw(
#         st.sampled_from(valid_action_space)
#     )


# @st.composite
# def master_user(draw: st.DrawFn) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#     return {
#         "role": "__master_role__",
#         "id": "__master__",
#         "tenant": "*",
#         "tokens": [
#             {
#                 "token": random_token()
#             }
#         ],
#     }, {
#         "__master_role__": {
#             "actions": valid_action_space,
#         }
#     }


# @st.composite
# def user_role_config(draw: st.DrawFn) -> Tuple[Dict[str, Any], Dict[str, Any]]:
#     role = random_role_name()
#     user = random_user_name()
#     actions_list = draw(actions)
#     if any(
#         action in actions_list
#         for action in [
#             "collection:add",
#             "collection:delete",
#             "collection:get",
#             "collection:query",
#             "collection:peek",
#             "collection:update",
#             "collection:upsert",
#             "collection:count",
#         ]
#     ):
#         actions_list.append("collection:get_collection")
#     if any(
#         action in actions_list
#         for action in [
#             "collection:peek",
#         ]
#     ):
#         actions_list.append("collection:get")
#     actions_list.extend(
#         [
#             "tenant:get_tenant",
#             "db:get_database",
#         ]
#     )
#     unauthorized_actions = set(valid_action_space) - set(actions_list)
#     _role_config = {
#         f"{role}": {
#             "actions": actions_list,
#             "unauthorized_actions": list(unauthorized_actions),
#         }
#     }

#     return {
#         "role": role,
#         "id": user,
#         "tenant": DEFAULT_TENANT,
#         "tokens": [
#             {
#                 "token": f"{random.randint(1,1000000)}_"
#                 + draw(
#                     st.text(
#                         alphabet=string.ascii_letters + string.digits,
#                         min_size=1,
#                         max_size=25,
#                     )
#                 )
#             }
#             for _ in range(2)
#         ],
#     }, _role_config


# @st.composite
# def rbac_config(draw: st.DrawFn) -> Dict[str, Any]:
#     user_roles = draw(
#         st.lists(user_role_config().filter(lambda t: t[0]), min_size=1, max_size=10)
#     )
#     muser_role = draw(st.lists(master_user(), min_size=1, max_size=1))
#     users = []
#     roles = []
#     for user, role in user_roles:
#         users.append(user)
#         roles.append(role)

#     for muser, mrole in muser_role:
#         users.append(muser)
#         roles.append(mrole)
#     roles_mapping = {}
#     for role in roles:
#         roles_mapping.update(role)
#     _rbac_config = {
#         "roles_mapping": roles_mapping,
#         "users": users,
#     }
#     return _rbac_config


# @st.composite
# def token_config(
#     draw: st.DrawFn
# ) -> Tuple[bool, str, str, str]:
#     # Returns persistence, token_header, client token, and a filepath
#     # to a TokenAuthenticationServerProvider config.
#     persistence = draw(st.booleans())

#     token_header = draw(
#         st.sampled_from([
#             "AUTHORIZATION",
#             "X_CHROMA_TOKEN",
#             None
#         ])
#     )

#     token = random_token()

#     # Get a temp file to write config to
#     temp = tempfile.NamedTemporaryFile(delete=False)
