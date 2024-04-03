import hypothesis.strategies as st

from hypothesis import given, settings
from overrides import override
from starlette.datastructures import Headers
from typing import Dict, List, Tuple

from chromadb.api import ServerAPI
from chromadb.auth import (
    UserIdentity,
    ServerAuthenticationProvider
)


class DummyServerAuthenticationProvider(ServerAuthenticationProvider):
    """
    We want to test functionality on the base class of
    ServerAuthenticationProvider but it has an abstract method, so we need
    to implement it here.
    """
    @override
    def authenticate(self, headers: Headers) -> UserIdentity:
        return UserIdentity(user_id="test_user")


@st.composite
def paths_config(
    draw: st.DrawFn
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    pass
    ignore_path = draw(st.sampled_from([
        "/api/v1/heartbeat",
        "/api/v1/reset",
        "/api/v1/version",
        "/api/v1/databases",
        "/api/v1/tenants",
        "/api/v1/collections",
        "/api/v1/count_collections",
        "/api/v1/collections",
    ]))
    # All these endpoints only accept GETs
    methods_to_ignore = draw(st.lists(
        st.sampled_from(["GET", "POST", "PUT", "DELETE"]),
        min_size=1,
        max_size=4
    ))

    paths_to_get = draw(st.lists(st.text(), min_size=1, max_size=10))
    methods_to_get = draw(st.lists(
        st.sampled_from(["GET", "POST", "PUT", "DELETE"]),
        min_size=1,
        max_size=4
    ))

    return {
        ignore_path: methods_to_ignore
    }, {
        path: methods_to_get
        for path in paths_to_get
    }


@settings(max_examples=100)
@given(paths_config())
def test_ignore_paths(
    api: ServerAPI,
    paths_config: Tuple[Dict[str, List[str]], Dict[str, List[str]]]
) -> None:
    (ignore_paths, get_paths) = paths_config
    api._system.settings.chroma_server_auth_ignore_paths = ignore_paths
    server_authn_provider = DummyServerAuthenticationProvider(api._system)
    for path, methods in ignore_paths.items():
        for method in methods:
            assert server_authn_provider.ignore_operation(method, path)
    for path, methods in get_paths.items():
        for method in methods:
            assert not server_authn_provider.ignore_operation(method, path)
