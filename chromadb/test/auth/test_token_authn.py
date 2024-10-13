import asyncio

from hypothesis import given, settings
from typing import Any, Dict

import hypothesis.strategies as st
import pytest

from chromadb.api import ServerAPI
from chromadb.config import System
from chromadb.test.conftest import _fastapi_fixture
from chromadb.test.auth.strategies import (
    random_token,
    random_token_transport_header,
    token_test_conf,
    list_col_async,
)


@settings(max_examples=10)
@given(
    token_test_conf(),
    random_token_transport_header(),
    st.booleans(),
    st.sampled_from(
        ["chromadb.api.async_fastapi.AsyncFastAPI", "chromadb.api.fastapi.FastAPI"]
    ),
)
def test_fastapi_server_token_authn_allows_when_it_should_allow(
    tconf: Dict[str, Any], transport_header: str, persistence: bool, api_impl: str
) -> None:
    for user in tconf["users"]:
        for token in user["tokens"]:
            api = _fastapi_fixture(
                is_persistent=persistence,
                chroma_auth_token_transport_header=transport_header,
                chroma_server_authn_provider="chromadb.auth.token_authn.TokenAuthenticationServerProvider",
                chroma_server_authn_credentials_file=tconf["filename"],
                chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
                chroma_client_auth_credentials=token,
                chroma_api_impl=api_impl,
            )
            _sys: System = next(api)
            _sys.reset_state()
            _api = _sys.instance(ServerAPI)
            _api.heartbeat()
            if api_impl == "chromadb.api.async_fastapi.AsyncFastAPI":
                cols = asyncio.get_event_loop().run_until_complete(list_col_async(_api))  # type: ignore
            else:
                cols = _api.list_collections()
            assert cols == []


@settings(max_examples=10)
@given(
    token_test_conf(),
    random_token(),
    random_token_transport_header(),
    st.booleans(),
    st.sampled_from(
        ["chromadb.api.async_fastapi.AsyncFastAPI", "chromadb.api.fastapi.FastAPI"]
    ),
)
def test_fastapi_server_token_authn_rejects_when_it_should_reject(
    tconf: Dict[str, Any],
    unauthorized_token: str,
    transport_header: str,
    persistence: bool,
    api_impl: str,
) -> None:
    # Make sure we actually have an unauthorized token
    for user in tconf["users"]:
        for t in user["tokens"]:
            if t == unauthorized_token:
                return

    for user in tconf["users"]:
        for t in user["tokens"]:
            _api = _fastapi_fixture(
                is_persistent=persistence,
                chroma_auth_token_transport_header=transport_header,
                chroma_server_authn_provider="chromadb.auth.token_authn.TokenAuthenticationServerProvider",
                chroma_server_authn_credentials_file=tconf["filename"],
                chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
                chroma_client_auth_credentials=unauthorized_token,
                chroma_api_impl=api_impl,
            )
            _sys: System = next(_api)
            _sys.reset_state()
            api = _sys.instance(ServerAPI)
            api.heartbeat()
            with pytest.raises(Exception) as e:
                if api_impl == "chromadb.api.async_fastapi.AsyncFastAPI":
                    asyncio.get_event_loop().run_until_complete(list_col_async(api))  # type: ignore
                else:
                    api.list_collections()

            assert "Forbidden" in str(e)
