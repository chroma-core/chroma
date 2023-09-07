import string
from typing import Dict, Any

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

from chromadb.api import API
from chromadb.config import System
from chromadb.test.conftest import _fastapi_fixture


@st.composite
def token_config(draw: st.DrawFn) -> Dict[str, Any]:
    token_header = draw(st.sampled_from(["AUTHORIZATION", "X_CHROMA_TOKEN", None]))
    server_provider = draw(
        st.sampled_from(["token", "chromadb.auth.token.TokenAuthServerProvider"])
    )
    client_provider = draw(
        st.sampled_from(["token", "chromadb.auth.token.TokenAuthClientProvider"])
    )
    server_credentials_provider = draw(
        st.sampled_from(
            ["chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"]
        )
    )
    token = draw(
        st.text(
            alphabet=string.digits + string.ascii_letters + string.punctuation,
            min_size=1,
            max_size=50,
        )
    )
    persistence = draw(st.booleans())
    return {
        "token_transport_header": token_header,
        "chroma_server_auth_credentials": token,
        "chroma_client_auth_credentials": token,
        "chroma_server_auth_provider": server_provider,
        "chroma_client_auth_provider": client_provider,
        "chroma_server_auth_credentials_provider": server_credentials_provider,
        "is_persistent": persistence,
    }


@settings(max_examples=10)
@given(token_config())
def test_fastapi_server_token_auth(token_config: Dict[str, Any]) -> None:
    api = _fastapi_fixture(
        is_persistent=token_config["is_persistent"],
        chroma_server_auth_provider=token_config["chroma_server_auth_provider"],
        chroma_server_auth_credentials_provider=token_config[
            "chroma_server_auth_credentials_provider"
        ],
        chroma_server_auth_credentials=token_config["chroma_server_auth_credentials"],
        chroma_client_auth_provider=token_config["chroma_client_auth_provider"],
        chroma_client_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_server_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_client_auth_credentials=token_config["chroma_client_auth_credentials"],
    )
    _sys: System = next(api)
    _sys.reset_state()
    _api = _sys.instance(API)
    _api.heartbeat()
    assert _api.list_collections() == []


@st.composite
def random_token(draw: st.DrawFn) -> str:
    return draw(
        st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=5)
    )


@st.composite
def invalid_token(draw: st.DrawFn) -> str:
    opposite_alphabet = set(string.printable) - set(
        string.digits + string.ascii_letters + string.punctuation
    )
    token = draw(st.text(alphabet=list(opposite_alphabet), min_size=1, max_size=50))
    return token


@settings(max_examples=10)
@given(tconf=token_config(), inval_tok=invalid_token())
def test_invalid_token(tconf: Dict[str, Any], inval_tok: str) -> None:
    api = _fastapi_fixture(
        is_persistent=tconf["is_persistent"],
        chroma_server_auth_provider=tconf["chroma_server_auth_provider"],
        chroma_server_auth_credentials_provider=tconf[
            "chroma_server_auth_credentials_provider"
        ],
        chroma_server_auth_credentials=tconf["chroma_server_auth_credentials"],
        chroma_server_auth_token_transport_header=tconf["token_transport_header"],
        chroma_client_auth_provider=tconf["chroma_client_auth_provider"],
        chroma_client_auth_token_transport_header=tconf["token_transport_header"],
        chroma_client_auth_credentials=inval_tok,
    )
    with pytest.raises(Exception) as e:
        _sys: System = next(api)
        _sys.reset_state()
        _sys.instance(API)
    assert "Invalid token" in str(e)


@settings(max_examples=10)
@given(token_config(), random_token())
def test_fastapi_server_token_auth_wrong_token(
    token_config: Dict[str, Any], random_token: str
) -> None:
    api = _fastapi_fixture(
        is_persistent=token_config["is_persistent"],
        chroma_server_auth_provider=token_config["chroma_server_auth_provider"],
        chroma_server_auth_credentials_provider=token_config[
            "chroma_server_auth_credentials_provider"
        ],
        chroma_server_auth_credentials=token_config["chroma_server_auth_credentials"],
        chroma_server_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_client_auth_provider=token_config["chroma_client_auth_provider"],
        chroma_client_auth_token_transport_header=token_config[
            "token_transport_header"
        ],
        chroma_client_auth_credentials=token_config["chroma_client_auth_credentials"]
        + random_token,
    )
    _sys: System = next(api)
    _sys.reset_state()
    _api = _sys.instance(API)
    _api.heartbeat()
    with pytest.raises(Exception) as e:
        _api.list_collections()
    assert "Unauthorized" in str(e)
