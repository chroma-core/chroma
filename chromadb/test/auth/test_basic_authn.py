import asyncio
from typing import Union
from chromadb.test.auth.strategies import (
    list_col_async,
)
import pytest

from chromadb.api import ServerAPI, AsyncServerAPI


def test_invalid_auth_cred(api_wrong_cred: Union[AsyncServerAPI, ServerAPI]) -> None:
    with pytest.raises(Exception) as e:
        if (
            api_wrong_cred.get_settings().chroma_api_impl
            == "chromadb.api.async_fastapi.AsyncFastAPI"
        ):
            asyncio.get_event_loop().run_until_complete(list_col_async(api_wrong_cred))
        else:
            api_wrong_cred.list_collections()
    assert "Forbidden" in str(e.value)


def test_server_basic_auth(
    api_with_server_auth: Union[AsyncServerAPI, ServerAPI]
) -> None:
    if (
        api_with_server_auth.get_settings().chroma_api_impl
        == "chromadb.api.async_fastapi.AsyncFastAPI"
    ):
        cols = asyncio.get_event_loop().run_until_complete(
            list_col_async(api_with_server_auth)
        )
    else:
        cols = api_with_server_auth.list_collections()
    assert len(cols) == 0
