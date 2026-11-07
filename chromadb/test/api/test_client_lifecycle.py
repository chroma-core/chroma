from typing import Any, Type, cast
from unittest.mock import patch

import pytest

import chromadb
from chromadb.api import ClientAPI
from chromadb.api.client import Client


def test_client_api_lifecycle() -> None:
    # Keep this variable annotated: lifecycle operations belong to the public API.
    with patch.object(Client, "__init__", return_value=None):
        client: ClientAPI = chromadb.PersistentClient()
    with patch.object(Client, "close") as close:
        client.close()
        with client as entered:
            assert entered is client
        assert close.call_count == 2


def test_factory_client_lifecycle() -> None:
    with patch.object(Client, "__init__", return_value=None):
        client = chromadb.PersistentClient()
        with patch.object(Client, "close") as close:
            client.close()
            with chromadb.PersistentClient() as entered:
                assert isinstance(entered, Client)
            assert close.call_count == 2


def _legacy_client() -> ClientAPI:
    def unimplemented(*args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    # Implement the pre-existing abstract API, without lifecycle methods.
    methods = {
        name: unimplemented
        for name in ClientAPI.__abstractmethods__
        if name not in {"close", "__enter__", "__exit__"}
    }
    legacy_type = cast(Type[ClientAPI], type("LegacyClient", (ClientAPI,), methods))
    return legacy_type()


def test_legacy_client_lifecycle_is_not_abstract() -> None:
    client = _legacy_client()
    assert client.__enter__() is client
    with pytest.raises(NotImplementedError, match="close"):
        client.close()
    with pytest.raises(NotImplementedError, match="close"):
        with client:
            pass


@pytest.mark.parametrize("raise_error", [False, True])
def test_client_api_context_manager_closes(raise_error: bool) -> None:
    client = _legacy_client()
    with patch.object(client, "close") as close:
        if raise_error:
            error = RuntimeError("operation failed")
            with pytest.raises(RuntimeError) as caught:
                with client as entered:
                    assert entered is client
                    raise error
            assert caught.value is error
        else:
            with client as entered:
                assert entered is client
        close.assert_called_once_with()
