import pytest

from chromadb.api import ServerAPI


def test_invalid_auth_cred(api_wrong_cred: ServerAPI) -> None:
    with pytest.raises(Exception) as e:
        api_wrong_cred.list_collections()
    assert "Forbidden" in str(e.value)


def test_server_basic_auth(api_with_server_auth: ServerAPI) -> None:
    cols = api_with_server_auth.list_collections()
    assert len(cols) == 0
