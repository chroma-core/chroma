import pytest


def test_invalid_auth_cred(api_wrong_cred):
    with pytest.raises(Exception) as e:
        api_wrong_cred.list_collections()
    assert "Unauthorized" in str(e.value)


def test_server_basic_auth(api_with_server_auth):
    cols = api_with_server_auth.list_collections()
    assert len(cols) == 0
