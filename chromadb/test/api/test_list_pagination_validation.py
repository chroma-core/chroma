import chromadb
import pytest

from chromadb.api.client import AdminClient

U32_MAX = 2**32 - 1


@pytest.fixture
def client():
    return chromadb.EphemeralClient()


@pytest.fixture
def admin_client(client):
    return AdminClient.from_system(client._system)


@pytest.mark.parametrize("field", ["limit", "offset"])
@pytest.mark.parametrize("value", [-1, U32_MAX + 1])
def test_list_collections_rejects_out_of_range_pagination(client, field, value):
    """Out-of-range pagination must not reach the u32 Rust bindings."""
    with pytest.raises(ValueError):
        client.list_collections(**{field: value})


@pytest.mark.parametrize("field", ["limit", "offset"])
@pytest.mark.parametrize("value", [-1, U32_MAX + 1])
def test_list_databases_rejects_out_of_range_pagination(admin_client, field, value):
    with pytest.raises(ValueError):
        admin_client.list_databases(**{field: value})


def test_list_collections_accepts_valid_pagination(client):
    assert list(client.list_collections(limit=0, offset=0)) == []
    assert list(client.list_collections()) == []


def test_list_databases_accepts_valid_pagination(admin_client):
    assert len(admin_client.list_databases(limit=1, offset=0)) == 1
