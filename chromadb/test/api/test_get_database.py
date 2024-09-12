import pytest
from chromadb.errors import NotFoundError
from chromadb.test.conftest import ClientFactories


def test_get_database_not_found(client_factories: ClientFactories) -> None:
    with pytest.raises(NotFoundError) as e_info:
        client_factories.create_client(database="does_not_exist")

    assert "Are you sure it exists?" in str(e_info.value)
