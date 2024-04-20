from chromadb.api import ClientAPI
from chromadb.api.configuration import CollectionConfiguration, ConfigurationParameter


# Client level tests
def test_default_collection_configuration(client: ClientAPI):
    """Test the default values of a collection configuration."""
    client.reset()

    collection = client.create_collection("test")
    assert collection.configuration == CollectionConfiguration()


# def test_create_collection_configuration(client: ClientAPI):
#     """Test the creation default of a collection configuration."""
#     client.reset()
#     configuration = CollectionConfiguration(parameters=[ConfigurationParameter("space", "cosine"), ConfigurationParameter("ef_construction", 1000)])

#     collection = client.create_collection("test", configuration=configuration)
#     assert collection.configuration == configuration


def test_create_get_collection_configuration(client: ClientAPI):
    """Test the creation and retrieval of a collection configuration."""
    client.reset()
    configuration = CollectionConfiguration()

    collection = client.create_collection("test", configuration=configuration)
    assert collection.configuration == configuration

    retrieved_collection = client.get_collection("test")
    assert retrieved_collection.configuration == configuration
