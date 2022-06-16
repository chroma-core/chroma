from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from typing import Iterable


# Convenience function to hoist single elements to lists
def _hoist_to_list(item):
    if isinstance(item, str):
        return [item]
    elif isinstance(item, Iterable):
        return item
    else:
        return [item]


class ChromaDataManager:

    # Don't access these directly
    class Queries:
        _gql_get_all_embeddings = gql(
            """
            query getAllEmbeddings {
                embeddings {
                    success
                    errors
                    embeddings {
                        id
                        data
                        identifier
                        label
                    }
                }
            }
            """
        )

        _gql_create_embedding = gql(
            """
            mutation newEmbedding ($data: [Float!]! $identifier: String!, $label: String! ) {
                    createEmbedding(data: $data, identifier: $identifier, label: $label) {
                        success
                        errors
                        embedding {
                            id
                            identifier
                            label
                        }
                    }
                }
            """
        )

        _gql_batch_create_embeddings = gql(
            """
            mutation newEmbeddingBatch ($data: [[Float!]!]!, $identifiers: [String!]!, $labels: [String!]!) {
                    batchCreateEmbeddings(data: $data, identifiers: $identifiers, labels: $labels) {
                        success
                        errors
                    }
                }
            """
        )

    def __init__(self):
        transport = AIOHTTPTransport(url="http://127.0.0.1:5000/graphql")
        self._client = Client(transport=transport, fetch_schema_from_transport=True)
        self._metadata_buffer = {}

    def get_embeddings(self):
        result = self._client.execute(self.Queries._gql_get_all_embeddings)
        return result

    async def get_embeddings_async(self):
        result = await self._client.execute_async(self.Queries._gql_get_all_embeddings)
        return result 

    # Storing embeddings requires the metadata to already be available
    def set_metadata(self, identifiers, labels):
        self._clear_metadata()
        
        identifiers = _hoist_to_list(identifiers)
        labels = _hoist_to_list(labels)

        labels = [str(l) for l in labels]
        identifiers = [str(i) for i in identifiers]

        # Sanity check that we have the right number of things 
        assert(len(identifiers) == len(labels))

        self._metadata_buffer["identifiers"] = identifiers
        self._metadata_buffer["labels"] = labels

    def _clear_metadata(self):
        self._metadata_buffer = {}
        
    def store_embedding(self, data):
        # This method is only for storing a single embedding
        assert(len(self._metadata_buffer["identifiers"]) == 1)

        identifier = self._metadata_buffer["identifiers"][0]
        label = self._metadata_buffer["labels"][0]

        params = {"data": data, "identifier": identifier, "label": label}
        result = self._client.execute(self.Queries._gql_create_embedding, variable_values=params)
        self._clear_metadata()
        return result

    def store_batch_embeddings(self, data):
        # Sanity check 
        assert(len(data) == len(self._metadata_buffer["identifiers"]))

        params = {"data": data, "identifiers": self._metadata_buffer["identifiers"], "labels": self._metadata_buffer["labels"]}
        result = self._client.execute(
            self.Queries._gql_batch_create_embeddings, variable_values=params
        )
        self._clear_metadata()    
        return result