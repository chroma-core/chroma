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
                        input_identifier
                        inference_identifier
                        label
                    }
                }
            }
            """
        )

        _gql_get_embeddings_page = gql(
            """
            query getEmbeddingsPage ($index: Int!) {
                embeddingsPage(index: $index) {
                    success
                    errors
                    embeddings {
                        id
                        data
                        input_identifier
                        inference_identifier
                        label
                    }
                    at_end
                }
            }
            """
        )

        _gql_create_embedding = gql(
            """
            mutation newEmbedding ($data: [Float!]! $input_identifier: String!, $inference_identifier: String!, $label: String! ) {
                    createEmbedding(data: $data, input_identifier: $input_identifier, inference_identifier: $inference_identifier, label: $label) {
                        success
                        errors
                        embedding {
                            id
                            data
                            input_identifier
                            inference_identifier
                            label
                        }
                    }
                }
            """
        )

        _gql_batch_create_embeddings = gql(
            """
            mutation newEmbeddingBatch ($data: [[Float!]!]!, $input_identifiers: [String!]!, $inference_identifiers: [String!]!, $labels: [String!]!) {
                    batchCreateEmbeddings(data: $data, input_identifiers: $input_identifiers, inference_identifiers: $inference_identifiers, labels: $labels) {
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
    
    def get_embeddings_page(self, index):
        params = {"index": index}
        result = self._client.execute(self.Queries._gql_get_embeddings_page, variable_values=params)
        return result 
    
    def get_embeddings_pages(self):
        index = 0
        all_results = []
        while True:
            result = self.get_embeddings_page(index)
            page = result["embeddingsPage"]
            all_results.extend(page["embeddings"])
            if page["at_end"]:
                break
            index = index + 1
        return all_results

    # Storing embeddings requires the metadata to already be available
    def set_metadata(self, input_identifiers, inference_identifiers, labels):
        self._clear_metadata()

        input_identifiers = _hoist_to_list(input_identifiers)
        inference_identifiers = _hoist_to_list(inference_identifiers)
        labels = _hoist_to_list(labels)

        input_identifiers = [str(i) for i in input_identifiers]
        inference_identifiers = [str(n) for n in inference_identifiers]
        labels = [str(l) for l in labels]

        # Sanity check that we have the right number of things
        assert len(input_identifiers) == len(labels)
        assert len(inference_identifiers) == len(labels)

        self._metadata_buffer["input_identifiers"] = input_identifiers
        self._metadata_buffer["inference_identifiers"] = inference_identifiers

        self._metadata_buffer["labels"] = labels

    def _clear_metadata(self):
        self._metadata_buffer = {}

    def store_embedding(self, data):
        # This method is only for storing a single embedding
        assert len(self._metadata_buffer["input_identifiers"]) == 1

        input_identifier = self._metadata_buffer["input_identifiers"][0]
        inference_identifier = self._metadata_buffer["inference_identifiers"][0]
        label = self._metadata_buffer["labels"][0]

        params = {
            "data": data,
            "input_identifier": input_identifier,
            "inference_identifier": inference_identifier,
            "label": label,
        }
        result = self._client.execute(self.Queries._gql_create_embedding, variable_values=params)
        self._clear_metadata()
        return result

    def store_batch_embeddings(self, data):
        # Sanity check
        assert len(data) == len(self._metadata_buffer["input_identifiers"])

        params = {
            "data": data,
            "input_identifiers": self._metadata_buffer["input_identifiers"],
            "inference_identifiers": self._metadata_buffer["inference_identifiers"],
            "labels": self._metadata_buffer["labels"],
        }
        result = self._client.execute(
            self.Queries._gql_batch_create_embeddings, variable_values=params
        )
        self._clear_metadata()
        return result
