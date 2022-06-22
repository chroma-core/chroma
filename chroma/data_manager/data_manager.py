from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from typing import Iterable
import json

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
        # _gql_get_all_embeddings = gql(
        #     """
        #     query getAllEmbeddings {
        #         embeddings {
        #             embeddings {
        #                 id
        #                 data
        #                 inputIdentifier
        #                 inferenceIdentifier
        #                 label
        #             }
        #         }
        #     }
        #     """
        # )

        _gql_get_embeddings_page = gql(
            """
            query embeddingsByPage ($first: Int, $after: String) {
                embeddingsByPage(first: $first, after: $after) {
                   pageInfo {
                        hasNextPage
                        hasPreviousPage
                        startCursor
                        endCursor
                    }
                    edges {
                        node {
                            id
                            data
                        }
                        cursor
                    }
                }
            }
            """
        )

        # _gql_create_embedding = gql(
        #     """
        #     mutation newEmbedding ($data: [Float!]! $input_identifier: String!, $inference_identifier: String!, $label: String! ) {
        #             createEmbedding(data: $data, input_identifier: $input_identifier, inference_identifier: $inference_identifier, label: $label) {
        #                 success
        #                 errors
        #                 embedding {
        #                     id
        #                     data
        #                     input_identifier
        #                     inference_identifier
        #                     label
        #                 }
        #             }
        #         }
        #     """
        # )

        # [
        #     {
        #         data: "data", 
        #         label: "label", 
        #         inferenceIdentifier: "inference_identifier",
        #         inputIdentifier: "asdfasdfasdfasdf",
        #         embeddingSetId: 1
        #     }
        # ]

        # type EmbeddingInput {
        #     data: String!
        #     label: String!
        #     inference_identifier: String!
        #     input_identifier: String!
        #     embedding_set_id: int!
        # }

        # type EmbeddingsInput {
        #     embeddings: [EmbeddingInput!]! 
        # }

        _gql_batch_create_embeddings = gql(
            """
            mutation batchCreateEmbeddings($embeddingsInput: EmbeddingsInput!) {
                addEmbeddings(embeddingsInput: $embeddingsInput) {
                    id
                    data
                    embeddingSet {
                        id
                    }
                }
            }
            """
        )

    def __init__(self):
        transport = AIOHTTPTransport(url="http://127.0.0.1:8000/graphql")
        self._client = Client(transport=transport, fetch_schema_from_transport=True)
        self._metadata_buffer = {}

    # def get_embeddings(self):
    #     result = self._client.execute(self.Queries._gql_get_all_embeddings)
    #     return result

    # async def get_embeddings_async(self):
    #     result = await self._client.execute_async(self.Queries._gql_get_all_embeddings)
    #     return result
    
    def get_embeddings_page(self, after):
        params = {"first": 100, "after": after}
        result = self._client.execute(self.Queries._gql_get_embeddings_page, variable_values=params)
        return result 
    
    def get_embeddings_pages(self):
        after = None
        all_results = []
        while True:
            result = self.get_embeddings_page(after)
            page = result["embeddingsByPage"]
            all_results.extend(page["edges"])

            page_info = page["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            end_cursor = page_info["endCursor"]
            if has_next_page:
                break
            after = end_cursor
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

    # def store_embedding(self, data):
    #     # This method is only for storing a single embedding
    #     assert len(self._metadata_buffer["input_identifiers"]) == 1

    #     input_identifier = self._metadata_buffer["input_identifiers"][0]
    #     inference_identifier = self._metadata_buffer["inference_identifiers"][0]
    #     label = self._metadata_buffer["labels"][0]

    #     params = {
    #         "data": data,
    #         "input_identifier": input_identifier,
    #         "inference_identifier": inference_identifier,
    #         "label": label,
    #     }
    #     result = self._client.execute(self.Queries._gql_create_embedding, variable_values=params)
    #     self._clear_metadata()
    #     return result

    def store_batch_embeddings(self, dataset):
        # Sanity check
        assert len(dataset) == len(self._metadata_buffer["input_identifiers"])

        new_embeddings = []
        for index, data in enumerate(dataset):
            new_embeddings.append({
                "data": json.dumps(dataset[index]),
                "inputIdentifier": self._metadata_buffer["input_identifiers"][index],
                "inferenceIdentifier": self._metadata_buffer["inference_identifiers"][index],
                "label": self._metadata_buffer["labels"][index],
                "embeddingSetId": 1
            })

        params = {
            "embeddingsInput": {"embeddings": new_embeddings}
        }
        result = self._client.execute(
            self.Queries._gql_batch_create_embeddings, variable_values=params
        )
        self._clear_metadata()
        return result
