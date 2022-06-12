from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport


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
                    }
                }
            }
            """
        )

        _gql_create_embedding = gql(
            """
            mutation newEmbedding ($data: [Float!]!) {
                    createEmbedding(data: $data) {
                        success
                        errors
                        embedding {
                            id
                        }
                    }
                }
            """
        )

        _gql_batch_create_embeddings = gql(
            """
            mutation newEmbeddingBatch ($data: [[Float!]!]!) {
                    batchCreateEmbeddings(data: $data) {
                        success
                        errors
                    }
                }
            """
        )

    def __init__(self):
        transport = AIOHTTPTransport(url="http://127.0.0.1:5000/graphql")
        self._client = Client(transport=transport, fetch_schema_from_transport=True)

    def get_embeddings(self):
        result = self._client.execute(self.Queries._gql_get_all_embeddings)
        return result

    async def get_embeddings_async(self):
        result = await self._client.execute_async(self.Queries._gql_get_all_embeddings)
        return result 
        
    def store_embedding(self, data):
        params = {"data": data}
        result = self._client.execute(self.Queries._gql_create_embedding, variable_values=params)
        return result

    def store_batch_embeddings(self, data):
        params = {"data": data}
        result = self._client.execute(
            self.Queries._gql_batch_create_embeddings, variable_values=params
        )
        return result
