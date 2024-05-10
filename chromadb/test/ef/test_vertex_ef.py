from os import getenv

import pytest
from chromadb.utils.embedding_functions import GoogleVertexEmbeddingFunction


def test_api_key() -> None:
    api_token = getenv('GVAI_API_KEY')
    assert api_token is not None
    assert len(api_token) > 0
    assert len(api_token) == 218 # according to access token, its length is 218.


@pytest.mark.skipif(
    not getenv('GVAI_API_KEY') or not getenv('PROJECT_ID'),
    reason='API_TOKEN or PROJECT_ID is not set, skipping test.'
)
def test_vertex_ef() -> None:
    vertex_ef = GoogleVertexEmbeddingFunction(getenv('GVAI_API_KEY'),
                                              'textembedding-gecko-multilingual',
                                              getenv('PROJECT_ID'))
    embeddings = vertex_ef(['Open source is awsome.'])
    assert embeddings is not None
    assert len(embeddings) > 0
