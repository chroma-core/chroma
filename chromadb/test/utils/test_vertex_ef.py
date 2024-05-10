from os import getenv

import pytest
from chromadb.utils.embedding_functions import GoogleVertexEmbeddingFunction


@pytest.mark.skipif(
    not getenv('GVAI_API_TOKEN') or not getenv('PROJECT_ID'),
    reason='API_TOKEN or PROJECT_ID is not set, skipping test.'
)
def test_vertex_ef() -> None:
    vertex_ef = GoogleVertexEmbeddingFunction(getenv('GVAI_API_TOKEN'),
                                              'textembedding-gecko-multilingual',
                                              getenv('PROJECT_ID'))
    embeddings = vertex_ef(['Open source is awsome.'])
    assert embeddings is not None
    assert len(embeddings) > 0
