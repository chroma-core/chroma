import os
from typing import Generator
import numpy as np
import pytest
import chromadb
from chromadb.api.types import (
    Embeddable,
    EmbeddingFunction,
    Image,
    Document,
)
from chromadb.test.property.invariants import _exact_distances
from chromadb.config import Settings
from chromadb.utils.embedding_functions import VoyageAIEmbeddingFunction


def random_image() -> Image:
    return np.random.randint(0, 255, size=(10, 10, 3), dtype=np.int64)


def random_document() -> Document:
    return str(random_image())


@pytest.fixture
def multimodal_collection() -> Generator[chromadb.Collection, None, None]:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")

    ef: EmbeddingFunction[Embeddable] = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )

    settings = Settings()
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        host = os.environ.get("CHROMA_SERVER_HOST", "localhost")
        port = int(os.environ.get("CHROMA_SERVER_HTTP_PORT", 0))
        settings.chroma_api_impl = "chromadb.api.fastapi.FastAPI"
        settings.chroma_server_http_port = port
        settings.chroma_server_host = host

    client = chromadb.Client(settings=settings)
    collection = client.create_collection(
        name="multimodal_collection", embedding_function=ef
    )
    yield collection
    client.clear_system_cache()


# Test adding and querying of a multimodal collection consisting of images and documents
def test_multimodal(
    multimodal_collection: chromadb.Collection,
    n_examples: int = 10,
    n_query_results: int = 3,
) -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")

    ef: EmbeddingFunction[Embeddable] = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    # Fix numpy's random seed for reproducibility
    random_state = np.random.get_state()
    np.random.seed(0)

    image_ids = [str(i) for i in range(n_examples)]
    images = [random_image() for _ in range(n_examples)]
    image_embeddings = ef(images)

    document_ids = [str(i) for i in range(n_examples, 2 * n_examples)]
    documents = [random_document() for _ in range(n_examples)]
    document_embeddings = ef(documents)

    # Trying to add a document and an image at the same time should fail
    with pytest.raises(
        ValueError,
        # This error string may be in any order
        match=r"Exactly one of (images|documents|uris)(?:, (images|documents|uris))?(?:, (images|documents|uris))? must be provided in add\.",
    ):
        multimodal_collection.add(
            ids=image_ids[0], documents=documents[0], images=images[0]
        )

    # Add some documents
    multimodal_collection.add(ids=document_ids, documents=documents)
    # Add some images
    multimodal_collection.add(ids=image_ids, images=images)

    # get() should return all the documents and images
    # ids corresponding to images should not have documents
    get_result = multimodal_collection.get(include=["documents"])
    assert len(get_result["ids"]) == len(document_ids) + len(image_ids)
    for i, id in enumerate(get_result["ids"]):
        assert id in document_ids or id in image_ids
        assert get_result["documents"] is not None
        if id in document_ids:
            assert get_result["documents"][i] == documents[document_ids.index(id)]
        if id in image_ids:
            assert get_result["documents"][i] is None

    # Generate a random query image
    query_image = random_image()
    query_image_embedding = ef([query_image])

    image_neighbor_indices, _ = _exact_distances(
        query_image_embedding, image_embeddings + document_embeddings
    )
    # Get the ids of the nearest neighbors
    nearest_image_neighbor_ids = [
        image_ids[i] if i < n_examples else document_ids[i % n_examples]
        for i in image_neighbor_indices[0][:n_query_results]
    ]

    # Generate a random query document
    query_document = random_document()
    query_document_embedding = ef([query_document])
    document_neighbor_indices, _ = _exact_distances(
        query_document_embedding, image_embeddings + document_embeddings
    )
    nearest_document_neighbor_ids = [
        image_ids[i] if i < n_examples else document_ids[i % n_examples]
        for i in document_neighbor_indices[0][:n_query_results]
    ]

    # Querying with both images and documents should fail
    with pytest.raises(ValueError):
        multimodal_collection.query(
            query_images=[query_image], query_texts=[query_document]
        )

    # Query with images
    query_result = multimodal_collection.query(
        query_images=[query_image], n_results=n_query_results, include=["documents"]
    )

    assert query_result["ids"][0] == nearest_image_neighbor_ids

    # Query with documents
    query_result = multimodal_collection.query(
        query_texts=[query_document], n_results=n_query_results, include=["documents"]
    )

    assert query_result["ids"][0] == nearest_document_neighbor_ids
    np.random.set_state(random_state)


@pytest.mark.xfail
def test_multimodal_update_with_image(
    multimodal_collection: chromadb.Collection,
) -> None:
    # Updating an entry with an existing document should remove the documentß

    document = random_document()
    image = random_image()
    id = "0"

    multimodal_collection.add(ids=id, documents=document)

    multimodal_collection.update(ids=id, images=image)

    get_result = multimodal_collection.get(ids=id, include=["documents"])
    assert get_result["documents"] is not None
    assert get_result["documents"][0] is None
