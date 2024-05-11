import os

import pytest

# import requests
# from requests import HTTPError
# from requests.exceptions import ConnectionError

from chromadb.utils.embedding_functions import NomicEmbeddingFunction


def test_nomic() -> None:
    """
    To learn more about the Nomic API: https://docs.nomic.ai/reference/endpoints/nomic-embed-text
    Export the NOMIC_API_KEY and optionally the NOMIC_MODEL environment variables.
    """
    if os.environ.get("NOMIC_API_KEY") is None:
        pytest.skip("NOMIC_API_KEY environment variable not set. Skipping test.")
    # try:
    #     response = requests.get(os.environ.get(???, ""))
    #     # If the response was successful, no Exception will be raised
    #     response.raise_for_status()
    # except (HTTPError, ConnectionError):
    #     pytest.skip("Nomic API server can't be reached. Skipping test.")
    ef = NomicEmbeddingFunction(
        api_key=os.environ.get("NOMIC_API_KEY") or "",
        model_name=os.environ.get("NOMIC_MODEL") or "nomic-embed-text-v1.5",
    )
    embeddings = ef(
        ["Henceforth, it is the map that precedes the territory", "nom nom Nomic"]
    )
    assert len(embeddings) == 2
