import os

import pytest
import requests
from requests import HTTPError
from requests.exceptions import ConnectionError

from chromadb.utils.embedding_functions import OllamaEmbeddingFunction


def test_ollama() -> None:
    """
    To set up the Ollama server, follow instructions at: https://github.com/ollama/ollama?tab=readme-ov-file
    Export the OLLAMA_SERVER_URL and OLLAMA_MODEL environment variables.
    """
    if (
        os.environ.get("OLLAMA_SERVER_URL") is None
        or os.environ.get("OLLAMA_MODEL") is None
    ):
        pytest.skip(
            "OLLAMA_SERVER_URL or OLLAMA_MODEL environment variable not set. Skipping test."
        )
    try:
        response = requests.get(os.environ.get("OLLAMA_SERVER_URL", ""))
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except (HTTPError, ConnectionError):
        pytest.skip("Ollama server not running. Skipping test.")
    ef = OllamaEmbeddingFunction(
        model_name=os.environ.get("OLLAMA_MODEL") or "nomic-embed-text",
        url=f"{os.environ.get('OLLAMA_SERVER_URL')}/embeddings",
    )
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert len(embeddings) == 2
