import pytest
import requests
from requests import HTTPError
from requests.exceptions import ConnectionError

from chromadb.utils.embedding_functions import OllamaEmbeddingFunction


def test_ollama() -> None:
    try:
        response = requests.get("http://localhost:11434/")
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except (HTTPError, ConnectionError) as _:
        pytest.skip("Ollama server not running. Skipping test.")
    ef = OllamaEmbeddingFunction(
        model_name="llama2",
        url="http://localhost:11434/api/embeddings",
    )
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert len(embeddings) == 2
