import os
import pytest
import requests
from requests import HTTPError
from requests.exceptions import ConnectionError
from pytest_httpserver import HTTPServer
import json
from unittest.mock import patch
from chromadb.utils.embedding_functions import NomicEmbeddingFunction


@pytest.mark.skipif(
    "NOMIC_API_KEY" not in os.environ,
    reason="NOMIC_API_KEY environment variable not set, skipping test.",
)
def test_nomic() -> None:
    """
    To learn more about the Nomic API: https://docs.nomic.ai/reference/endpoints/nomic-embed-text
    Export the NOMIC_API_KEY and optionally the NOMIC_MODEL environment variables.
    """
    try:
        response = requests.get("https://api-atlas.nomic.ai/v1/health", timeout=10)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except (HTTPError, ConnectionError):
        pytest.skip("Nomic API server can't be reached. Skipping test.")
    ef = NomicEmbeddingFunction(
        api_key=os.environ.get("NOMIC_API_KEY") or "",
        model_name=os.environ.get("NOMIC_MODEL") or "nomic-embed-text-v1.5",
    )
    embeddings = ef(
        ["Henceforth, it is the map that precedes the territory", "nom nom Nomic"]
    )
    assert len(embeddings) == 2


def test_nomic_no_api_key() -> None:
    """
    To learn more about the Nomic API: https://docs.nomic.ai/reference/endpoints/nomic-embed-text
    Test intentionaly excludes the NOMIC_API_KEY.
    """
    with pytest.raises(ValueError, match="No Nomic API key provided"):
        NomicEmbeddingFunction(
            api_key="",
            model_name=os.environ.get("NOMIC_MODEL") or "nomic-embed-text-v1.5",
        )


def test_nomic_no_model() -> None:
    """
    To learn more about the Nomic API: https://docs.nomic.ai/reference/endpoints/nomic-embed-text
    Test intentionally excludes the NOMIC_MODEL. api_key does not matter since we expect an error before hitting API.
    """
    with pytest.raises(ValueError, match="No Nomic embedding model provided"):
        NomicEmbeddingFunction(
            api_key="does-not-matter",
            model_name="",
        )


def test_handle_nomic_api_returns_error() -> None:
    """
    To learn more about the Nomic API: https://docs.nomic.ai/reference/endpoints/nomic-embed-text
    Mocks an error from the Nomic API, so model and api key don't matter.
    """
    with HTTPServer() as httpserver:
        httpserver.expect_oneshot_request(
            "/embedding/text", method="POST"
        ).respond_with_data(
            json.dumps({"detail": "error"}),
            status=400,
        )
        nomic_ef = NomicEmbeddingFunction(
            api_key="does-not-matter",
            model_name="does-not-matter",
        )
        with patch.object(
            nomic_ef,
            "_api_url",
            f"http://{httpserver.host}:{httpserver.port}/embedding/text",
        ):
            with pytest.raises(Exception):
                nomic_ef(["test text"])
