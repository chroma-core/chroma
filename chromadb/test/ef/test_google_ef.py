import pytest

from chromadb import __version__
from chromadb.utils import embedding_functions


@pytest.mark.parametrize(
    "ef_name, api_key_env_var",
    [
        ("GoogleGenerativeAiEmbeddingFunction", "GEMINI_API_KEY"),
        ("GooglePalmEmbeddingFunction", "CHROMA_GOOGLE_PALM_API_KEY"),
    ],
)
def test_google_generativeai_ef_sends_client_header(
    monkeypatch: pytest.MonkeyPatch, ef_name: str, api_key_env_var: str
) -> None:
    pytest.importorskip("google.generativeai")
    from google.generativeai import client as genai_client

    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv(api_key_env_var, "test-key")

    getattr(embedding_functions, ef_name)()

    client_config = genai_client._client_manager.client_config
    assert client_config["client_options"].api_key == "test-key"
    header, value = client_config["client_info"].to_grpc_metadata()
    assert header == "x-goog-api-client"
    assert value.startswith(f"chroma/{__version__} ")
