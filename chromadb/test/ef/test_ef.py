from chromadb.utils import embedding_functions
from chromadb.utils.embedding_functions import (
    EmbeddingFunction,
    register_embedding_function,
)
from typing import Dict, Any
import pytest
from chromadb.api.types import (
    Embeddings,
    Space,
    Embeddable,
    SparseEmbeddingFunction,
)
from chromadb.api.models.CollectionCommon import validation_context


def test_get_builtins_holds() -> None:
    """
    Ensure that `get_builtins` is consistent after the ef migration.

    This test is intended to be temporary until the ef migration is complete as
    these expected builtins are likely to grow as long as users add new
    embedding functions.

    REMOVE ME ON THE NEXT EF ADDITION
    """
    expected_builtins = {
        "AmazonBedrockEmbeddingFunction",
        "BasetenEmbeddingFunction",
        "CloudflareWorkersAIEmbeddingFunction",
        "CohereEmbeddingFunction",
        "VoyageAIEmbeddingFunction",
        "GoogleGenerativeAiEmbeddingFunction",
        "GooglePalmEmbeddingFunction",
        "GoogleVertexEmbeddingFunction",
        "GoogleGeminiEmbeddingFunction",
        "GoogleGenaiEmbeddingFunction",  # Backward compatibility alias
        "HuggingFaceEmbeddingFunction",
        "HuggingFaceEmbeddingServer",
        "InstructorEmbeddingFunction",
        "JinaEmbeddingFunction",
        "MistralEmbeddingFunction",
        "MorphEmbeddingFunction",
        "NomicEmbeddingFunction",
        "ONNXMiniLM_L6_V2",
        "OllamaEmbeddingFunction",
        "OpenAIEmbeddingFunction",
        "OpenCLIPEmbeddingFunction",
        "RoboflowEmbeddingFunction",
        "SentenceTransformerEmbeddingFunction",
        "Text2VecEmbeddingFunction",
        "ChromaLangchainEmbeddingFunction",
        "TogetherAIEmbeddingFunction",
        "DefaultEmbeddingFunction",
        "HuggingFaceSparseEmbeddingFunction",
        "FastembedSparseEmbeddingFunction",
        "Bm25EmbeddingFunction",
        "ChromaCloudQwenEmbeddingFunction",
        "ChromaCloudSpladeEmbeddingFunction",
        "ChromaBm25EmbeddingFunction",
        "PerplexityEmbeddingFunction",
    }

    assert expected_builtins == embedding_functions.get_builtins()


def test_default_ef_exists() -> None:
    assert hasattr(embedding_functions, "DefaultEmbeddingFunction")
    default_ef = embedding_functions.DefaultEmbeddingFunction()

    assert default_ef is not None
    assert isinstance(default_ef, EmbeddingFunction) or isinstance(
        default_ef, SparseEmbeddingFunction
    )


def test_ef_imports() -> None:
    for ef in embedding_functions.get_builtins():
        # Langchain embedding function is a special snowflake
        if ef == "ChromaLangchainEmbeddingFunction":
            continue
        assert hasattr(embedding_functions, ef)
        assert isinstance(getattr(embedding_functions, ef), type)
        assert issubclass(
            getattr(embedding_functions, ef), EmbeddingFunction
        ) or issubclass(getattr(embedding_functions, ef), SparseEmbeddingFunction)


@register_embedding_function
class CustomEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(self, dim: int = 3):
        self._dim = dim

    @validation_context("custom_ef_call")
    def __call__(self, input: Embeddable) -> Embeddings:
        raise Exception("This is a test exception")

    @staticmethod
    def name() -> str:
        return "custom_ef"

    def get_config(self) -> Dict[str, Any]:
        return {"dim": self._dim}

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "CustomEmbeddingFunction":
        return CustomEmbeddingFunction(dim=config["dim"])

    def default_space(self) -> Space:
        return "cosine"


def test_validation_context_with_custom_ef() -> None:
    custom_ef = CustomEmbeddingFunction()

    with pytest.raises(Exception) as excinfo:
        custom_ef(["test data"])

    original_msg = "This is a test exception"
    expected_msg = f"{original_msg} in custom_ef_call."
    assert str(excinfo.value) == expected_msg
    assert excinfo.value.args == (expected_msg,)


@pytest.mark.parametrize(
    "ef_name, required_module, provider_env_var, kwargs",
    [
        (
            "BasetenEmbeddingFunction",
            "openai",
            "BASETEN_API_KEY",
            {"api_key": None, "api_base": "http://localhost"},
        ),
        (
            "CloudflareWorkersAIEmbeddingFunction",
            "httpx",
            "CLOUDFLARE_API_KEY",
            {"model_name": "m", "account_id": "a"},
        ),
        ("CohereEmbeddingFunction", "cohere", "COHERE_API_KEY", {}),
        ("HuggingFaceEmbeddingFunction", "httpx", "HUGGINGFACE_API_KEY", {}),
        (
            "HuggingFaceEmbeddingServer",
            "httpx",
            "HUGGINGFACE_API_KEY",
            {"url": "http://localhost"},
        ),
        ("JinaEmbeddingFunction", "PIL", "JINA_API_KEY", {}),
        ("OpenAIEmbeddingFunction", "openai", "OPENAI_API_KEY", {}),
        ("PerplexityEmbeddingFunction", "perplexity", "PERPLEXITY_API_KEY", {}),
        ("RoboflowEmbeddingFunction", "PIL", "ROBOFLOW_API_KEY", {}),
        (
            "TogetherAIEmbeddingFunction",
            "httpx",
            "TOGETHER_API_KEY",
            {"model_name": "m"},
        ),
        ("VoyageAIEmbeddingFunction", "voyageai", "VOYAGE_API_KEY", {}),
    ],
)
def test_explicit_api_key_env_var_is_respected(
    monkeypatch: pytest.MonkeyPatch,
    ef_name: str,
    required_module: str,
    provider_env_var: str,
    kwargs: Dict[str, Any],
) -> None:
    pytest.importorskip(required_module)
    monkeypatch.setenv(provider_env_var, "provider-key")
    monkeypatch.setenv("MY_CUSTOM_API_KEY", "custom-key")

    ef = getattr(embedding_functions, ef_name)(
        api_key_env_var="MY_CUSTOM_API_KEY", **kwargs
    )

    assert ef.api_key_env_var == "MY_CUSTOM_API_KEY"
    assert ef.get_config()["api_key_env_var"] == "MY_CUSTOM_API_KEY"


def test_provider_api_key_env_var_used_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("openai")
    monkeypatch.setenv("OPENAI_API_KEY", "provider-key")

    ef = embedding_functions.OpenAIEmbeddingFunction()

    assert ef.api_key_env_var == "OPENAI_API_KEY"
    assert ef.api_key == "provider-key"
