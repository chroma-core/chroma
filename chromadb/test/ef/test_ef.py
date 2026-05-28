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


def _construct_ef(name: str, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Construct one of the affected EFs, skipping if its SDK is unavailable."""
    monkeypatch.setenv("OPENAI_API_KEY", "test")
    monkeypatch.setenv("COHERE_API_KEY", "test")
    monkeypatch.setenv("MORPH_API_KEY", "test")
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test")
    monkeypatch.setenv("TOGETHER_API_KEY", "test")
    monkeypatch.setenv("VOYAGE_API_KEY", "test")
    monkeypatch.setenv("JINA_API_KEY", "test")
    monkeypatch.setenv("HUGGINGFACE_API_KEY", "test")
    monkeypatch.setenv("CLOUDFLARE_API_KEY", "test")
    monkeypatch.setenv("CHROMA_CLOUDFLARE_ACCOUNT_ID", "test")

    if name == "openai":
        from chromadb.utils.embedding_functions.openai_embedding_function import (
            OpenAIEmbeddingFunction,
        )

        return OpenAIEmbeddingFunction(model_name="text-embedding-3-small")
    if name == "cohere":
        from chromadb.utils.embedding_functions.cohere_embedding_function import (
            CohereEmbeddingFunction,
        )

        return CohereEmbeddingFunction()
    if name == "morph":
        from chromadb.utils.embedding_functions.morph_embedding_function import (
            MorphEmbeddingFunction,
        )

        return MorphEmbeddingFunction()
    if name == "perplexity":
        from chromadb.utils.embedding_functions.perplexity_embedding_function import (
            PerplexityEmbeddingFunction,
        )

        return PerplexityEmbeddingFunction()
    if name == "together_ai":
        from chromadb.utils.embedding_functions.together_ai_embedding_function import (
            TogetherAIEmbeddingFunction,
        )

        monkeypatch.setenv("CHROMA_TOGETHER_AI_API_KEY", "test")
        return TogetherAIEmbeddingFunction(model_name="togethercomputer/m2-bert-80M-8k-retrieval")
    if name == "voyageai":
        from chromadb.utils.embedding_functions.voyageai_embedding_function import (
            VoyageAIEmbeddingFunction,
        )

        return VoyageAIEmbeddingFunction()
    if name == "jina":
        from chromadb.utils.embedding_functions.jina_embedding_function import (
            JinaEmbeddingFunction,
        )

        return JinaEmbeddingFunction()
    if name == "huggingface":
        from chromadb.utils.embedding_functions.huggingface_embedding_function import (
            HuggingFaceEmbeddingFunction,
        )

        return HuggingFaceEmbeddingFunction()
    if name == "cloudflare_workers_ai":
        from chromadb.utils.embedding_functions.cloudflare_workers_ai_embedding_function import (  # noqa: E501
            CloudflareWorkersAIEmbeddingFunction,
        )

        monkeypatch.setenv("CHROMA_CLOUDFLARE_API_KEY", "test")
        return CloudflareWorkersAIEmbeddingFunction(
            model_name="@cf/baai/bge-base-en-v1.5", account_id="test"
        )
    raise AssertionError(f"unknown EF name {name}")


# Builtin EFs that historically rejected any update whose new config contained
# model_name, even when the value was unchanged. Tracked by name to keep the
# test parametrization stable when an SDK is missing locally.
_EF_NAMES = [
    "openai",
    "cohere",
    "morph",
    "perplexity",
    "together_ai",
    "voyageai",
    "jina",
    "huggingface",
    "cloudflare_workers_ai",
]


@pytest.mark.parametrize("name", _EF_NAMES)
def test_validate_config_update_accepts_unchanged_model_name(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Updating an EF with the same model_name must not raise.

    Regression test: previously these EFs raised
    "The model name cannot be changed after the embedding function has been
    initialized." whenever the new config contained model_name at all, even
    when it matched the existing value. That made it impossible to update any
    other field through `overwrite_embedding_function`, since `get_config()`
    always includes model_name.
    """
    try:
        ef = _construct_ef(name, monkeypatch)
    except (ImportError, ValueError) as exc:
        # ValueError covers EFs that re-raise ImportError as ValueError when
        # an optional SDK is missing.
        pytest.skip(f"{name} SDK not available: {exc}")

    cfg = ef.get_config()
    ef.validate_config_update(cfg, cfg)


@pytest.mark.parametrize("name", _EF_NAMES)
def test_validate_config_update_rejects_changed_model_name(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changing the model_name must still raise."""
    try:
        ef = _construct_ef(name, monkeypatch)
    except (ImportError, ValueError) as exc:
        pytest.skip(f"{name} SDK not available: {exc}")

    old_cfg = ef.get_config()
    new_cfg = dict(old_cfg)
    key = "model_name" if "model_name" in new_cfg else "model"
    new_cfg[key] = "definitely-a-different-model"
    with pytest.raises(ValueError, match="cannot be changed"):
        ef.validate_config_update(old_cfg, new_cfg)
