from chromadb.utils import embedding_functions
from chromadb.api.types import EmbeddingFunction


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
        "CohereEmbeddingFunction",
        "GoogleGenerativeAiEmbeddingFunction",
        "GooglePalmEmbeddingFunction",
        "GoogleVertexEmbeddingFunction",
        "HuggingFaceEmbeddingFunction",
        "HuggingFaceEmbeddingServer",
        "InstructorEmbeddingFunction",
        "JinaEmbeddingFunction",
        "ONNXMiniLM_L6_V2",
        "OllamaEmbeddingFunction",
        "OpenAIEmbeddingFunction",
        "OpenCLIPEmbeddingFunction",
        "RoboflowEmbeddingFunction",
        "SentenceTransformerEmbeddingFunction",
        "Text2VecEmbeddingFunction",
        "ChromaLangchainEmbeddingFunction",
    }

    assert embedding_functions.get_builtins().issubset(expected_builtins)


def test_default_ef_exists() -> None:
    assert hasattr(embedding_functions, "DefaultEmbeddingFunction")
    default_ef = embedding_functions.DefaultEmbeddingFunction()

    assert default_ef is not None
    assert isinstance(default_ef, EmbeddingFunction)


def test_ef_imports() -> None:
    for ef in embedding_functions.get_builtins():
        # Langchain embedding function is a special snowflake
        if ef == "ChromaLangchainEmbeddingFunction":
            continue
        assert hasattr(embedding_functions, ef)
        assert isinstance(getattr(embedding_functions, ef), type)
        assert issubclass(getattr(embedding_functions, ef), EmbeddingFunction)
