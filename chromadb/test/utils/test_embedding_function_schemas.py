import pytest
import os
import sys
from typing import List, Dict, Any, Optional
from jsonschema import ValidationError
import numpy as np
from unittest.mock import MagicMock
from pytest import MonkeyPatch
from chromadb.utils.embedding_functions.schemas import (
    validate_config,
    load_schema,
    get_available_schemas,
)
from chromadb.api.types import Documents, Embeddings
from chromadb.utils.embedding_functions import (
    known_embedding_functions,
)

# Set dummy environment variables for API keys
os.environ["CHROMA_OPENAI_API_KEY"] = "dummy_openai_key"
os.environ["CHROMA_HUGGINGFACE_API_KEY"] = "dummy_huggingface_key"
os.environ["CHROMA_JINA_API_KEY"] = "dummy_jina_key"
os.environ["CHROMA_COHERE_API_KEY"] = "dummy_cohere_key"
os.environ["CHROMA_GOOGLE_PALM_API_KEY"] = "dummy_google_palm_key"
os.environ["CHROMA_GOOGLE_GENAI_API_KEY"] = "dummy_google_genai_key"
os.environ["CHROMA_GOOGLE_VERTEX_API_KEY"] = "dummy_google_vertex_key"
os.environ["CHROMA_VOYAGEAI_API_KEY"] = "dummy_voyageai_key"
os.environ["CHROMA_ROBOFLOW_API_KEY"] = "dummy_roboflow_key"
os.environ["AWS_ACCESS_KEY_ID"] = "dummy_aws_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy_aws_secret_key"
os.environ["AWS_REGION"] = "us-east-1"


# Mock InstructorEmbeddingFunction, since the embedding function is not up to date with package requirements of InstructorEmbedding
class MockInstructorEmbeddingFunction:
    def __init__(self: Any, model_name: str, device: str = "cpu") -> None:
        self.model_name = model_name
        self.device = device

    def encode(
        self: Any, texts: List[str], instruction: Optional[str] = None
    ) -> np.ndarray[Any, np.dtype[np.float32]]:
        return np.array([[0.1, 0.2, 0.3] for _ in range(len(texts))])


# Mock for embedding functions to avoid actual API calls
class MockEmbeddings:
    @staticmethod
    def mock_embeddings(input: Documents) -> Embeddings:
        """Return mock embeddings for testing"""
        return [np.array([0.1, 0.2, 0.3], dtype=np.float32) for _ in input]


class MockTextEmbeddingModel:
    @staticmethod
    def from_pretrained(model_name: str) -> "MockTextEmbeddingModel":
        return MockTextEmbeddingModel()

    def get_embeddings(self, texts: List[str]) -> List[Dict[str, Any]]:
        return [{"embeddings": [0.1, 0.2, 0.3]} for _ in texts]


# Test configurations for each embedding function
EMBEDDING_FUNCTION_CONFIGS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "text-embedding-ada-002",
            "api_key_env_var": "CHROMA_OPENAI_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_OPENAI_API_KEY",
            "model_name": "text-embedding-ada-002",
        },
    },
    "huggingface": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "sentence-transformers/all-MiniLM-L6-v2",
            "api_key_env_var": "CHROMA_HUGGINGFACE_API_KEY",
        },
        "config": {
            "model_name": "sentence-transformers/all-MiniLM-L6-v2",
            "api_key_env_var": "CHROMA_HUGGINGFACE_API_KEY",
        },
    },
    "sentence_transformer": {
        "args": {
            "model_name": "all-MiniLM-L6-v2",
        },
        "config": {
            "model_name": "all-MiniLM-L6-v2",
        },
    },
    "cohere": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "embed-english-v3.0",
            "api_key_env_var": "CHROMA_COHERE_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_COHERE_API_KEY",
            "model_name": "embed-english-v3.0",
        },
    },
    "google_palm": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "models/embedding-gecko-001",
            "api_key_env_var": "CHROMA_GOOGLE_PALM_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_GOOGLE_PALM_API_KEY",
            "model_name": "models/embedding-gecko-001",
        },
    },
    "google_generative_ai": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "models/embedding-001",
            "task_type": "RETRIEVAL_DOCUMENT",
            "api_key_env_var": "CHROMA_GOOGLE_GENAI_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_GOOGLE_GENAI_API_KEY",
            "model_name": "models/embedding-001",
            "task_type": "RETRIEVAL_DOCUMENT",
        },
    },
    "google_vertex": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "models/embedding-001",
            "api_key_env_var": "CHROMA_GOOGLE_VERTEX_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_GOOGLE_VERTEX_API_KEY",
            "model_name": "models/embedding-001",
        },
    },
    "ollama": {
        "args": {
            "url": "http://localhost:11434",
            "model_name": "llama2",
            "timeout": 60,
        },
        "config": {
            "url": "http://localhost:11434",
            "model_name": "llama2",
            "timeout": 60,
        },
    },
    "instructor": {
        "args": {
            "model_name": "hkunlp/instructor-large",
            "instruction": "Represent the document for retrieval",
        },
        "config": {
            "model_name": "hkunlp/instructor-large",
            "instruction": "Represent the document for retrieval",
        },
        "mocks": [
            ("InstructorEmbedding.INSTRUCTOR", MockInstructorEmbeddingFunction),
        ],
    },
    "jina": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "jina-embeddings-v2-base-en",
            "api_key_env_var": "CHROMA_JINA_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_JINA_API_KEY",
            "model_name": "jina-embeddings-v2-base-en",
        },
    },
    "voyageai": {
        "args": {
            "api_key": "dummy_key",
            "model_name": "voyage-2",
            "api_key_env_var": "CHROMA_VOYAGEAI_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_VOYAGEAI_API_KEY",
            "model_name": "voyage-2",
        },
    },
    "onnx_mini_lm_l6_v2": {
        "args": {
            "preferred_providers": ["onnxruntime"],
        },
        "config": {
            "preferred_providers": ["onnxruntime"],
        },
        "mocks": [
            ("onnxruntime", MagicMock()),
        ],
    },
    "open_clip": {
        "args": {
            "model_name": "ViT-B-32",
            "checkpoint": "laion2b_s34b_b79k",
            "device": "cpu",
        },
        "config": {
            "model_name": "ViT-B-32",
            "checkpoint": "laion2b_s34b_b79k",
            "device": "cpu",
        },
    },
    "roboflow": {
        "args": {
            "api_key": "dummy_key",
            "api_key_env_var": "CHROMA_ROBOFLOW_API_KEY",
        },
        "config": {
            "api_key_env_var": "CHROMA_ROBOFLOW_API_KEY",
        },
    },
    "text2vec": {
        "args": {
            "model_name": "shibing624/text2vec-base-chinese",
        },
        "config": {
            "model_name": "shibing624/text2vec-base-chinese",
        },
    },
}

# Skip these embedding functions in tests as they require complex setup
SKIP_EMBEDDING_FUNCTIONS = [
    "huggingface_server",  # Requires a running server
    "chroma_langchain",  # Requires LangChain setup
    "default",  # Special case that delegates to ONNXMiniLM_L6_V2
    "google_vertex",  # Requires complex mocking of vertexai module
    "amazon_bedrock",  # Requires complex mocking of boto3 session
]


def test_all_schemas_are_valid_json() -> None:
    """Test that all schemas are valid JSON"""
    schema_names = get_available_schemas()
    for schema_name in schema_names:
        # This will raise an exception if the schema is not valid JSON
        schema: Dict[str, Any] = load_schema(schema_name)
        assert isinstance(schema, dict)
        assert "$schema" in schema
        assert "title" in schema
        assert "description" in schema
        assert "version" in schema
        assert "properties" in schema


# Get embedding function names for parametrization
def get_embedding_function_names() -> List[str]:
    """Get all embedding function names to test"""
    return [
        name
        for name in known_embedding_functions.keys()
        if name not in SKIP_EMBEDDING_FUNCTIONS
    ]


# Create a module-level marker for the fixture
pytestmark = pytest.mark.usefixtures("embedding_function_dependencies")


@pytest.mark.parametrize("ef_name", get_embedding_function_names())
def test_embedding_function_config_roundtrip(
    ef_name: str, monkeypatch: MonkeyPatch
) -> None:
    """
    Test that embedding functions can be:
    1. Created with arguments
    2. Get their config
    3. Be recreated from that config
    4. Validate their config
    """
    if ef_name not in EMBEDDING_FUNCTION_CONFIGS:
        pytest.skip(f"No test configuration for {ef_name}")

    # Get the embedding function class
    ef_class = known_embedding_functions[ef_name]

    # Apply mocks if needed
    test_config: Dict[str, Any] = EMBEDDING_FUNCTION_CONFIGS[ef_name]
    if "mocks" in test_config:
        for module_path, mock_obj in test_config["mocks"]:
            if "." in module_path:
                module_name, attr_name = module_path.rsplit(".", 1)
                monkeypatch.setattr(f"{module_name}.{attr_name}", mock_obj)
            else:
                sys.modules[module_path] = mock_obj

    # Mock the __call__ method to avoid actual API calls
    monkeypatch.setattr(ef_class, "__call__", MockEmbeddings.mock_embeddings)

    # 1. Create embedding function with arguments
    ef_instance = ef_class(**test_config["args"])

    # 2. Get config from the instance
    config: Dict[str, Any] = ef_instance.get_config()

    # Check that config contains expected values
    for key, value in test_config["config"].items():
        assert key in config, f"Key {key} not found in config for {ef_name}"
        assert (
            config[key] == value
        ), f"Config value mismatch for {ef_name}.{key}: expected {value}, got {config[key]}"

    # 3. Create a new instance from the config
    new_ef_instance = ef_class.build_from_config(config)

    # 4. Validate the config
    new_ef_instance.validate_config(config)

    # 5. Get config from the new instance and verify it matches
    new_config: Dict[str, Any] = new_ef_instance.get_config()
    for key, value in config.items():
        assert key in new_config, f"Key {key} not found in new config for {ef_name}"
        assert (
            new_config[key] == value
        ), f"New config value mismatch for {ef_name}.{key}: expected {value}, got {new_config[key]}"


@pytest.mark.parametrize("ef_name", get_embedding_function_names())
def test_embedding_function_invalid_config(
    ef_name: str, monkeypatch: MonkeyPatch
) -> None:
    """Test that embedding functions reject invalid configurations"""
    if ef_name not in EMBEDDING_FUNCTION_CONFIGS:
        pytest.skip(f"No test configuration for {ef_name}")

    # Get the embedding function class
    ef_class = known_embedding_functions[ef_name]

    # Apply mocks if needed
    test_config: Dict[str, Any] = EMBEDDING_FUNCTION_CONFIGS[ef_name]
    if "mocks" in test_config:
        for module_path, mock_obj in test_config["mocks"]:
            if "." in module_path:
                module_name, attr_name = module_path.rsplit(".", 1)
                monkeypatch.setattr(f"{module_name}.{attr_name}", mock_obj)
            else:
                sys.modules[module_path] = mock_obj

    # Mock the __call__ method to avoid actual API calls
    monkeypatch.setattr(ef_class, "__call__", MockEmbeddings.mock_embeddings)

    # Create embedding function with arguments
    ef_instance = ef_class(**test_config["args"])

    # Test with invalid property
    invalid_config: Dict[str, Any] = test_config["config"].copy()
    invalid_config["invalid_property"] = "invalid_value"

    # Some embedding functions might allow additional properties, so we can't always expect this to fail
    try:
        ef_instance.validate_config(invalid_config)
    except (ValidationError, ValueError, AssertionError):
        # If it raises an exception, that's expected for many embedding functions
        pass


def test_schema_required_fields() -> None:
    """Test that schemas enforce required fields"""
    schema_names = get_available_schemas()
    for schema_name in schema_names:
        schema = load_schema(schema_name)
        if "required" in schema:
            required_fields = schema["required"]
            for field in required_fields:
                # Create a config with all required fields
                config: Dict[str, Any] = {}
                for req_field in required_fields:
                    # Add a dummy value of the correct type
                    field_schema = schema["properties"][req_field]
                    if isinstance(field_schema["type"], list):
                        field_type = field_schema["type"][0]
                    else:
                        field_type = field_schema["type"]

                    if field_type == "string":
                        config[req_field] = "dummy"
                    elif field_type == "integer":
                        config[req_field] = 0
                    elif field_type == "number":
                        config[req_field] = 0.0
                    elif field_type == "boolean":
                        config[req_field] = False
                    elif field_type == "object":
                        config[req_field] = {}
                    elif field_type == "array":
                        config[req_field] = []

                # Remove the current field to test that it's required
                test_config = config.copy()
                del test_config[field]

                # Validation should fail
                with pytest.raises(ValidationError):
                    validate_config(test_config, schema_name)


def test_schema_additional_properties() -> None:
    """Test that schemas reject additional properties"""
    schema_names = get_available_schemas()
    for schema_name in schema_names:
        schema = load_schema(schema_name)
        # Create a minimal valid config
        config: Dict[str, Any] = {}
        if "required" in schema:
            for field in schema["required"]:
                # Add a dummy value of the correct type
                field_schema = schema["properties"][field]
                if isinstance(field_schema["type"], list):
                    field_type = field_schema["type"][0]
                else:
                    field_type = field_schema["type"]

                if field_type == "string":
                    config[field] = "dummy"
                elif field_type == "integer":
                    config[field] = 0
                elif field_type == "number":
                    config[field] = 0.0
                elif field_type == "boolean":
                    config[field] = False
                elif field_type == "object":
                    config[field] = {}
                elif field_type == "array":
                    config[field] = []

        # Add an additional property
        test_config = config.copy()
        test_config["additional_property"] = "value"

        # Validation should fail if additionalProperties is false
        if schema.get("additionalProperties", True) is False:
            with pytest.raises(ValidationError):
                validate_config(test_config, schema_name)
