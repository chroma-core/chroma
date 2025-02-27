import pytest
import os
from typing import List, Dict, Any
from jsonschema import ValidationError

from chromadb.utils.embedding_functions.schemas import validate_config, load_schema
from chromadb.utils.embedding_functions import (
    OpenAIEmbeddingFunction,
    HuggingFaceEmbeddingFunction,
    OllamaEmbeddingFunction,
    JinaEmbeddingFunction,
    ONNXMiniLM_L6_V2,
    OpenCLIPEmbeddingFunction,
)

# Set dummy environment variables for API keys
os.environ["OPENAI_API_KEY"] = "dummy_openai_key"
os.environ["HUGGINGFACE_API_KEY"] = "dummy_huggingface_key"
os.environ["JINA_API_KEY"] = "dummy_jina_key"


# Path to the schemas directory
SCHEMAS_DIR = os.path.dirname(
    os.path.abspath(
        os.path.join(__file__, "../../../utils/embedding_functions/schemas")
    )
)


def get_all_schema_names() -> List[str]:
    """Get all schema names from the schemas directory"""
    schema_files = [f for f in os.listdir(SCHEMAS_DIR) if f.endswith(".json")]
    return [os.path.splitext(f)[0] for f in schema_files]


def test_all_schemas_are_valid_json() -> None:
    """Test that all schemas are valid JSON"""
    schema_names = get_all_schema_names()
    for schema_name in schema_names:
        # This will raise an exception if the schema is not valid JSON
        schema = load_schema(schema_name)
        assert isinstance(schema, dict)
        assert "$schema" in schema
        assert "title" in schema
        assert "description" in schema
        assert "version" in schema
        assert "properties" in schema


def test_validate_config_with_valid_configs() -> None:
    """Test validate_config with valid configurations"""
    # Test OpenAI
    openai_config = {
        "api_key_env_var": "OPENAI_API_KEY",
        "model_name": "text-embedding-ada-002",
    }
    validate_config(openai_config, "openai")

    # Test HuggingFace
    huggingface_config: Dict[str, str] = {
        "model_name": "sentence-transformers/all-MiniLM-L6-v2",
        "api_key_env_var": "HUGGINGFACE_API_KEY",
    }
    validate_config(huggingface_config, "huggingface")

    # Test Google PaLM
    google_palm_config = {
        "api_key_env_var": "GOOGLE_PALM_API_KEY",
        "model_name": "models/embedding-gecko-001",
    }
    validate_config(google_palm_config, "google_palm")

    # Test Google Generative AI
    google_generative_ai_config = {
        "api_key_env_var": "GOOGLE_API_KEY",
        "model_name": "models/embedding-001",
        "task_type": "RETRIEVAL_DOCUMENT",
    }
    validate_config(google_generative_ai_config, "google_generative_ai")

    # Test Google Vertex
    google_vertex_config = {
        "api_key_env_var": "GOOGLE_API_KEY",
        "model_name": "textembedding-gecko",
        "project_id": "cloud-large-language-models",
        "region": "us-central1",
    }
    validate_config(google_vertex_config, "google_vertex")

    # Test Ollama
    ollama_config = {
        "url": "http://localhost:11434",
        "model_name": "llama2",
        "timeout": 60,
    }
    validate_config(ollama_config, "ollama")

    # Test Jina
    jina_config = {
        "api_key_env_var": "JINA_API_KEY",
        "model_name": "jina-embeddings-v2-base-en",
    }
    validate_config(jina_config, "jina")

    # Test ONNX MiniLM L6 V2
    onnx_config = {"preferred_providers": ["onnxruntime"]}
    validate_config(onnx_config, "onnx_mini_lm_l6_v2")

    # Test OpenCLIP
    open_clip_config = {
        "model_name": "ViT-B-32",
        "checkpoint": "laion2b_s34b_b79k",
        "device": "cpu",
    }
    validate_config(open_clip_config, "open_clip")


def test_validate_config_with_invalid_configs() -> None:
    """Test validate_config with invalid configurations"""
    # Test OpenAI - missing required field
    openai_config: Dict[str, str] = {"model_name": "text-embedding-ada-002"}
    with pytest.raises(ValidationError):
        validate_config(openai_config, "openai")

    # Test HuggingFace - missing required field
    huggingface_config: Dict[str, Any] = {}
    with pytest.raises(ValidationError):
        validate_config(huggingface_config, "huggingface")

    # Test Google PaLM - invalid type
    google_palm_config = {
        "api_key_env_var": 123,  # Should be string
        "model_name": "models/embedding-gecko-001",
    }
    with pytest.raises(ValidationError):
        validate_config(google_palm_config, "google_palm")

    # Test Google Generative AI - additional property
    google_generative_ai_config = {
        "api_key_env_var": "GOOGLE_API_KEY",
        "model_name": "models/embedding-001",
        "task_type": "RETRIEVAL_DOCUMENT",
        "invalid_property": "value",  # Not allowed
    }
    with pytest.raises(ValidationError):
        validate_config(google_generative_ai_config, "google_generative_ai")

    # Test Google Vertex - missing required field
    google_vertex_config = {
        "api_key_env_var": "GOOGLE_API_KEY",
        "model_name": "textembedding-gecko",
        "region": "us-central1"
        # Missing project_id
    }
    with pytest.raises(ValidationError):
        validate_config(google_vertex_config, "google_vertex")

    # Test Ollama - missing required field
    ollama_config = {
        "url": "http://localhost:11434"
        # Missing model_name
    }
    with pytest.raises(ValidationError):
        validate_config(ollama_config, "ollama")


def test_embedding_function_validate_config_methods() -> None:
    """Test the validate_config methods of embedding functions"""
    # Test OpenAI
    openai_ef = OpenAIEmbeddingFunction(api_key="dummy")
    openai_valid_config: Dict[str, str] = {
        "api_key_env_var": "OPENAI_API_KEY",
        "model_name": "text-embedding-ada-002",
    }
    openai_ef.validate_config(openai_valid_config)  # Should not raise

    openai_invalid_config: Dict[str, str] = {
        "model_name": "text-embedding-ada-002"
        # Missing api_key_env_var
    }
    with pytest.raises(ValidationError):
        openai_ef.validate_config(openai_invalid_config)

    # Test HuggingFace
    huggingface_ef = HuggingFaceEmbeddingFunction(
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        api_key_env_var="HUGGINGFACE_API_KEY",
    )
    huggingface_valid_config: Dict[str, str] = {
        "model_name": "sentence-transformers/all-MiniLM-L6-v2",
        "api_key_env_var": "HUGGINGFACE_API_KEY",
    }
    huggingface_ef.validate_config(huggingface_valid_config)  # Should not raise

    huggingface_invalid_config: Dict[str, str] = {"invalid_property": "value"}
    with pytest.raises(ValidationError):
        huggingface_ef.validate_config(huggingface_invalid_config)

    # Test Ollama
    ollama_valid_config: Dict[str, Any] = {
        "url": "http://localhost:11434",
        "model_name": "llama2",
        "timeout": 60,
    }
    OllamaEmbeddingFunction.build_from_config(ollama_valid_config).validate_config(
        ollama_valid_config
    )

    # Test Jina
    jina_valid_config: Dict[str, str] = {
        "api_key_env_var": "JINA_API_KEY",
        "model_name": "jina-embeddings-v2-base-en",
    }
    JinaEmbeddingFunction.build_from_config(jina_valid_config).validate_config(
        jina_valid_config
    )

    # Test ONNX MiniLM L6 V2
    onnx_valid_config: Dict[str, List[str]] = {"preferred_providers": ["onnxruntime"]}
    ONNXMiniLM_L6_V2.build_from_config(onnx_valid_config).validate_config(
        onnx_valid_config
    )

    # Test OpenCLIP
    openclip_valid_config: Dict[str, Any] = {
        "model_name": "ViT-B-32",
        "checkpoint": "laion2b_s34b_b79k",
        "device": "cpu",
    }
    OpenCLIPEmbeddingFunction.build_from_config(openclip_valid_config).validate_config(
        openclip_valid_config
    )


def test_schema_required_fields() -> None:
    """Test that schemas enforce required fields"""
    schema_names = get_all_schema_names()
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
    schema_names = get_all_schema_names()
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
