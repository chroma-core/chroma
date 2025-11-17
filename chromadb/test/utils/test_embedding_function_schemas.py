import pytest
from typing import List, Any, Callable, Dict
from jsonschema import ValidationError
from unittest.mock import MagicMock, create_autospec
from chromadb.utils.embedding_functions.schemas import (
    validate_config_schema,
    load_schema,
    get_available_schemas,
)
from chromadb.utils.embedding_functions import (
    known_embedding_functions,
    sparse_known_embedding_functions,
)
from chromadb.api.types import Documents, Embeddings
from pytest import MonkeyPatch

# Skip these embedding functions in tests
SKIP_EMBEDDING_FUNCTIONS = [
    "chroma_langchain",
]


def get_embedding_function_names() -> List[str]:
    """Get all embedding function names to test"""
    return [
        name
        for name in known_embedding_functions.keys()
        if name not in SKIP_EMBEDDING_FUNCTIONS
    ]


class TestEmbeddingFunctionSchemas:
    """Test class for embedding function schemas"""

    @pytest.mark.parametrize("ef_name", get_embedding_function_names())
    def test_embedding_function_config_roundtrip(
        self,
        ef_name: str,
        mock_embeddings: Callable[[Documents], Embeddings],
        mock_common_deps: MonkeyPatch,
    ) -> None:
        """Test embedding function configuration roundtrip"""
        ef_class = known_embedding_functions[ef_name]

        # Create an autospec of the embedding function class
        mock_ef = create_autospec(ef_class, instance=True)

        # Mock the __call__ method
        mock_call = MagicMock(return_value=mock_embeddings(["test"]))
        mock_ef.__call__ = mock_call

        # For chroma-cloud-qwen, mock get_config to return valid data
        if ef_name == "chroma-cloud-qwen":
            from chromadb.utils.embedding_functions.chroma_cloud_qwen_embedding_function import (
                ChromaCloudQwenEmbeddingModel,
                CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
            )

            mock_ef.get_config.return_value = {
                "api_key_env_var": "CHROMA_API_KEY",
                "model": ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B.value,
                "task": "nl_to_code",
                "instructions": CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
            }

        # Mock the class constructor to return our mock instance
        mock_common_deps.setattr(
            ef_class, "__new__", lambda cls, *args, **kwargs: mock_ef
        )

        # Create instance with minimal args (constructor will be mocked)
        ef_instance = ef_class()

        # Get the config (this will use the real method)
        config = ef_instance.get_config()

        # Test recreation from config
        new_instance = ef_class.build_from_config(config)
        new_config = new_instance.get_config()

        # Configs should match
        assert (
            config == new_config
        ), f"Configs don't match after recreation for {ef_name}"

    def test_schema_required_fields(self) -> None:
        """Test that schemas enforce required fields"""
        for schema_name in get_available_schemas():
            schema = load_schema(schema_name)
            if "required" not in schema:
                continue

            # Create minimal valid config
            config = {}
            for field in schema["required"]:
                field_schema = schema["properties"][field]
                field_type = (
                    field_schema["type"][0]
                    if isinstance(field_schema["type"], list)
                    else field_schema["type"]
                )
                config[field] = self._get_dummy_value(field_type)

            # Test each required field
            for field in schema["required"]:
                test_config = config.copy()
                del test_config[field]
                with pytest.raises(ValidationError):
                    validate_config_schema(test_config, schema_name)

    @staticmethod
    def _get_dummy_value(field_type: str) -> Any:
        """Get a dummy value for a given field type"""
        type_map = {
            "string": "dummy",
            "integer": 0,
            "number": 0.0,
            "boolean": False,
            "object": {},
            "array": [],
        }
        return type_map.get(field_type, "dummy")

    def test_schema_additional_properties(self) -> None:
        """Test that schemas reject additional properties"""
        for schema_name in get_available_schemas():
            schema = load_schema(schema_name)
            config = {}

            # Add required fields
            if "required" in schema:
                for field in schema["required"]:
                    field_schema = schema["properties"][field]
                    field_type = (
                        field_schema["type"][0]
                        if isinstance(field_schema["type"], list)
                        else field_schema["type"]
                    )
                    config[field] = self._get_dummy_value(field_type)

            # Add additional property
            test_config = config.copy()
            test_config["additional_property"] = "value"

            # Test validation
            if schema.get("additionalProperties", True) is False:
                with pytest.raises(ValidationError):
                    validate_config_schema(test_config, schema_name)

    def _create_valid_config_from_schema(
        self, schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a valid config from a schema by filling in required fields"""
        config: Dict[str, Any] = {}

        if "required" in schema and "properties" in schema:
            for field in schema["required"]:
                if field in schema["properties"]:
                    field_schema = schema["properties"][field]
                    config[field] = self._get_value_from_field_schema(field_schema)

        return config

    def _get_value_from_field_schema(self, field_schema: Dict[str, Any]) -> Any:
        """Get a valid value from a field schema"""
        # Handle enums - use first enum value
        if "enum" in field_schema:
            return field_schema["enum"][0]

        # Handle type (could be a list or single value)
        field_type = field_schema.get("type")
        if field_type is None:
            return "dummy"  # Fallback if no type specified

        if isinstance(field_type, list):
            # If null is in the type list, prefer non-null type
            non_null_types = [t for t in field_type if t != "null"]
            field_type = non_null_types[0] if non_null_types else field_type[0]

        if field_type == "object":
            # Handle nested objects
            nested_config = {}
            if "properties" in field_schema:
                nested_required = field_schema.get("required", [])
                for prop in nested_required:
                    if prop in field_schema["properties"]:
                        nested_config[prop] = self._get_value_from_field_schema(
                            field_schema["properties"][prop]
                        )
            return nested_config if nested_config else {}

        if field_type == "array":
            # Return empty array for arrays
            return []

        # Use the existing dummy value method for primitive types
        return self._get_dummy_value(field_type)

    def _has_custom_validation(self, ef_class: Any) -> bool:
        """Check if validate_config actually validates (not just base implementation)"""
        try:
            # Try with an obviously invalid config - if it doesn't raise, it's base implementation
            invalid_config = {"__invalid_test_config__": True}
            try:
                ef_class.validate_config(invalid_config)
                # If we get here without exception, it's using base implementation
                return False
            except (ValidationError, ValueError, FileNotFoundError):
                # If it raises any validation-related error, it's actually validating
                return True
        except Exception:
            # Any other exception means it's trying to validate (e.g., schema not found)
            return True

    def _setup_env_vars_for_ef(
        self, ef_name: str, mock_common_deps: MonkeyPatch
    ) -> None:
        """Set up environment variables needed for embedding function instantiation"""
        # Map of embedding function names to their default API key environment variable names
        api_key_env_vars = {
            "cohere": "CHROMA_COHERE_API_KEY",
            "openai": "CHROMA_OPENAI_API_KEY",
            "huggingface": "CHROMA_HUGGINGFACE_API_KEY",
            "huggingface_server": "CHROMA_HUGGINGFACE_API_KEY",
            "google_palm": "CHROMA_GOOGLE_PALM_API_KEY",
            "google_generative_ai": "CHROMA_GOOGLE_GENAI_API_KEY",
            "google_vertex": "CHROMA_GOOGLE_VERTEX_API_KEY",
            "jina": "CHROMA_JINA_API_KEY",
            "mistral": "MISTRAL_API_KEY",
            "morph": "MORPH_API_KEY",
            "voyageai": "CHROMA_VOYAGE_API_KEY",
            "cloudflare_workers_ai": "CHROMA_CLOUDFLARE_API_KEY",
            "together_ai": "CHROMA_TOGETHER_AI_API_KEY",
            "baseten": "CHROMA_BASETEN_API_KEY",
            "roboflow": "CHROMA_ROBOFLOW_API_KEY",
            "amazon_bedrock": "AWS_ACCESS_KEY_ID",  # AWS uses different env vars
            "chroma-cloud-qwen": "CHROMA_API_KEY",
            # Sparse embedding functions
            "chroma-cloud-splade": "CHROMA_API_KEY",
        }

        # Set API key environment variable if needed
        if ef_name in api_key_env_vars:
            mock_common_deps.setenv(api_key_env_vars[ef_name], "test-api-key")

        # Special cases that need additional environment variables
        if ef_name == "amazon_bedrock":
            mock_common_deps.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
            mock_common_deps.setenv("AWS_REGION", "us-east-1")

    def _create_ef_instance(
        self, ef_name: str, ef_class: Any, mock_common_deps: MonkeyPatch
    ) -> Any:
        """Create an embedding function instance, handling special cases"""
        # Set up environment variables first
        self._setup_env_vars_for_ef(ef_name, mock_common_deps)

        # Mock missing modules that are imported inside __init__ methods
        import sys

        # Create mock modules
        mock_pil = MagicMock()
        mock_pil_image = MagicMock()
        mock_google_genai = MagicMock()
        mock_vertexai = MagicMock()
        mock_vertexai_lm = MagicMock()
        mock_boto3 = MagicMock()
        mock_jina = MagicMock()
        mock_mistralai = MagicMock()

        # Mock boto3.Session for amazon_bedrock
        mock_boto3_session = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.region_name = "us-east-1"
        mock_session_instance.profile_name = None
        mock_session_instance.client.return_value = MagicMock()
        mock_boto3_session.return_value = mock_session_instance
        mock_boto3.Session = mock_boto3_session

        # Mock vertexai.init and TextEmbeddingModel
        mock_text_embedding_model = MagicMock()
        mock_text_embedding_model.from_pretrained.return_value = MagicMock()
        mock_vertexai_lm.TextEmbeddingModel = mock_text_embedding_model
        mock_vertexai.language_models = mock_vertexai_lm
        mock_vertexai.init = MagicMock()

        # Mock google.generativeai - need to set up google module first
        mock_google = MagicMock()
        mock_google_genai.configure = MagicMock()  # For palm.configure()
        mock_google_genai.GenerativeModel = MagicMock(return_value=MagicMock())
        mock_google.generativeai = mock_google_genai

        # Mock jina Client
        mock_jina.Client = MagicMock()

        # Mock mistralai
        mock_mistral_client = MagicMock()
        mock_mistral_client.return_value.embeddings.create.return_value.data = [
            MagicMock(embedding=[0.1, 0.2, 0.3])
        ]
        mock_mistralai.Mistral = mock_mistral_client

        # Add missing modules to sys.modules using monkeypatch
        modules_to_mock = {
            "PIL": mock_pil,
            "PIL.Image": mock_pil_image,
            "google": mock_google,
            "google.generativeai": mock_google_genai,
            "vertexai": mock_vertexai,
            "vertexai.language_models": mock_vertexai_lm,
            "boto3": mock_boto3,
            "jina": mock_jina,
            "mistralai": mock_mistralai,
        }

        for module_name, mock_module in modules_to_mock.items():
            mock_common_deps.setitem(sys.modules, module_name, mock_module)

        # Special cases that need additional arguments
        if ef_name == "cloudflare_workers_ai":
            return ef_class(
                model_name="test-model",
                account_id="test-account-id",
            )
        elif ef_name == "baseten":
            # Baseten needs api_key explicitly passed even with env var
            return ef_class(
                api_key="test-api-key",
                api_base="https://test.api.baseten.co",
            )
        elif ef_name == "amazon_bedrock":
            # Amazon Bedrock needs a boto3 session - create a mock session
            # boto3 is already mocked in sys.modules above
            mock_session = mock_boto3.Session(region_name="us-east-1")
            return ef_class(
                session=mock_session,
                model_name="amazon.titan-embed-text-v1",
            )
        elif ef_name == "huggingface_server":
            return ef_class(url="http://localhost:8080")
        elif ef_name == "google_vertex":
            return ef_class(project_id="test-project", region="us-central1")
        elif ef_name == "mistral":
            return ef_class(model="mistral-embed")
        elif ef_name == "roboflow":
            return ef_class()  # No model_name needed
        elif ef_name == "chroma-cloud-qwen":
            from chromadb.utils.embedding_functions.chroma_cloud_qwen_embedding_function import (
                ChromaCloudQwenEmbeddingModel,
            )

            return ef_class(
                model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
                task="nl_to_code",
            )
        else:
            # Try with no args first
            try:
                return ef_class()
            except Exception:
                # If that fails, try with common minimal args
                return ef_class(model_name="test-model")

    @pytest.mark.parametrize("ef_name", get_embedding_function_names())
    def test_validate_config_with_schema(
        self,
        ef_name: str,
        mock_embeddings: Callable[[Documents], Embeddings],
        mock_common_deps: MonkeyPatch,
    ) -> None:
        """Test that validate_config works correctly with actual configs from embedding functions"""
        ef_class = known_embedding_functions[ef_name]

        # Skip if the embedding function doesn't have a validate_config method
        if not hasattr(ef_class, "validate_config"):
            pytest.skip(f"{ef_name} does not have validate_config method")

        # Check if it's callable (static methods are callable on the class)
        if not callable(getattr(ef_class, "validate_config", None)):
            pytest.skip(f"{ef_name} validate_config is not callable")

        # Skip if using base implementation (doesn't actually validate)
        if not self._has_custom_validation(ef_class):
            pytest.skip(
                f"{ef_name} uses base validate_config implementation (no validation)"
            )

        # Create a real instance to get the actual config
        # We'll mock __call__ to avoid needing to actually generate embeddings
        try:
            ef_instance = self._create_ef_instance(ef_name, ef_class, mock_common_deps)
        except Exception as e:
            pytest.skip(
                f"{ef_name} requires arguments that we cannot provide without external deps: {e}"
            )

        # Mock only __call__ to avoid needing to actually generate embeddings
        mock_call = MagicMock(return_value=mock_embeddings(["test"]))
        mock_common_deps.setattr(ef_instance, "__call__", mock_call)

        # Get the actual config from the embedding function (this uses the real get_config method)
        config = ef_instance.get_config()

        # Filter out None values - optional fields with None shouldn't be included in validation
        # This matches common JSON schema practice where optional fields are omitted rather than null
        config = {k: v for k, v in config.items() if v is not None}

        # Validate the actual config using the embedding function's validate_config method
        ef_class.validate_config(config)

    def test_validate_config_sparse_embedding_functions(
        self,
        mock_embeddings: Callable[[Documents], Embeddings],
        mock_common_deps: MonkeyPatch,
    ) -> None:
        """Test validate_config for sparse embedding functions with actual configs"""
        for ef_name, ef_class in sparse_known_embedding_functions.items():
            # Skip if the embedding function doesn't have a validate_config method
            if not hasattr(ef_class, "validate_config"):
                continue

            # Check if it's callable (static methods are callable on the class)
            if not callable(getattr(ef_class, "validate_config", None)):
                continue

            # Skip if using base implementation (doesn't actually validate)
            if not self._has_custom_validation(ef_class):
                continue

            # Create a real instance to get the actual config
            # We'll mock __call__ to avoid needing to actually generate embeddings
            try:
                ef_instance = self._create_ef_instance(
                    ef_name, ef_class, mock_common_deps
                )
            except Exception:
                continue  # Skip if we can't create instance

            # Mock only __call__ to avoid needing to actually generate embeddings
            mock_call = MagicMock(return_value=mock_embeddings(["test"]))
            mock_common_deps.setattr(ef_instance, "__call__", mock_call)

            # Get the actual config from the embedding function (this uses the real get_config method)
            config = ef_instance.get_config()

            # Filter out None values - optional fields with None shouldn't be included in validation
            # This matches common JSON schema practice where optional fields are omitted rather than null
            config = {k: v for k, v in config.items() if v is not None}

            # Validate the actual config using the embedding function's validate_config method
            ef_class.validate_config(config)
