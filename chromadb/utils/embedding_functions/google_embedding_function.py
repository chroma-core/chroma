from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, cast, Optional
import os
import numpy as np
import numpy.typing as npt
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import warnings


class GoogleGeminiEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the google-genai Python package installed and have a Gemini API key."""

    def __init__(
        self,
        model_name: str = "gemini-embedding-001",
        task_type: Optional[str] = None,
        dimension: Optional[int] = None,
        api_key_env_var: str = "GEMINI_API_KEY",
        vertexai: Optional[bool] = None,
        project: Optional[str] = None,
        location: Optional[str] = None,
    ):
        """
        Initialize the GoogleGeminiEmbeddingFunction.

        Args:
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "gemini-embedding-001".
            task_type (str, optional): The task type for the embeddings.
                Valid values include SEMANTIC_SIMILARITY, CLASSIFICATION, CLUSTERING,
                RETRIEVAL_DOCUMENT, RETRIEVAL_QUERY, CODE_RETRIEVAL_QUERY,
                QUESTION_ANSWERING, FACT_VERIFICATION.
            dimension (int, optional): The output dimensionality for the embeddings.
                Supported range: 128–3072. If None, the model's default is used.
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "GEMINI_API_KEY".
            vertexai (bool, optional): Whether to use Vertex AI.
            project (str, optional): The Google Cloud project ID (required for Vertex AI).
            location (str, optional): The Google Cloud location/region (required for Vertex AI).
        """
        try:
            import google.genai as genai
        except ImportError:
            raise ValueError(
                "The google-genai python package is not installed. Please install it with `pip install google-genai`"
            )

        self.model_name = model_name
        self.task_type = task_type
        self.dimension = dimension
        self.api_key_env_var = api_key_env_var
        self.vertexai = vertexai
        self.project = project
        self.location = location
        self.api_key = os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.client = genai.Client(
            api_key=self.api_key, vertexai=vertexai, project=project, location=location
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        if not input:
            raise ValueError("Input documents cannot be empty")
        if not isinstance(input, (list, tuple)):
            raise ValueError("Input must be a list or tuple of documents")
        if not all(isinstance(doc, str) for doc in input):
            raise ValueError("All input documents must be strings")

        from google.genai.types import EmbedContentConfig

        config = EmbedContentConfig(
            task_type=self.task_type,
            output_dimensionality=self.dimension,
        )

        try:
            response = self.client.models.embed_content(
                model=self.model_name,
                contents=input,
                config=config,
            )
        except Exception as e:
            raise ValueError(f"Failed to generate embeddings: {str(e)}") from e

        # Validate response structure
        if not hasattr(response, "embeddings") or not response.embeddings:
            raise ValueError("No embeddings returned from the API")

        embeddings_list = []
        for ce in response.embeddings:
            if not hasattr(ce, "values"):
                raise ValueError("Malformed embedding response: missing 'values'")
            embeddings_list.append(np.array(ce.values, dtype=np.float32))

        return cast(Embeddings, embeddings_list)

    @staticmethod
    def name() -> str:
        return "google_gemini"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        task_type = config.get("task_type")
        dimension = config.get("dimension")
        api_key_env_var = config.get("api_key_env_var", "GEMINI_API_KEY")
        vertexai = config.get("vertexai")
        project = config.get("project")
        location = config.get("location")

        if model_name is None:
            raise ValueError("The model name is required.")

        return GoogleGeminiEmbeddingFunction(
            model_name=model_name,
            task_type=task_type,
            dimension=dimension,
            api_key_env_var=api_key_env_var,
            vertexai=vertexai,
            project=project,
            location=location,
        )

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "model_name": self.model_name,
            "api_key_env_var": self.api_key_env_var,
            "vertexai": self.vertexai,
            "project": self.project,
            "location": self.location,
        }
        if self.task_type is not None:
            config["task_type"] = self.task_type
        if self.dimension is not None:
            config["dimension"] = self.dimension
        return config

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        if "dimension" in new_config:
            raise ValueError(
                "The dimension cannot be changed after the embedding function has been initialized."
            )
        if "vertexai" in new_config:
            raise ValueError(
                "The vertexai cannot be changed after the embedding function has been initialized."
            )
        if "project" in new_config:
            raise ValueError(
                "The project cannot be changed after the embedding function has been initialized."
            )
        if "location" in new_config:
            raise ValueError(
                "The location cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "google_gemini")


# Backward compatibility alias
GoogleGenaiEmbeddingFunction = GoogleGeminiEmbeddingFunction


class GoogleGenerativeAiEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the google.generativeai Python package installed and have a Google API key."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "gemini-embedding-001",
        task_type: str = "RETRIEVAL_DOCUMENT",
        api_key_env_var: str = "GEMINI_API_KEY",
        dimension: Optional[int] = None,
    ):
        """
        Initialize the GoogleGenerativeAiEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Google Generative AI API.
                Defaults to "GEMINI_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "gemini-embedding-001".
            task_type (str, optional): The task type for the embeddings.
                Use "RETRIEVAL_DOCUMENT" for embedding documents and "RETRIEVAL_QUERY" for embedding queries.
                Defaults to "RETRIEVAL_DOCUMENT".
            dimension (int, optional): The output dimensionality for the embeddings.
                If None, the model's default dimensionality is used.
        """
        try:
            import google.generativeai as genai
        except ImportError:
            raise ValueError(
                "The Google Generative AI python package is not installed. Please install it with `pip install google-generativeai`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )
        if os.getenv("GOOGLE_API_KEY") is not None:
            self.api_key_env_var = "GOOGLE_API_KEY"
        else:
            self.api_key_env_var = api_key_env_var

        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.model_name = model_name
        self.task_type = task_type
        self.dimension = dimension

        genai.configure(api_key=self.api_key)
        self._genai = genai

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        if not all(isinstance(item, str) for item in input):
            raise ValueError(
                "Google Generative AI only supports text documents, not images"
            )

        embeddings_list: List[npt.NDArray[np.float32]] = []
        for text in input:
            kwargs: Dict[str, Any] = {
                "model": self.model_name,
                "content": text,
                "task_type": self.task_type,
            }
            if self.dimension is not None:
                kwargs["output_dimensionality"] = self.dimension
            embedding_result = self._genai.embed_content(**kwargs)
            embeddings_list.append(
                np.array(embedding_result["embedding"], dtype=np.float32)
            )

        return cast(Embeddings, embeddings_list)

    @staticmethod
    def name() -> str:
        return "google_generative_ai"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        task_type = config.get("task_type")
        dimension = config.get("dimension")

        if api_key_env_var is None or model_name is None or task_type is None:
            assert False, "This code should not be reached"

        return GoogleGenerativeAiEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            task_type=task_type,
            dimension=dimension,
        )

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "task_type": self.task_type,
        }
        if self.dimension is not None:
            config["dimension"] = self.dimension
        return config

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        if "task_type" in new_config:
            raise ValueError(
                "The task type cannot be changed after the embedding function has been initialized."
            )
        if "dimension" in new_config:
            raise ValueError(
                "The dimension cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "google_generative_ai")


class GooglePalmEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the google.generativeai Python package installed and have a PaLM API key."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "models/embedding-gecko-001",
        api_key_env_var: str = "CHROMA_GOOGLE_PALM_API_KEY",
    ):
        """
        Initialize the GooglePalmEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Google PaLM API.
                Defaults to "CHROMA_GOOGLE_PALM_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "models/embedding-gecko-001".
        """
        try:
            import google.generativeai as palm
        except ImportError:
            raise ValueError(
                "The Google Generative AI python package is not installed. Please install it with `pip install google-generativeai`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )
        if os.getenv("GOOGLE_API_KEY") is not None:
            self.api_key_env_var = "GOOGLE_API_KEY"
        else:
            self.api_key_env_var = api_key_env_var

        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.model_name = model_name

        palm.configure(api_key=self.api_key)
        self._palm = palm

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Google PaLM only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Google PaLM only supports text documents, not images")

        return [
            np.array(
                self._palm.generate_embeddings(model=self.model_name, text=text)[
                    "embedding"
                ],
                dtype=np.float32,
            )
            for text in input
        ]

    @staticmethod
    def name() -> str:
        return "google_palm"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return GooglePalmEmbeddingFunction(
            api_key_env_var=api_key_env_var, model_name=model_name
        )

    def get_config(self) -> Dict[str, Any]:
        return {"api_key_env_var": self.api_key_env_var, "model_name": self.model_name}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "google_palm")


class GoogleVertexEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the vertexai Python package installed and have Google Cloud credentials configured."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "textembedding-gecko",
        project_id: str = "cloud-large-language-models",
        region: str = "us-central1",
        api_key_env_var: str = "CHROMA_GOOGLE_VERTEX_API_KEY",
    ):
        """
        Initialize the GoogleVertexEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Google Vertex AI API.
                Defaults to "CHROMA_GOOGLE_VERTEX_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "textembedding-gecko".
            project_id (str, optional): The Google Cloud project ID.
                Defaults to "cloud-large-language-models".
            region (str, optional): The Google Cloud region.
                Defaults to "us-central1".
        """
        try:
            import vertexai
            from vertexai.language_models import TextEmbeddingModel
        except ImportError:
            raise ValueError(
                "The vertexai python package is not installed. Please install it with `pip install google-cloud-aiplatform`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )
        if os.getenv("GOOGLE_API_KEY") is not None:
            self.api_key_env_var = "GOOGLE_API_KEY"
        else:
            self.api_key_env_var = api_key_env_var

        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.model_name = model_name
        self.project_id = project_id
        self.region = region

        vertexai.init(project=project_id, location=region)
        self._model = TextEmbeddingModel.from_pretrained(model_name)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Google Vertex only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Google Vertex only supports text documents, not images")

        embeddings_list: List[npt.NDArray[np.float32]] = []
        for text in input:
            embedding_result = self._model.get_embeddings([text])
            embeddings_list.append(
                np.array(embedding_result[0].values, dtype=np.float32)
            )

        # Convert to the expected Embeddings type (List[Vector])
        return cast(Embeddings, embeddings_list)

    @staticmethod
    def name() -> str:
        return "google_vertex"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        project_id = config.get("project_id")
        region = config.get("region")

        if (
            api_key_env_var is None
            or model_name is None
            or project_id is None
            or region is None
        ):
            assert False, "This code should not be reached"

        return GoogleVertexEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            project_id=project_id,
            region=region,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "project_id": self.project_id,
            "region": self.region,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        if "project_id" in new_config:
            raise ValueError(
                "The project ID cannot be changed after the embedding function has been initialized."
            )
        if "region" in new_config:
            raise ValueError(
                "The region cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "google_vertex")
