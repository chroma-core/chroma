from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import Any, Dict, List, Optional
import os
import numpy as np


class OCIGenAIEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    Generate embeddings with Oracle Cloud Infrastructure (OCI) Generative AI.

    This embedding function relies on the ``oci`` python package, which you can
    install with ``pip install oci``. It calls the Generative AI inference
    ``EmbedText`` API with an on-demand serving mode, so any embedding model
    listed for your region (for example ``cohere.embed-v4.0`` or
    ``cohere.embed-multilingual-v3.0``) can be used.

    Credentials are never stored in the collection configuration. Only the
    *location* of the credentials is persisted (the config file path, the
    profile name and the authentication type), mirroring how the Amazon
    Bedrock embedding function persists boto3 session arguments.
    """

    SUPPORTED_AUTH_TYPES = (
        "API_KEY",
        "SECURITY_TOKEN",
        "INSTANCE_PRINCIPAL",
        "RESOURCE_PRINCIPAL",
    )
    SUPPORTED_TRUNCATE = ("NONE", "START", "END")
    SUPPORTED_INPUT_TYPES = (
        "SEARCH_DOCUMENT",
        "SEARCH_QUERY",
        "CLASSIFICATION",
        "CLUSTERING",
        "IMAGE",
    )
    # The OCI Generative AI EmbedText API rejects requests with more than 96 inputs.
    MAX_BATCH_SIZE = 96

    def __init__(
        self,
        model_name: str = "cohere.embed-v4.0",
        compartment_id: Optional[str] = None,
        service_endpoint: Optional[str] = None,
        auth_type: str = "API_KEY",
        auth_profile: str = "DEFAULT",
        auth_file_location: str = "~/.oci/config",
        truncate: str = "END",
        input_type: Optional[str] = None,
        output_dimensions: Optional[int] = None,
    ):
        """Initialize OCIGenAIEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the OCI Generative AI embedding
                model. Defaults to "cohere.embed-v4.0".
            compartment_id (str): OCID of the compartment used to call the service.
                Required.
            service_endpoint (str, optional): Inference endpoint, e.g.
                "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com".
                When omitted, the endpoint is derived from the region of the OCI
                profile (or of the instance/resource principal).
            auth_type (str, optional): One of "API_KEY", "SECURITY_TOKEN",
                "INSTANCE_PRINCIPAL" or "RESOURCE_PRINCIPAL". Defaults to "API_KEY".
            auth_profile (str, optional): Profile name in the OCI config file, used by
                the API_KEY and SECURITY_TOKEN auth types. Defaults to "DEFAULT".
            auth_file_location (str, optional): Path of the OCI config file, used by
                the API_KEY and SECURITY_TOKEN auth types. Defaults to "~/.oci/config".
            truncate (str, optional): How inputs longer than the model's context are
                truncated: "NONE", "START" or "END". Defaults to "END".
            input_type (str, optional): Force a single input type for every request
                ("SEARCH_DOCUMENT", "SEARCH_QUERY", "CLASSIFICATION", "CLUSTERING" or
                "IMAGE"). When omitted, documents are embedded with "SEARCH_DOCUMENT"
                and queries with "SEARCH_QUERY", which is what the Cohere embed
                models are trained for.
            output_dimensions (int, optional): Requested embedding size for models that
                support it (e.g. 256, 512, 1024 or 1536 for cohere.embed-v4.0).

        Example:
            >>> ef = OCIGenAIEmbeddingFunction(
            ...     model_name="cohere.embed-v4.0",
            ...     compartment_id="ocid1.compartment.oc1..<your-compartment>",
            ...     service_endpoint="https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
            ... )
            >>> embeddings = ef(["Hello, world!", "How are you?"])
        """
        try:
            import oci
        except ImportError:
            raise ValueError(
                "The oci python package is not installed. Please install it with `pip install oci`"
            )

        if not compartment_id:
            raise ValueError(
                "compartment_id is required: pass the OCID of the compartment that is "
                "authorized to call the OCI Generative AI service."
            )

        auth_type = auth_type.upper()
        if auth_type not in self.SUPPORTED_AUTH_TYPES:
            raise ValueError(
                f"Unsupported auth_type '{auth_type}'. Expected one of "
                f"{', '.join(self.SUPPORTED_AUTH_TYPES)}."
            )

        truncate = truncate.upper()
        if truncate not in self.SUPPORTED_TRUNCATE:
            raise ValueError(
                f"Unsupported truncate '{truncate}'. Expected one of "
                f"{', '.join(self.SUPPORTED_TRUNCATE)}."
            )

        if input_type is not None:
            input_type = input_type.upper()
            if input_type not in self.SUPPORTED_INPUT_TYPES:
                raise ValueError(
                    f"Unsupported input_type '{input_type}'. Expected one of "
                    f"{', '.join(self.SUPPORTED_INPUT_TYPES)}."
                )

        if output_dimensions is not None and output_dimensions <= 0:
            raise ValueError("output_dimensions must be a positive integer.")

        self.model_name = model_name
        self.compartment_id = compartment_id
        self.service_endpoint = service_endpoint
        self.auth_type = auth_type
        self.auth_profile = auth_profile
        self.auth_file_location = auth_file_location
        self.truncate = truncate
        self.input_type = input_type
        self.output_dimensions = output_dimensions

        self._models = oci.generative_ai_inference.models
        self._client = self._build_client(oci)

    def _build_client(self, oci: Any) -> Any:
        """Create a GenerativeAiInferenceClient for the configured auth type."""
        client_kwargs: Dict[str, Any] = {
            "retry_strategy": oci.retry.DEFAULT_RETRY_STRATEGY,
        }
        if self.service_endpoint:
            client_kwargs["service_endpoint"] = self.service_endpoint

        config: Dict[str, Any]
        if self.auth_type in ("API_KEY", "SECURITY_TOKEN"):
            config = oci.config.from_file(
                file_location=self.auth_file_location,
                profile_name=self.auth_profile,
            )
            if self.auth_type == "SECURITY_TOKEN":
                token_file = config.get("security_token_file")
                if not token_file:
                    raise ValueError(
                        f"Profile '{self.auth_profile}' in {self.auth_file_location} has no "
                        "security_token_file. Run `oci session authenticate` to create a "
                        "session-token profile, or use auth_type='API_KEY'."
                    )
                with open(os.path.expanduser(token_file), "r") as f:
                    token = f.read().strip()
                private_key = oci.signer.load_private_key_from_file(
                    os.path.expanduser(config["key_file"]), config.get("pass_phrase")
                )
                client_kwargs["signer"] = oci.auth.signers.SecurityTokenSigner(
                    token, private_key
                )
        elif self.auth_type == "INSTANCE_PRINCIPAL":
            config = {}
            client_kwargs[
                "signer"
            ] = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        else:  # RESOURCE_PRINCIPAL
            config = {}
            client_kwargs["signer"] = oci.auth.signers.get_resource_principals_signer()

        return oci.generative_ai_inference.GenerativeAiInferenceClient(
            config, **client_kwargs
        )

    def _embed(self, input: Documents, input_type: str) -> Embeddings:
        embeddings: Embeddings = []
        texts = list(input)
        for start in range(0, len(texts), self.MAX_BATCH_SIZE):
            batch = texts[start : start + self.MAX_BATCH_SIZE]
            details = self._models.EmbedTextDetails(
                inputs=batch,
                serving_mode=self._models.OnDemandServingMode(model_id=self.model_name),
                compartment_id=self.compartment_id,
                truncate=self.truncate,
                input_type=input_type,
            )
            if self.output_dimensions is not None:
                details.output_dimensions = self.output_dimensions
            response = self._client.embed_text(details)
            embeddings.extend(
                np.array(embedding, dtype=np.float32)
                for embedding in response.data.embeddings
            )
        return embeddings

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        return self._embed(input, self.input_type or "SEARCH_DOCUMENT")

    def embed_query(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for search queries.

        Unless ``input_type`` was pinned in the constructor, queries are embedded
        with the ``SEARCH_QUERY`` input type while documents use ``SEARCH_DOCUMENT``.
        """
        return self._embed(input, self.input_type or "SEARCH_QUERY")

    @staticmethod
    def name() -> str:
        return "oci_genai"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        compartment_id = config.get("compartment_id")
        if model_name is None or compartment_id is None:
            raise ValueError(
                "OCI Generative AI embedding function config requires 'model_name' and "
                "'compartment_id'."
            )

        return OCIGenAIEmbeddingFunction(
            model_name=model_name,
            compartment_id=compartment_id,
            service_endpoint=config.get("service_endpoint"),
            auth_type=config.get("auth_type", "API_KEY"),
            auth_profile=config.get("auth_profile", "DEFAULT"),
            auth_file_location=config.get("auth_file_location", "~/.oci/config"),
            truncate=config.get("truncate", "END"),
            input_type=config.get("input_type"),
            output_dimensions=config.get("output_dimensions"),
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "compartment_id": self.compartment_id,
            "service_endpoint": self.service_endpoint,
            "auth_type": self.auth_type,
            "auth_profile": self.auth_profile,
            "auth_file_location": self.auth_file_location,
            "truncate": self.truncate,
            "input_type": self.input_type,
            "output_dimensions": self.output_dimensions,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        if "output_dimensions" in new_config:
            raise ValueError(
                "The output dimensions cannot be changed after the embedding function has been initialized."
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
        validate_config_schema(config, "oci_genai")
