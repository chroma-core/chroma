from chromadb.api.types import Embeddings, Documents, EmbeddingFunction
from typing import List, Dict, Any, Optional, Union
from chromadb.utils.embedding_functions.schemas import validate_config_schema

import os
import numpy as np
import warnings

class RunPodEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        endpoint_id: str,
        model_name: str,
        api_key_env_var: str = "RUNPOD_API_KEY",
        api_key: Optional[str] = None,
        timeout: Optional[int] = 300,
    ):
        """
        Initialize the RunPodEmbeddingFunction.
        
        Args:
            endpoint_id (str): The RunPod endpoint ID for your embedding model
            model_name (str): The name of the model to use for embeddings.
            api_key (str, optional): The RunPod API key. If not provided, will use environment variable.
            timeout (int, optional): Timeout in seconds for API requests. Defaults to 300.
        """
        try:
            import runpod
        except ImportError:
            raise ValueError(
                "The runpod python package is not installed. Please install it with `pip install runpod`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.endpoint_id = endpoint_id
        self.model_name = model_name
        self.timeout = timeout
        self._runpod = runpod  # Store module reference for later use

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Handle empty input
        if not input:
            return []

        # Ensure input is a list
        if isinstance(input, str):
            input = [input]

        # Set API key before each call to avoid multi-tenant conflicts
        self._runpod.api_key = self.api_key
        endpoint = self._runpod.Endpoint(self.endpoint_id)

        # Submit all requests in parallel first
        run_requests = []
        for document in input:
            input_payload = {
                "input": {
                    "model": self.model_name,
                    "input": document
                }
            }
            run_request = endpoint.run(input_payload)
            run_requests.append(run_request)

        # Collect results
        embeddings = []
        for i, run_request in enumerate(run_requests):
            try:
                status = run_request.status()

                if status in ("FAILED", "CANCELLED", "TIMED_OUT"):
                    raise RuntimeError(f"RunPod endpoint failed with status '{status}': {run_request}")
                elif status == "COMPLETED":
                    output = run_request.output()
                else:
                    output = run_request.output(timeout=self.timeout)

                if output and "data" in output:
                    data_list = output["data"]
                    if len(data_list) > 0 and "embedding" in data_list[0]:
                        embedding = data_list[0]["embedding"]
                    else:
                        raise ValueError(f"No embedding found in response data: {data_list}")
                else:
                    raise ValueError(f"Unexpected output format. Expected 'output.data[0].embedding', got: {output}")

                embeddings.append(np.array(embedding, dtype=np.float32))

            except Exception as e:
                raise RuntimeError(f"Failed to generate embedding for document {i}: {e}")

        return embeddings

    @staticmethod
    def name() -> str:
        """Return the name of this embedding function."""
        return "runpod"

    def default_space(self) -> str:
        """Return the default space for this embedding function."""
        # Most embedding models work best with cosine similarity
        return "cosine"
    
    def supported_spaces(self) -> List[str]:
        """Return the supported spaces for this embedding function."""
        return ["cosine", "l2", "ip"]
    

    def list_available_models(self) -> List[str]:
        """Return the list of models supported by this embedding endpoint."""
        # Set API key before call to avoid multi-tenant conflicts
        self._runpod.api_key = self.api_key
        endpoint = self._runpod.Endpoint(self.endpoint_id)

        models = []
        try:
            run_request = endpoint.run({ "input": { "openai_route": "/v1/models" } })
            status = run_request.status()

            # Get output with timeout handling
            if status != "COMPLETED":
                output = run_request.output(timeout=self.timeout)
            else:
                output = run_request.output()

            if output and "data" in output:
                models = [model["id"] for model in output["data"]]
            else:
                raise ValueError(f"Unexpected output format. Got: {output}")
        except Exception as e:
            raise RuntimeError(f"Failed to list models: {e}")

        return models
        
    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        """Build the embedding function from a configuration dictionary."""
        endpoint_id = config.get("endpoint_id")
        model_name = config.get("model_name")
        api_key_env_var = config.get("api_key_env_var", "RUNPOD_API_KEY")
        timeout = config.get("timeout", 300)

        if endpoint_id is None or model_name is None:
            raise ValueError("endpoint_id and model_name are required in config")

        return RunPodEmbeddingFunction(
            endpoint_id=endpoint_id,
            model_name=model_name,
            api_key_env_var=api_key_env_var,
            timeout=timeout,
        )

    def get_config(self) -> Dict[str, Any]:
        """Get the configuration for this embedding function."""
        return {
            "endpoint_id": self.endpoint_id,
            "model_name": self.model_name,
            "api_key_env_var": self.api_key_env_var,
            "timeout": self.timeout,
            # Note: We don't include api_key for security reasons
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """Validate that a configuration update is allowed."""
        # Prevent changing the model name as it could affect embedding compatibility
        if "model_name" in new_config and new_config["model_name"] != old_config.get("model_name"):
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        
        # Prevent changing the endpoint_id as it could affect embedding compatibility
        if "endpoint_id" in new_config and new_config["endpoint_id"] != old_config.get("endpoint_id"):
            raise ValueError(
                "The endpoint_id cannot be changed after the embedding function has been initialized."
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
        validate_config_schema(config, "runpod")