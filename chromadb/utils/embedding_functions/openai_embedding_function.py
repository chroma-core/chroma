import logging
from typing import Mapping, Optional, cast

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class OpenAIEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "text-embedding-ada-002",
        organization_id: Optional[str] = None,
        api_base: Optional[str] = None,
        api_type: Optional[str] = None,
        api_version: Optional[str] = None,
        deployment_id: Optional[str] = None,
        default_headers: Optional[Mapping[str, str]] = None,
        dimensions: Optional[int] = None,
    ):
        """
        Initialize the OpenAIEmbeddingFunction.
        Args:
            api_key (str, optional): Your API key for the OpenAI API. If not
                provided, it will raise an error to provide an OpenAI API key.
            organization_id(str, optional): The OpenAI organization ID if applicable
            model_name (str, optional): The name of the model to use for text
                embeddings. Defaults to "text-embedding-ada-002".
            api_base (str, optional): The base path for the API. If not provided,
                it will use the base path for the OpenAI API. This can be used to
                point to a different deployment, such as an Azure deployment.
            api_type (str, optional): The type of the API deployment. This can be
                used to specify a different deployment, such as 'azure'. If not
                provided, it will use the default OpenAI deployment.
            api_version (str, optional): The api version for the API. If not provided,
                it will use the api version for the OpenAI API. This can be used to
                point to a different deployment, such as an Azure deployment.
            deployment_id (str, optional): Deployment ID for Azure OpenAI.
            default_headers (Mapping, optional): A mapping of default headers to be sent with each API request.
            dimensions (int, optional): The number of dimensions for the embeddings.
                Only supported for `text-embedding-3` or later models from OpenAI.
                https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions

        """
        try:
            import openai
        except ImportError:
            raise ValueError(
                "The openai python package is not installed. Please install it with `pip install openai`"
            )

        self._api_key = api_key or openai.api_key
        # If the api key is still not set, raise an error
        if self._api_key is None:
            raise ValueError(
                "Please provide an OpenAI API key. You can get one at https://platform.openai.com/account/api-keys"
            )

        if api_base is not None:
            openai.api_base = api_base

        if api_version is not None:
            openai.api_version = api_version

        self._api_type = api_type
        if api_type is not None:
            openai.api_type = api_type

        if organization_id is not None:
            openai.organization = organization_id

        self._v1 = openai.__version__.startswith("1.")
        if self._v1:
            if api_type == "azure":
                self._client = openai.AzureOpenAI(
                    api_key=api_key,
                    api_version=api_version,
                    azure_endpoint=api_base,
                    default_headers=default_headers,
                ).embeddings
            else:
                self._client = openai.OpenAI(
                    api_key=api_key, base_url=api_base, default_headers=default_headers
                ).embeddings
        else:
            self._client = openai.Embedding
        self._model_name = model_name
        self._deployment_id = deployment_id
        self._dimensions = dimensions or openai.NOT_GIVEN

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate the embeddings for the given `input`.

        # About ignoring types
        We are not enforcing the openai library, therefore, `mypy` has hard times trying
        to figure out what the types are for `self._client.create()` which throws an
        error when trying to sort the list. If, eventually we include the `openai` lib
        we can remove the type ignore tag.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the given input sorted by index
        """
        # replace newlines, which can negatively affect performance.
        input = [t.replace("\n", " ") for t in input]

        # Call the OpenAI Embedding API
        if self._v1:
            embeddings = self._client.create(
                input=input,
                model=self._deployment_id or self._model_name,
                dimensions=self._dimensions,
            ).data

            # Sort resulting embeddings by index
            sorted_embeddings = sorted(
                embeddings, key=lambda e: e.index  # type: ignore
            )

            # Return just the embeddings
            return cast(Embeddings, [result.embedding for result in sorted_embeddings])
        else:
            if self._api_type == "azure":
                embeddings = self._client.create(
                    input=input, engine=self._deployment_id or self._model_name
                )["data"]
            else:
                embeddings = self._client.create(input=input, model=self._model_name)[
                    "data"
                ]

            # Sort resulting embeddings by index
            sorted_embeddings = sorted(
                embeddings, key=lambda e: e["index"]  # type: ignore
            )

            # Return just the embeddings
            return cast(
                Embeddings, [result["embedding"] for result in sorted_embeddings]
            )
