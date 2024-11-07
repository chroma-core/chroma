import logging

import httpx

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from chromadb.errors import InvalidArgumentError


logger = logging.getLogger(__name__)


class GooglePalmEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the google.generativeai Python package installed and have a PaLM API key."""

    def __init__(self, api_key: str, model_name: str = "models/embedding-gecko-001"):
        if not api_key:
            raise InvalidArgumentError("Please provide a PaLM API key.")

        if not model_name:
            raise InvalidArgumentError("Please provide the model name.")

        try:
            import google.generativeai as palm
        except ImportError:
            raise InvalidArgumentError(
                "The Google Generative AI python package is not installed. Please install it with `pip install google-generativeai`"
            )

        palm.configure(api_key=api_key)
        self._palm = palm
        self._model_name = model_name

    def __call__(self, input: Documents) -> Embeddings:
        return [
            self._palm.generate_embeddings(model=self._model_name, text=text)[
                "embedding"
            ]
            for text in input
        ]


class GoogleGenerativeAiEmbeddingFunction(EmbeddingFunction[Documents]):
    """To use this EmbeddingFunction, you must have the google.generativeai Python package installed and have a Google API key."""

    """Use RETRIEVAL_DOCUMENT for the task_type for embedding, and RETRIEVAL_QUERY for the task_type for retrieval."""

    def __init__(
        self,
        api_key: str,
        model_name: str = "models/embedding-001",
        task_type: str = "RETRIEVAL_DOCUMENT",
    ):
        if not api_key:
            raise InvalidArgumentError("Please provide a Google API key.")

        if not model_name:
            raise InvalidArgumentError("Please provide the model name.")

        try:
            import google.generativeai as genai
        except ImportError:
            raise InvalidArgumentError(
                "The Google Generative AI python package is not installed. Please install it with `pip install google-generativeai`"
            )

        genai.configure(api_key=api_key)
        self._genai = genai
        self._model_name = model_name
        self._task_type = task_type
        self._task_title = None
        if self._task_type == "RETRIEVAL_DOCUMENT":
            self._task_title = "Embedding of single string"

    def __call__(self, input: Documents) -> Embeddings:
        return [
            self._genai.embed_content(
                model=self._model_name,
                content=text,
                task_type=self._task_type,
                title=self._task_title,
            )["embedding"]
            for text in input
        ]


class GoogleVertexEmbeddingFunction(EmbeddingFunction[Documents]):
    # Follow API Quickstart for Google Vertex AI
    # https://cloud.google.com/vertex-ai/docs/generative-ai/start/quickstarts/api-quickstart
    # Information about the text embedding modules in Google Vertex AI
    # https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings
    def __init__(
        self,
        api_key: str,
        model_name: str = "textembedding-gecko",
        project_id: str = "cloud-large-language-models",
        region: str = "us-central1",
    ):
        self._api_url = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{region}/publishers/goole/models/{model_name}:predict"
        self._session = httpx.Client()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            response = self._session.post(
                self._api_url, json={"instances": [{"content": text}]}
            ).json()

            if "predictions" in response:
                embeddings.append(response["predictions"]["embeddings"]["values"])

        return embeddings
