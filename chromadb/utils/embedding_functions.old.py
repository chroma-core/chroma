import hashlib
import logging
from functools import cached_property

from tenacity import stop_after_attempt, wait_random, retry, retry_if_exception

from chromadb.api.types import (
    Document,
    Documents,
    Embedding,
    Image,
    Images,
    EmbeddingFunction,
    Embeddings,
    is_image,
    is_document,
)

from io import BytesIO
from pathlib import Path
import os
import tarfile
import requests
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union, cast
import numpy as np
import numpy.typing as npt
import importlib
import inspect
import json
import sys
import base64

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False


logger = logging.getLogger(__name__)


class SentenceTransformerEmbeddingFunction(EmbeddingFunction[Documents]):
    # Since we do dynamic imports we have to type this as Any
    models: Dict[str, Any] = {}

    # If you have a beefier machine, try "gtr-t5-large".
    # for a full list of options: https://huggingface.co/sentence-transformers, https://www.sbert.net/docs/pretrained_models.html
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        device: str = "cpu",
        normalize_embeddings: bool = False,
        **kwargs: Any,
    ):
        """Initialize SentenceTransformerEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the SentenceTransformer model, defaults to "all-MiniLM-L6-v2"
            device (str, optional): Device used for computation, defaults to "cpu"
            normalize_embeddings (bool, optional): Whether to normalize returned vectors, defaults to False
            **kwargs: Additional arguments to pass to the SentenceTransformer model.
        """
        if model_name not in self.models:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ValueError(
                    "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
                )
            self.models[model_name] = SentenceTransformer(
                model_name, device=device, **kwargs
            )
        self._model = self.models[model_name]
        self._normalize_embeddings = normalize_embeddings

    def __call__(self, input: Documents) -> Embeddings:
        return cast(
            Embeddings,
            self._model.encode(
                list(input),
                convert_to_numpy=True,
                normalize_embeddings=self._normalize_embeddings,
            ).tolist(),
        )


class Text2VecEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, model_name: str = "shibing624/text2vec-base-chinese"):
        try:
            from text2vec import SentenceModel
        except ImportError:
            raise ValueError(
                "The text2vec python package is not installed. Please install it with `pip install text2vec`"
            )
        self._model = SentenceModel(model_name_or_path=model_name)

    def __call__(self, input: Documents) -> Embeddings:
        return cast(
            Embeddings, self._model.encode(list(input), convert_to_numpy=True).tolist()
        )  # noqa E501


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

        """
        try:
            import openai
        except ImportError:
            raise ValueError(
                "The openai python package is not installed. Please install it with `pip install openai`"
            )

        if api_key is not None:
            openai.api_key = api_key
        # If the api key is still not set, raise an error
        elif openai.api_key is None:
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

    def __call__(self, input: Documents) -> Embeddings:
        # replace newlines, which can negatively affect performance.
        input = [t.replace("\n", " ") for t in input]

        # Call the OpenAI Embedding API
        if self._v1:
            embeddings = self._client.create(
                input=input, model=self._deployment_id or self._model_name
            ).data

            # Sort resulting embeddings by index
            sorted_embeddings = sorted(embeddings, key=lambda e: e.index)

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
            sorted_embeddings = sorted(embeddings, key=lambda e: e["index"])

            # Return just the embeddings
            return cast(
                Embeddings, [result["embedding"] for result in sorted_embeddings]
            )


def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction[Documents]]:
    if is_thin_client:
        return None
    else:
        return ONNXMiniLM_L6_V2()


class OpenCLIPEmbeddingFunction(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(
        self,
        model_name: str = "ViT-B-32",
        checkpoint: str = "laion2b_s34b_b79k",
        device: Optional[str] = "cpu",
    ) -> None:
        try:
            import open_clip
        except ImportError:
            raise ValueError(
                "The open_clip python package is not installed. Please install it with `pip install open-clip-torch`. https://github.com/mlfoundations/open_clip"
            )
        try:
            self._torch = importlib.import_module("torch")
        except ImportError:
            raise ValueError(
                "The torch python package is not installed. Please install it with `pip install torch`"
            )

        try:
            self._PILImage = importlib.import_module("PIL.Image")
        except ImportError:
            raise ValueError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
            )

        model, _, preprocess = open_clip.create_model_and_transforms(
            model_name=model_name, pretrained=checkpoint
        )
        self._model = model
        self._model.to(device)
        self._preprocess = preprocess
        self._tokenizer = open_clip.get_tokenizer(model_name=model_name)

    def _encode_image(self, image: Image) -> Embedding:
        pil_image = self._PILImage.fromarray(image)
        with self._torch.no_grad():
            image_features = self._model.encode_image(
                self._preprocess(pil_image).unsqueeze(0)
            )
            image_features /= image_features.norm(dim=-1, keepdim=True)
            return cast(Embedding, image_features.squeeze().tolist())

    def _encode_text(self, text: Document) -> Embedding:
        with self._torch.no_grad():
            text_features = self._model.encode_text(self._tokenizer(text))
            text_features /= text_features.norm(dim=-1, keepdim=True)
            return cast(Embedding, text_features.squeeze().tolist())

    def __call__(self, input: Union[Documents, Images]) -> Embeddings:
        embeddings: Embeddings = []
        for item in input:
            if is_image(item):
                embeddings.append(self._encode_image(cast(Image, item)))
            elif is_document(item):
                embeddings.append(self._encode_text(cast(Document, item)))
        return embeddings


class RoboflowEmbeddingFunction(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(
        self, api_key: str = "", api_url = "https://infer.roboflow.com"
    ) -> None:
        """
        Create a RoboflowEmbeddingFunction.

        Args:
            api_key (str): Your API key for the Roboflow API.
            api_url (str, optional): The URL of the Roboflow API. Defaults to "https://infer.roboflow.com".
        """
        if not api_key:
            api_key = os.environ.get("ROBOFLOW_API_KEY")

        self._api_url = api_url
        self._api_key = api_key 

        try:
            self._PILImage = importlib.import_module("PIL.Image")
        except ImportError:
            raise ValueError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
            )

    def __call__(self, input: Union[Documents, Images]) -> Embeddings:
        embeddings = []

        for item in input:
            if is_image(item):
                image = self._PILImage.fromarray(item)

                buffer = BytesIO()
                image.save(buffer, format="JPEG")
                base64_image = base64.b64encode(buffer.getvalue()).decode("utf-8")

                infer_clip_payload = {
                    "image": {
                        "type": "base64",
                        "value": base64_image,
                    },
                }

                res = requests.post(
                    f"{self._api_url}/clip/embed_image?api_key={self._api_key}",
                    json=infer_clip_payload,
                )

                result = res.json()['embeddings']

                embeddings.append(result[0])
            
            elif is_document(item):
                infer_clip_payload = {
                    "text": input,
                }

                res = requests.post(
                    f"{self._api_url}/clip/embed_text?api_key={self._api_key}",
                    json=infer_clip_payload,
                )

                result = res.json()['embeddings']

                embeddings.append(result[0])

        return embeddings

      
# List of all classes in this module
_classes = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if obj.__module__ == __name__
]


def get_builtins() -> List[str]:
    return _classes
