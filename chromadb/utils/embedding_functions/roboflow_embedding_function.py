import logging

from chromadb.api.types import (
    Documents,
    Images,
    EmbeddingFunction,
    Embeddings,
    is_image,
    is_document,
)

from io import BytesIO
import os
import requests
from typing import Union
import importlib
import base64

logger = logging.getLogger(__name__)


class RoboflowEmbeddingFunction(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(self, api_key: str = "", api_url="https://infer.roboflow.com") -> None:
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

                result = res.json()["embeddings"]

                embeddings.append(result[0])

            elif is_document(item):
                infer_clip_payload = {
                    "text": input,
                }

                res = requests.post(
                    f"{self._api_url}/clip/embed_text?api_key={self._api_key}",
                    json=infer_clip_payload,
                )

                result = res.json()["embeddings"]

                embeddings.append(result[0])

        return embeddings