from chromadb.api.types import (
    Embeddings,
    EmbeddingFunction,
    Space,
    Embeddable,
    is_document,
)
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Optional, cast
import os
import warnings
import numpy as np

# Media file extensions that Marengo can embed when a document is a public URL.
_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp")
_AUDIO_EXTS = (".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac")


class TwelveLabsEmbeddingFunction(EmbeddingFunction[Embeddable]):
    """
    Embedding function for the TwelveLabs Marengo multimodal model.

    Marengo embeds text, images, and audio into a single 512-dimensional space,
    so text queries can retrieve media stored in the same Chroma collection.

    Each input document is either plain text or a publicly accessible media URL.
    Documents ending in a known image/audio extension (or carrying an explicit
    ``image:``/``audio:`` prefix) are sent as media URLs; everything else is sent
    as text. Marengo ingests media by URL only, so local image arrays are not
    supported here -- host the media and pass its URL as a document instead.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "marengo3.0",
        api_key_env_var: str = "TWELVELABS_API_KEY",
    ):
        """
        Initialize the TwelveLabsEmbeddingFunction.

        Args:
            api_key (str, optional): Your TwelveLabs API key. Prefer setting the
                environment variable named by ``api_key_env_var`` so the key is
                not persisted in the collection config.
            model_name (str, optional): The Marengo model to use.
                Defaults to "marengo3.0".
            api_key_env_var (str, optional): Environment variable name that holds
                your TwelveLabs API key. Defaults to "TWELVELABS_API_KEY".
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
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

        self.model_name = model_name
        self._api_url = "https://api.twelvelabs.io/v1.3/embed"
        self._session = httpx.Client()
        self._session.headers.update({"x-api-key": self.api_key})

    def _media_kind(self, item: str) -> Optional[str]:
        """Return "image" or "audio" if the document is a media URL, else None."""
        lowered = item.lower()
        if lowered.startswith("image:"):
            return "image"
        if lowered.startswith("audio:"):
            return "audio"
        if not (lowered.startswith("http://") or lowered.startswith("https://")):
            return None
        # Strip query string before checking the extension.
        path = lowered.split("?", 1)[0]
        if path.endswith(_IMAGE_EXTS):
            return "image"
        if path.endswith(_AUDIO_EXTS):
            return "audio"
        return None

    def _embed_one(self, item: str) -> np.ndarray:
        data: Dict[str, Any] = {"model_name": self.model_name}
        kind = self._media_kind(item)
        if kind == "image":
            data["image_url"] = (
                item.split(":", 1)[1] if item.lower().startswith("image:") else item
            )
        elif kind == "audio":
            data["audio_url"] = (
                item.split(":", 1)[1] if item.lower().startswith("audio:") else item
            )
        else:
            data["text"] = item

        # An empty `files` entry forces a multipart/form-data request, which the
        # /embed endpoint requires.
        resp = self._session.post(
            self._api_url, data=data, files={"_": (None, "")}, timeout=60
        ).json()

        if "text_embedding" in resp:
            segments = resp["text_embedding"]["segments"]
        elif "image_embedding" in resp:
            segments = resp["image_embedding"]["segments"]
        elif "audio_embedding" in resp:
            segments = resp["audio_embedding"]["segments"]
        else:
            raise RuntimeError(resp.get("message", f"Unexpected response: {resp}"))

        return np.array(segments[0]["float"], dtype=np.float32)

    def __call__(self, input: Embeddable) -> Embeddings:
        """
        Get Marengo embeddings for a list of text and/or media-URL documents.

        Args:
            input (Embeddable): A list of text strings and/or media URLs.

        Returns:
            Embeddings: A list of 512-dimensional embeddings.

        Example:
            >>> ef = TwelveLabsEmbeddingFunction()
            >>> ef(["a cat playing piano", "https://example.com/cat.jpg"])
        """
        embeddings: Embeddings = []
        for item in input:
            if not is_document(item):
                raise ValueError(
                    "TwelveLabsEmbeddingFunction only supports text and media-URL "
                    "documents. Host images/audio and pass their URLs as documents."
                )
            embeddings.append(self._embed_one(cast(str, item)))
        return embeddings

    @staticmethod
    def name() -> str:
        return "twelvelabs"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Embeddable]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"  # this is for type checking

        return TwelveLabsEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
        }

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
        validate_config_schema(config, "twelvelabs")
