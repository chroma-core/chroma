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

if TYPE_CHECKING:
    from onnxruntime import InferenceSession
    from tokenizers import Tokenizer

logger = logging.getLogger(__name__)


def _verify_sha256(fname: str, expected_sha256: str) -> bool:
    sha256_hash = hashlib.sha256()
    with open(fname, "rb") as f:
        # Read and update hash in chunks to avoid using too much memory
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest() == expected_sha256


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


# In order to remove dependencies on sentence-transformers, which in turn depends on
# pytorch and sentence-piece we have created a default ONNX embedding function that
# implements the same functionality as "all-MiniLM-L6-v2" from sentence-transformers.
# visit https://github.com/chroma-core/onnx-embedding for the source code to generate
# and verify the ONNX model.
class ONNXMiniLM_L6_V2(EmbeddingFunction[Documents]):
    MODEL_NAME = "all-MiniLM-L6-v2"
    DOWNLOAD_PATH = Path.home() / ".cache" / "chroma" / "onnx_models" / MODEL_NAME
    EXTRACTED_FOLDER_NAME = "onnx"
    ARCHIVE_FILENAME = "onnx.tar.gz"
    MODEL_DOWNLOAD_URL = (
        "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
    )
    _MODEL_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"

    # https://github.com/python/mypy/issues/7291 mypy makes you type the constructor if
    # no args
    def __init__(self, preferred_providers: Optional[List[str]] = None) -> None:
        # Import dependencies on demand to mirror other embedding functions. This
        # breaks typechecking, thus the ignores.
        # convert the list to set for unique values
        if preferred_providers and not all(
            [isinstance(i, str) for i in preferred_providers]
        ):
            raise ValueError("Preferred providers must be a list of strings")
        # check for duplicate providers
        if preferred_providers and len(preferred_providers) != len(
            set(preferred_providers)
        ):
            raise ValueError("Preferred providers must be unique")
        self._preferred_providers = preferred_providers
        try:
            # Equivalent to import onnxruntime
            self.ort = importlib.import_module("onnxruntime")
        except ImportError:
            raise ValueError(
                "The onnxruntime python package is not installed. Please install it with `pip install onnxruntime`"
            )
        try:
            # Equivalent to from tokenizers import Tokenizer
            self.Tokenizer = importlib.import_module("tokenizers").Tokenizer
        except ImportError:
            raise ValueError(
                "The tokenizers python package is not installed. Please install it with `pip install tokenizers`"
            )
        try:
            # Equivalent to from tqdm import tqdm
            self.tqdm = importlib.import_module("tqdm").tqdm
        except ImportError:
            raise ValueError(
                "The tqdm python package is not installed. Please install it with `pip install tqdm`"
            )

    # Borrowed from https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
    # Download with tqdm to preserve the sentence-transformers experience
    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=3),
        retry=retry_if_exception(lambda e: "does not match expected SHA256" in str(e)),
    )
    def _download(self, url: str, fname: str, chunk_size: int = 1024) -> None:
        resp = requests.get(url, stream=True)
        total = int(resp.headers.get("content-length", 0))
        with open(fname, "wb") as file, self.tqdm(
            desc=str(fname),
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=chunk_size):
                size = file.write(data)
                bar.update(size)
        if not _verify_sha256(fname, self._MODEL_SHA256):
            # if the integrity of the file is not verified, remove it
            os.remove(fname)
            raise ValueError(
                f"Downloaded file {fname} does not match expected SHA256 hash. Corrupted download or malicious file."
            )

    # Use pytorches default epsilon for division by zero
    # https://pytorch.org/docs/stable/generated/torch.nn.functional.normalize.html
    def _normalize(self, v: npt.NDArray) -> npt.NDArray:
        norm = np.linalg.norm(v, axis=1)
        norm[norm == 0] = 1e-12
        return cast(npt.NDArray, v / norm[:, np.newaxis])

    def _forward(self, documents: List[str], batch_size: int = 32) -> npt.NDArray:
        # We need to cast to the correct type because the type checker doesn't know that init_model_and_tokenizer will set the values
        self.tokenizer = cast(self.Tokenizer, self.tokenizer)
        self.model = cast(self.ort.InferenceSession, self.model)
        all_embeddings = []
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            encoded = [self.tokenizer.encode(d) for d in batch]
            input_ids = np.array([e.ids for e in encoded])
            attention_mask = np.array([e.attention_mask for e in encoded])
            onnx_input = {
                "input_ids": np.array(input_ids, dtype=np.int64),
                "attention_mask": np.array(attention_mask, dtype=np.int64),
                "token_type_ids": np.array(
                    [np.zeros(len(e), dtype=np.int64) for e in input_ids],
                    dtype=np.int64,
                ),
            }
            model_output = self.model.run(None, onnx_input)
            last_hidden_state = model_output[0]
            # Perform mean pooling with attention weighting
            input_mask_expanded = np.broadcast_to(
                np.expand_dims(attention_mask, -1), last_hidden_state.shape
            )
            embeddings = np.sum(last_hidden_state * input_mask_expanded, 1) / np.clip(
                input_mask_expanded.sum(1), a_min=1e-9, a_max=None
            )
            embeddings = self._normalize(embeddings).astype(np.float32)
            all_embeddings.append(embeddings)
        return np.concatenate(all_embeddings)

    @cached_property
    def tokenizer(self) -> "Tokenizer":
        tokenizer = self.Tokenizer.from_file(
            os.path.join(
                self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "tokenizer.json"
            )
        )
        # max_seq_length = 256, for some reason sentence-transformers uses 256 even though the HF config has a max length of 128
        # https://github.com/UKPLab/sentence-transformers/blob/3e1929fddef16df94f8bc6e3b10598a98f46e62d/docs/_static/html/models_en_sentence_embeddings.html#LL480
        tokenizer.enable_truncation(max_length=256)
        tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=256)
        return tokenizer

    @cached_property
    def model(self) -> "InferenceSession":
        if self._preferred_providers is None or len(self._preferred_providers) == 0:
            if len(self.ort.get_available_providers()) > 0:
                logger.debug(
                    f"WARNING: No ONNX providers provided, defaulting to available providers: "
                    f"{self.ort.get_available_providers()}"
                )
            self._preferred_providers = self.ort.get_available_providers()
        elif not set(self._preferred_providers).issubset(
            set(self.ort.get_available_providers())
        ):
            raise ValueError(
                f"Preferred providers must be subset of available providers: {self.ort.get_available_providers()}"
            )

        # Suppress onnxruntime warnings. This produces logspew, mainly when onnx tries to use CoreML, which doesn't fit this model.
        so = self.ort.SessionOptions()
        so.log_severity_level = 3

        return self.ort.InferenceSession(
            os.path.join(self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "model.onnx"),
            # Since 1.9 onnyx runtime requires providers to be specified when there are multiple available - https://onnxruntime.ai/docs/api/python/api_summary.html
            # This is probably not ideal but will improve DX as no exceptions will be raised in multi-provider envs
            providers=self._preferred_providers,
            sess_options=so,
        )

    def __call__(self, input: Documents) -> Embeddings:
        # Only download the model when it is actually used
        self._download_model_if_not_exists()
        return cast(Embeddings, self._forward(input).tolist())

    def _download_model_if_not_exists(self) -> None:
        onnx_files = [
            "config.json",
            "model.onnx",
            "special_tokens_map.json",
            "tokenizer_config.json",
            "tokenizer.json",
            "vocab.txt",
        ]
        extracted_folder = os.path.join(self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME)
        onnx_files_exist = True
        for f in onnx_files:
            if not os.path.exists(os.path.join(extracted_folder, f)):
                onnx_files_exist = False
                break
        # Model is not downloaded yet
        if not onnx_files_exist:
            os.makedirs(self.DOWNLOAD_PATH, exist_ok=True)
            if not os.path.exists(
                os.path.join(self.DOWNLOAD_PATH, self.ARCHIVE_FILENAME)
            ) or not _verify_sha256(
                os.path.join(self.DOWNLOAD_PATH, self.ARCHIVE_FILENAME),
                self._MODEL_SHA256,
            ):
                self._download(
                    url=self.MODEL_DOWNLOAD_URL,
                    fname=os.path.join(self.DOWNLOAD_PATH, self.ARCHIVE_FILENAME),
                )
            with tarfile.open(
                name=os.path.join(self.DOWNLOAD_PATH, self.ARCHIVE_FILENAME),
                mode="r:gz",
            ) as tar:
                tar.extractall(path=self.DOWNLOAD_PATH)


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
