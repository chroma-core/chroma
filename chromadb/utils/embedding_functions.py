import logging

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from pathlib import Path
import os
import tarfile
import requests
from typing import Any, Dict, List, cast
import numpy as np
import numpy.typing as npt
import importlib
import inspect
import sys
from typing import Optional

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False

logger = logging.getLogger(__name__)


class SentenceTransformerEmbeddingFunction(EmbeddingFunction):
    # Since we do dynamic imports we have to type this as Any
    models: Dict[str, Any] = {}

    # If you have a beefier machine, try "gtr-t5-large".
    # for a full list of options: https://huggingface.co/sentence-transformers, https://www.sbert.net/docs/pretrained_models.html
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        device: str = "cpu",
        normalize_embeddings: bool = False,
    ):
        if model_name not in self.models:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ValueError(
                    "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
                )
            self.models[model_name] = SentenceTransformer(model_name, device=device)
        self._model = self.models[model_name]
        self._normalize_embeddings = normalize_embeddings

    def __call__(self, texts: Documents) -> Embeddings:
        return self._model.encode(  # type: ignore
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=self._normalize_embeddings,
        ).tolist()


class Text2VecEmbeddingFunction(EmbeddingFunction):
    def __init__(self, model_name: str = "shibing624/text2vec-base-chinese"):
        try:
            from text2vec import SentenceModel
        except ImportError:
            raise ValueError(
                "The text2vec python package is not installed. Please install it with `pip install text2vec`"
            )
        self._model = SentenceModel(model_name_or_path=model_name)

    def __call__(self, texts: Documents) -> Embeddings:
        return self._model.encode(list(texts), convert_to_numpy=True).tolist()  # type: ignore # noqa E501


class OpenAIEmbeddingFunction(EmbeddingFunction):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "text-embedding-ada-002",
        organization_id: Optional[str] = None,
        api_base: Optional[str] = None,
        api_type: Optional[str] = None,
        api_version: Optional[str] = None,
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

        if api_type is not None:
            openai.api_type = api_type

        if organization_id is not None:
            openai.organization = organization_id

        self._client = openai.Embedding
        self._model_name = model_name

    def __call__(self, texts: Documents) -> Embeddings:
        # replace newlines, which can negatively affect performance.
        texts = [t.replace("\n", " ") for t in texts]

        # Call the OpenAI Embedding API
        embeddings = self._client.create(input=texts, engine=self._model_name)["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings, key=lambda e: e["index"])  # type: ignore

        # Return just the embeddings
        return [result["embedding"] for result in sorted_embeddings]


class CohereEmbeddingFunction(EmbeddingFunction):
    def __init__(self, api_key: str, model_name: str = "large"):
        try:
            import cohere
        except ImportError:
            raise ValueError(
                "The cohere python package is not installed. Please install it with `pip install cohere`"
            )

        self._client = cohere.Client(api_key)
        self._model_name = model_name

    def __call__(self, texts: Documents) -> Embeddings:
        # Call Cohere Embedding API for each document.
        return [
            embeddings
            for embeddings in self._client.embed(texts=texts, model=self._model_name)
        ]


class HuggingFaceEmbeddingFunction(EmbeddingFunction):
    """
    This class is used to get embeddings for a list of texts using the HuggingFace API.
    It requires an API key and a model name. The default model name is "sentence-transformers/all-MiniLM-L6-v2".
    """

    def __init__(
        self, api_key: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    ):
        """
        Initialize the HuggingFaceEmbeddingFunction.

        Args:
            api_key (str): Your API key for the HuggingFace API.
            model_name (str, optional): The name of the model to use for text embeddings. Defaults to "sentence-transformers/all-MiniLM-L6-v2".
        """
        try:
            import requests
        except ImportError:
            raise ValueError(
                "The requests python package is not installed. Please install it with `pip install requests`"
            )
        self._api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_name}"
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

    def __call__(self, texts: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            texts (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> hugging_face = HuggingFaceEmbeddingFunction(api_key="your_api_key")
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = hugging_face(texts)
        """
        # Call HuggingFace Embedding API for each document
        return self._session.post(  # type: ignore
            self._api_url, json={"inputs": texts, "options": {"wait_for_model": True}}
        ).json()


class InstructorEmbeddingFunction(EmbeddingFunction):
    # If you have a GPU with at least 6GB try model_name = "hkunlp/instructor-xl" and device = "cuda"
    # for a full list of options: https://github.com/HKUNLP/instructor-embedding#model-list
    def __init__(
        self,
        model_name: str = "hkunlp/instructor-base",
        device: str = "cpu",
        instruction: Optional[str] = None,
    ):
        try:
            from InstructorEmbedding import INSTRUCTOR
        except ImportError:
            raise ValueError(
                "The InstructorEmbedding python package is not installed. Please install it with `pip install InstructorEmbedding`"
            )
        self._model = INSTRUCTOR(model_name, device=device)
        self._instruction = instruction

    def __call__(self, texts: Documents) -> Embeddings:
        if self._instruction is None:
            return self._model.encode(texts).tolist()  # type: ignore

        texts_with_instructions = [[self._instruction, text] for text in texts]
        return self._model.encode(texts_with_instructions).tolist()  # type: ignore


# In order to remove dependencies on sentence-transformers, which in turn depends on
# pytorch and sentence-piece we have created a default ONNX embedding function that
# implements the same functionality as "all-MiniLM-L6-v2" from sentence-transformers.
# visit https://github.com/chroma-core/onnx-embedding for the source code to generate
# and verify the ONNX model.
class ONNXMiniLM_L6_V2(EmbeddingFunction):
    MODEL_NAME = "all-MiniLM-L6-v2"
    DOWNLOAD_PATH = Path.home() / ".cache" / "chroma" / "onnx_models" / MODEL_NAME
    EXTRACTED_FOLDER_NAME = "onnx"
    ARCHIVE_FILENAME = "onnx.tar.gz"
    MODEL_DOWNLOAD_URL = (
        "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
    )
    tokenizer = None
    model = None

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

    # Use pytorches default epsilon for division by zero
    # https://pytorch.org/docs/stable/generated/torch.nn.functional.normalize.html
    def _normalize(self, v: npt.NDArray) -> npt.NDArray:  # type: ignore
        norm = np.linalg.norm(v, axis=1)
        norm[norm == 0] = 1e-12
        return v / norm[:, np.newaxis]  # type: ignore

    def _forward(self, documents: List[str], batch_size: int = 32) -> npt.NDArray:  # type: ignore
        # We need to cast to the correct type because the type checker doesn't know that init_model_and_tokenizer will set the values
        self.tokenizer = cast(self.Tokenizer, self.tokenizer)  # type: ignore
        self.model = cast(self.ort.InferenceSession, self.model)  # type: ignore
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

    def _init_model_and_tokenizer(self) -> None:
        if self.model is None and self.tokenizer is None:
            self.tokenizer = self.Tokenizer.from_file(
                os.path.join(
                    self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "tokenizer.json"
                )
            )
            # max_seq_length = 256, for some reason sentence-transformers uses 256 even though the HF config has a max length of 128
            # https://github.com/UKPLab/sentence-transformers/blob/3e1929fddef16df94f8bc6e3b10598a98f46e62d/docs/_static/html/models_en_sentence_embeddings.html#LL480
            self.tokenizer.enable_truncation(max_length=256)
            self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=256)

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
            self.model = self.ort.InferenceSession(
                os.path.join(
                    self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "model.onnx"
                ),
                # Since 1.9 onnyx runtime requires providers to be specified when there are multiple available - https://onnxruntime.ai/docs/api/python/api_summary.html
                # This is probably not ideal but will improve DX as no exceptions will be raised in multi-provider envs
                providers=self._preferred_providers,
            )

    def __call__(self, texts: Documents) -> Embeddings:
        # Only download the model when it is actually used
        self._download_model_if_not_exists()
        self._init_model_and_tokenizer()
        res = cast(Embeddings, self._forward(texts).tolist())
        return res

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


def DefaultEmbeddingFunction() -> Optional[EmbeddingFunction]:
    if is_thin_client:
        return None
    else:
        return ONNXMiniLM_L6_V2()


class GooglePalmEmbeddingFunction(EmbeddingFunction):
    """To use this EmbeddingFunction, you must have the google.generativeai Python package installed and have a PaLM API key."""

    def __init__(self, api_key: str, model_name: str = "models/embedding-gecko-001"):
        if not api_key:
            raise ValueError("Please provide a PaLM API key.")

        if not model_name:
            raise ValueError("Please provide the model name.")

        try:
            import google.generativeai as palm
        except ImportError:
            raise ValueError(
                "The Google Generative AI python package is not installed. Please install it with `pip install google-generativeai`"
            )

        palm.configure(api_key=api_key)
        self._palm = palm
        self._model_name = model_name

    def __call__(self, texts: Documents) -> Embeddings:
        return [
            self._palm.generate_embeddings(model=self._model_name, text=text)[
                "embedding"
            ]
            for text in texts
        ]


class GoogleVertexEmbeddingFunction(EmbeddingFunction):
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
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

    def __call__(self, texts: Documents) -> Embeddings:
        embeddings = []
        for text in texts:
            response = self._session.post(
                self._api_url, json={"instances": [{"content": text}]}
            ).json()

            if "predictions" in response:
                embeddings.append(response["predictions"]["embeddings"]["values"])

        return embeddings


class VoyageAIEmbeddingFunction(EmbeddingFunction):
    def __init__(self, api_key: str, model_name: str = "voyage-01", batch_size: int = 8):
        """
        Initialize the VoyageAIEmbeddingFunction.

        Args:
        api_key (str): Your API key for the HuggingFace API.
        model_name (str, optional): The name of the model to use for text embeddings. Defaults to "voyage-01".
        batch_size (int, optional): The number of documents to send at a time. Defaults to 8 (The max supported 3rd Nov 2023).
        """
        if batch_size > 8:
            print(f"Voyage AI as of (3rd Nov 2023) has a batch size of max 8")

        if not api_key:
            raise ValueError("Please provide a VoyageAI API key.")

        try:
            import voyageai
            from voyageai import get_embeddings
        except ImportError:
            raise ValueError("The VoyageAI python package is not installed. Please install it with `pip install voyageai`")

        voyageai.api_key = api_key  # Voyage API Key
        self.batch_size = batch_size
        self.model = model_name
        self.get_embeddings = get_embeddings

    def __call__(self, texts: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
        texts (Documents): A list of texts to get embeddings for.

        Returns:
        Embeddings: The embeddings for the texts.

        Example:
        >>> voyage_ef = VoyageAIEmbeddingFunction(api_key="your_api_key")
        >>> texts = ["Hello, world!", "How are you?"]
        >>> embeddings = voyage_ef(texts)
        """
        iters = range(0, len(texts), self.batch_size)
        embeddings = []
        for i in iters:
            results = self.get_embeddings(
                texts[i : i + self.batch_size], 
                batch_size=self.batch_size, 
                model=self.model
            )
            embeddings += results;
        return embeddings;

# List of all classes in this module
_classes = [
    name
    for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass)
    if obj.__module__ == __name__
]


def get_builtins() -> List[str]:
    return _classes
