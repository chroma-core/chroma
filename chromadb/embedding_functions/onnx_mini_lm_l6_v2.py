import hashlib
import importlib
import os
import tarfile
from functools import cached_property
from pathlib import Path
from typing import List, Dict, Any, Optional, cast
import numpy as np
import numpy.typing as npt
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random

from chromadb.embedding_functions.embedding_function import EmbeddingFunction, Space
from chromadb.api.types import Embeddings, Documents


def _verify_sha256(fname: str, expected_sha256: str) -> bool:
    """
    Verify that a file has the expected SHA256 hash.

    Args:
        fname: The path to the file to verify.
        expected_sha256: The expected SHA256 hash.

    Returns:
        True if the file has the expected SHA256 hash, False otherwise.
    """
    sha256_hash = hashlib.sha256()
    with open(fname, "rb") as f:
        # Read and update hash in chunks to avoid using too much memory
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest() == expected_sha256


# In order to remove dependencies on sentence-transformers, which in turn depends on
# pytorch and sentence-piece we have created a default ONNX embedding function that
# implements the same functionality as "all-MiniLM-L6-v2" from sentence-transformers.
# visit https://github.com/chroma-core/onnx-embedding for the source code to generate
# and verify the ONNX model.
class ONNXMiniLM_L6_V2(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the ONNX MiniLM L6 V2 model.
    """

    MODEL_NAME = "all-MiniLM-L6-v2"
    DOWNLOAD_PATH = Path.home() / ".cache" / "chroma" / "onnx_models" / MODEL_NAME
    EXTRACTED_FOLDER_NAME = "onnx"
    ARCHIVE_FILENAME = "onnx.tar.gz"
    MODEL_DOWNLOAD_URL = (
        "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
    )
    _MODEL_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"

    def __init__(self, preferred_providers: Optional[List[str]] = None) -> None:
        """
        Initialize the ONNXMiniLM_L6_V2 embedding function.

        Args:
            preferred_providers (List[str], optional): The preferred ONNX runtime providers.
                Defaults to None.
        """
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

        self.preferred_providers = preferred_providers

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

        try:
            # Equivalent to import httpx
            self.httpx = importlib.import_module("httpx")
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
            )

    # Borrowed from https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
    # Download with tqdm to preserve the sentence-transformers experience
    @retry(  # type: ignore
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=3),
        retry=retry_if_exception(lambda e: "does not match expected SHA256" in str(e)),
    )
    def _download(self, url: str, fname: str, chunk_size: int = 1024) -> None:
        """
        Download the onnx model from the URL and save it to the file path.

        Args:
            url: The URL to download the model from.
            fname: The path to save the model to.
            chunk_size: The chunk size to use when downloading.
        """
        with self.httpx.stream("GET", url) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            # Can use tqdm to display a progress bar
            with open(fname, "wb") as file, self.tqdm(
                desc=fname,
                total=total,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                downloaded = 0
                for data in resp.iter_bytes(chunk_size=chunk_size):
                    size = file.write(data)
                    bar.update(size)
                    downloaded += size

        if not _verify_sha256(fname, self._MODEL_SHA256):
            os.remove(fname)
            raise ValueError(
                f"Downloaded file {fname} does not match expected SHA256 {self._MODEL_SHA256}"
            )

    def _normalize(self, v: npt.NDArray[np.float32]) -> npt.NDArray[np.float32]:
        """
        Normalize a vector.

        Args:
            v: The vector to normalize.

        Returns:
            The normalized vector.
        """
        norm = np.linalg.norm(v, axis=1)
        return cast(npt.NDArray[np.float32], v / norm[:, np.newaxis])

    def _forward(
        self, documents: List[str], batch_size: int = 32
    ) -> npt.NDArray[np.float32]:
        """
        Generate embeddings for a list of documents.

        Args:
            documents: The documents to generate embeddings for.
            batch_size: The batch size to use when generating embeddings.

        Returns:
            The embeddings for the documents.
        """
        all_embeddings = []
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            tokens = self.tokenizer.encode(batch)
            if len(tokens) > self.max_tokens():
                raise ValueError(
                    f"Batch size {len(tokens)} is greater than the max tokens {self.max_tokens()}"
                )
            input_ids = np.array([e.ids for e in tokens])
            attention_mask = np.array([e.attention_mask for e in tokens])
            token_type_ids = np.array([e.type_ids for e in tokens])

            model_inputs = {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
                "token_type_ids": token_type_ids,
            }

            outputs = self.model.run(None, model_inputs)
            embeddings = outputs[0]
            embeddings = self._normalize(embeddings)
            all_embeddings.append(embeddings)
        return np.vstack(all_embeddings)

    @cached_property
    def tokenizer(self) -> Any:
        """
        Get the tokenizer for the model.

        Returns:
            The tokenizer for the model.
        """
        tokenizer_path = os.path.join(
            self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "tokenizer.json"
        )
        return self.Tokenizer.from_file(tokenizer_path)

    @cached_property
    def model(self) -> Any:
        """
        Get the model.

        Returns:
            The model.
        """
        so = self.ort.SessionOptions()
        so.graph_optimization_level = self.ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        return self.ort.InferenceSession(
            os.path.join(self.DOWNLOAD_PATH, self.EXTRACTED_FOLDER_NAME, "model.onnx"),
            # Since 1.9 onnyx runtime requires providers to be specified when there are multiple available - https://onnxruntime.ai/docs/api/python/api_summary.html
            # This is probably not ideal but will improve DX as no exceptions will be raised in multi-provider envs
            providers=self.preferred_providers,
            sess_options=so,
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # ONNX Mini LM L6 V2 only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError(
                "ONNX Mini LM L6 V2 only supports text documents, not images"
            )

        # Only download the model when it is actually used
        self._download_model_if_not_exists()
        # Cast to List[str] since we've verified all items are strings
        embeddings = self._forward([str(item) for item in input])

        # Convert to numpy arrays and cast to the expected Embeddings type
        return cast(
            Embeddings,
            [np.array(embedding, dtype=np.float32) for embedding in embeddings],
        )

    def _download_model_if_not_exists(self) -> None:
        """
        Download the model if it doesn't exist.
        """
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

    def name(self) -> str:
        return "onnx_mini_lm_l6_v2"

    def default_space(self) -> Space:
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    def max_tokens(self) -> int:
        # Default token limit for ONNX Mini LM L6 V2 model
        return 256

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        preferred_providers = config.get("preferred_providers")

        return ONNXMiniLM_L6_V2(preferred_providers=preferred_providers)

    def get_config(self) -> Dict[str, Any]:
        return {"preferred_providers": self.preferred_providers}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        # Preferred providers can be changed, so no validation needed
        pass

    def validate_config(self, config: Dict[str, Any]) -> None:
        # TODO: Validate with JSON schema
        pass
