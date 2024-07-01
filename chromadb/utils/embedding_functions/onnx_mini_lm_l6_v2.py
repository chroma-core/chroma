import hashlib
import importlib
import logging
import os
import tarfile
from functools import cached_property
from pathlib import Path
import filelock
from typing import List, Optional, cast

import numpy as np
import numpy.typing as npt
import httpx
from onnxruntime import InferenceSession, get_available_providers, SessionOptions
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random
from tokenizers import Tokenizer

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


def _verify_sha256(fname: str, expected_sha256: str) -> bool:
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
    MODEL_NAME = "all-MiniLM-L6-v2"
    DOWNLOAD_PATH: Path
    EXTRACTED_FOLDER_NAME = "onnx"
    ARCHIVE_FILENAME = "onnx.tar.gz"
    MODEL_DOWNLOAD_URL = (
        "https://chroma-onnx-models.s3.amazonaws.com/all-MiniLM-L6-v2/onnx.tar.gz"
    )
    _MODEL_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"

    # https://github.com/python/mypy/issues/7291 mypy makes you type the constructor if
    # no args
    def __init__(
        self,
        preferred_providers: Optional[List[str]] = None,
        download_path: Optional[Path] = None,
    ) -> None:
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

        self.DOWNLOAD_PATH = (
            download_path
            if download_path
            else Path.home() / ".cache" / "chroma" / "onnx_models" / self.MODEL_NAME
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

        About ignored types:
        tenacity.retry decorator is a bit convoluted when it comes to type annotations
        which makes mypy unhappy. If some smart folk knows how to fix this in an
        elegant way, please do so.
        """
        with httpx.stream("GET", url) as resp:
            total = int(resp.headers.get("content-length", 0))
            with open(fname, "wb") as file, self.tqdm(
                desc=str(fname),
                total=total,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for data in resp.iter_bytes(chunk_size=chunk_size):
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
    def _normalize(self, v: npt.NDArray[np.float32]) -> npt.NDArray[np.float32]:
        norm = np.linalg.norm(v, axis=1)
        norm[norm == 0] = 1e-12
        return cast(npt.NDArray[np.float32], v / norm[:, np.newaxis])

    def _forward(
        self, documents: List[str], batch_size: int = 32
    ) -> npt.NDArray[np.float32]:
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
    def tokenizer(self) -> Tokenizer:
        tokenizer = Tokenizer.from_file(
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
    def model(self) -> InferenceSession:
        if self._preferred_providers is None or len(self._preferred_providers) == 0:
            if len(get_available_providers()) > 0:
                logger.debug(
                    f"WARNING: No ONNX providers provided, defaulting to available providers: "
                    f"{get_available_providers()}"
                )
            self._preferred_providers = get_available_providers()
        elif not set(self._preferred_providers).issubset(
            set(get_available_providers())
        ):
            raise ValueError(
                f"Preferred providers must be subset of available providers: {get_available_providers()}"
            )

        # Suppress onnxruntime warnings. This produces logspew, mainly when onnx tries to use CoreML, which doesn't fit this model.
        so = SessionOptions()
        so.log_severity_level = 3

        return InferenceSession(
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
        lock_file = os.path.join(self.DOWNLOAD_PATH, ".lock")
        os.makedirs(self.DOWNLOAD_PATH, exist_ok=True)

        with filelock.FileLock(lock_file):
            onnx_files_exist = True
            for f in onnx_files:
                if not os.path.exists(os.path.join(extracted_folder, f)):
                    onnx_files_exist = False
                    break
            # Model is not downloaded yet
            if not onnx_files_exist:
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
