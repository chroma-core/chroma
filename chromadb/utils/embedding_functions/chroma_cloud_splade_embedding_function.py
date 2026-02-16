from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseVector,
    SparseVectors,
    Documents,
)
from typing import Any, Dict, List, Optional, Tuple, Union
from enum import Enum
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.sparse_embedding_utils import (
    max_pool_sparse_vectors,
    normalize_sparse_vector,
)
import logging
import os
from chromadb.utils.embedding_functions.utils import _get_shared_system_client

logger = logging.getLogger(__name__)

# SPLADE models are BERT-based with a 512 token max sequence length.
# 2 tokens are reserved for [CLS] and [SEP] special tokens.
_SPLADE_MAX_CONTENT_TOKENS = 510
_SPLADE_TOKENIZER_MODEL = "bert-base-uncased"


class ChromaCloudSpladeEmbeddingModel(Enum):
    SPLADE_PP_EN_V1 = "prithivida/Splade_PP_en_v1"


class ChromaCloudSpladeEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    _tokenizer: Any = None

    def __init__(
        self,
        api_key_env_var: str = "CHROMA_API_KEY",
        model: ChromaCloudSpladeEmbeddingModel = ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        include_tokens: bool = False,
    ):
        """
        Initialize the ChromaCloudSpladeEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "CHROMA_API_KEY".
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
            )
        self.api_key_env_var = api_key_env_var
        # First, try to get API key from environment variable
        self.api_key = os.getenv(self.api_key_env_var)
        # If not found in env var, try to get it from existing client instances
        if not self.api_key:
            SharedSystemClient = _get_shared_system_client()
            self.api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()
        # Raise error if still no API key found
        if not self.api_key:
            raise ValueError(
                f"API key not found in environment variable {self.api_key_env_var} "
                f"or in any existing client instances"
            )
        self.model = model
        self.include_tokens = bool(include_tokens)
        self._api_url = "https://embed.trychroma.com/embed_sparse"
        self._session = httpx.Client()
        self._session.headers.update(
            {
                "x-chroma-token": self.api_key,
                "x-chroma-embedding-model": self.model.value,
            }
        )

    def __del__(self) -> None:
        """
        Cleanup the HTTP client session when the object is destroyed.
        """
        if hasattr(self, "_session"):
            self._session.close()

    def close(self) -> None:
        """
        Explicitly close the HTTP client session.
        Call this method when you're done using the embedding function.
        """
        if hasattr(self, "_session"):
            self._session.close()

    @classmethod
    def _get_tokenizer(cls) -> Any:
        """Lazy-load and cache the BERT tokenizer used by SPLADE models.

        Returns ``None`` if the tokenizer cannot be loaded (e.g. no network
        access to HuggingFace Hub). Callers must handle the ``None`` case.
        """
        if cls._tokenizer is None:
            try:
                from tokenizers import Tokenizer

                cls._tokenizer = Tokenizer.from_pretrained(
                    _SPLADE_TOKENIZER_MODEL
                )
            except Exception:
                logger.debug(
                    "Could not load BERT tokenizer for SPLADE chunking; "
                    "falling back to character-based estimation."
                )
                return None
        return cls._tokenizer

    @classmethod
    def _chunk_text(cls, text: str) -> List[str]:
        """Split text into chunks that each fit within the SPLADE token limit.

        When the BERT tokenizer is available, splits at exact token boundaries.
        Otherwise falls back to a conservative character-based estimate (~4
        characters per BERT token, splitting on word boundaries).

        Returns the original text as a single-element list if it fits within
        the limit.
        """
        tokenizer = cls._get_tokenizer()
        if tokenizer is not None:
            return cls._chunk_text_with_tokenizer(text, tokenizer)
        return cls._chunk_text_by_chars(text)

    @staticmethod
    def _chunk_text_with_tokenizer(text: str, tokenizer: Any) -> List[str]:
        """Chunk using the real BERT tokenizer for precise splitting."""
        encoding = tokenizer.encode(text, add_special_tokens=False)
        token_ids = encoding.ids

        if len(token_ids) <= _SPLADE_MAX_CONTENT_TOKENS:
            return [text]

        offsets = encoding.offsets
        chunks: List[str] = []
        for i in range(0, len(token_ids), _SPLADE_MAX_CONTENT_TOKENS):
            chunk_offsets = offsets[i : i + _SPLADE_MAX_CONTENT_TOKENS]
            if chunk_offsets:
                start_char = chunk_offsets[0][0]
                end_char = chunk_offsets[-1][1]
                chunk = text[start_char:end_char]
                if chunk.strip():
                    chunks.append(chunk)

        return chunks if chunks else [text]

    @staticmethod
    def _chunk_text_by_chars(text: str) -> List[str]:
        """Fallback: chunk by character count when no tokenizer is available.

        Uses a conservative estimate of ~4 characters per BERT token, giving
        a maximum of ~2000 characters per chunk for 510 content tokens. Splits
        on word boundaries to avoid breaking words.
        """
        max_chars = _SPLADE_MAX_CONTENT_TOKENS * 4  # ~2040, conservative
        if len(text) <= max_chars:
            return [text]

        chunks: List[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= max_chars:
                chunks.append(remaining)
                break

            # Find a word boundary to split at
            split_at = max_chars
            while split_at > 0 and remaining[split_at] != " ":
                split_at -= 1
            if split_at == 0:
                split_at = max_chars  # force split if no space found

            chunk = remaining[:split_at].strip()
            if chunk:
                chunks.append(chunk)
            remaining = remaining[split_at:].strip()

        return chunks if chunks else [text]

    def __call__(self, input: Documents) -> SparseVectors:
        """
        Generate embeddings for the given documents.

        Documents that exceed the SPLADE token limit (512 tokens) are
        automatically split into chunks. Each chunk is embedded independently,
        and the per-chunk sparse vectors are combined via element-wise max
        pooling to produce a single sparse vector per input document.

        Args:
            input (Documents): The documents to generate embeddings for.
        """
        if not input:
            return []

        # Chunk documents that exceed the token limit and track the mapping
        # back to the original document index.
        all_chunks: List[str] = []
        doc_chunk_ranges: List[Tuple[int, int]] = []

        for doc in input:
            start = len(all_chunks)
            chunks = self._chunk_text(doc)
            all_chunks.extend(chunks)
            doc_chunk_ranges.append((start, len(all_chunks)))

        chunk_embeddings = self._embed_texts(all_chunks)

        # Aggregate chunk embeddings per original document via max pooling.
        result: SparseVectors = []
        for start, end in doc_chunk_ranges:
            doc_vectors = chunk_embeddings[start:end]
            if len(doc_vectors) == 1:
                result.append(doc_vectors[0])
            else:
                result.append(max_pool_sparse_vectors(doc_vectors))

        return result

    def _embed_texts(self, texts: List[str]) -> SparseVectors:
        """Send texts to the Chroma Cloud sparse embedding API."""
        payload: Dict[str, Union[str, List[str]]] = {
            "texts": texts,
            "task": "",
            "target": "",
            "fetch_tokens": "true" if self.include_tokens is True else "false",
        }

        try:
            import httpx

            response = self._session.post(self._api_url, json=payload, timeout=60)
            response.raise_for_status()
            json_response = response.json()
            return self._parse_response(json_response)
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Failed to get embeddings from Chroma Cloud API: HTTP {e.response.status_code} - {e.response.text}"
            )
        except httpx.TimeoutException:
            raise RuntimeError("Request to Chroma Cloud API timed out after 60 seconds")
        except httpx.HTTPError as e:
            raise RuntimeError(f"Failed to get embeddings from Chroma Cloud API: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error calling Chroma Cloud API: {e}")

    def _parse_response(self, response: Any) -> SparseVectors:
        """
        Parse the response from the Chroma Cloud Sparse Embedding API.
        """
        raw_embeddings = response["embeddings"]

        # Normalize each sparse vector (sort indices and validate)
        normalized_vectors: SparseVectors = []
        for emb in raw_embeddings:
            # Handle both dict format and SparseVector format
            if isinstance(emb, dict):
                indices = emb.get("indices", [])
                values = emb.get("values", [])
                raw_labels = emb.get("labels") if self.include_tokens else None
                labels: Optional[List[str]] = raw_labels if raw_labels else None
            else:
                # Already a SparseVector, extract its data
                assert isinstance(emb, SparseVector)
                indices = emb.indices
                values = emb.values
                labels = emb.labels if self.include_tokens else None

            normalized_vectors.append(
                normalize_sparse_vector(indices=indices, values=values, labels=labels)
            )

        return normalized_vectors

    @staticmethod
    def name() -> str:
        return "chroma-cloud-splade"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model = config.get("model")
        if model is None:
            raise ValueError("model must be provided in config")
        if not api_key_env_var:
            raise ValueError("api_key_env_var must be provided in config")
        return ChromaCloudSpladeEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model=ChromaCloudSpladeEmbeddingModel(model),
            include_tokens=config.get("include_tokens", False),
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model": self.model.value,
            "include_tokens": self.include_tokens,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        immutable_keys = {"include_tokens", "model"}
        for key in immutable_keys:
            if key in new_config and new_config[key] != old_config.get(key):
                raise ValueError(
                    f"Updating '{key}' is not supported for chroma-cloud-splade"
                )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "chroma-cloud-splade")
