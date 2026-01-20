from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, TypedDict

from chromadb.api.types import Documents, SparseEmbeddingFunction, SparseVectors
from chromadb.base_types import SparseVector
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.embedding_functions.schemas.bm25_tokenizer import (
    Bm25Tokenizer,
    DEFAULT_CHROMA_BM25_STOPWORDS as _DEFAULT_STOPWORDS,
    get_english_stemmer,
    Murmur3AbsHasher,
)

NAME = "chroma_bm25"

DEFAULT_K = 1.2
DEFAULT_B = 0.75
DEFAULT_AVG_DOC_LENGTH = 256.0
DEFAULT_TOKEN_MAX_LENGTH = 40

DEFAULT_CHROMA_BM25_STOPWORDS: List[str] = list(_DEFAULT_STOPWORDS)


class _HashedToken:
    __slots__ = ("hash", "label")

    def __init__(self, hash: int, label: Optional[str]):
        self.hash = hash
        self.label = label

    def __hash__(self) -> int:
        return self.hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _HashedToken):
            return NotImplemented
        return self.hash == other.hash

    def __lt__(self, other: "_HashedToken") -> bool:
        return self.hash < other.hash


class ChromaBm25Config(TypedDict, total=False):
    k: float
    b: float
    avg_doc_length: float
    token_max_length: int
    stopwords: List[str]
    include_tokens: bool


class ChromaBm25EmbeddingFunction(SparseEmbeddingFunction[Documents]):
    def __init__(
        self,
        k: float = DEFAULT_K,
        b: float = DEFAULT_B,
        avg_doc_length: float = DEFAULT_AVG_DOC_LENGTH,
        token_max_length: int = DEFAULT_TOKEN_MAX_LENGTH,
        stopwords: Optional[Iterable[str]] = None,
        include_tokens: bool = False,
    ) -> None:
        """Initialize the BM25 sparse embedding function."""

        self.k = float(k)
        self.b = float(b)
        self.avg_doc_length = float(avg_doc_length)
        self.token_max_length = int(token_max_length)
        self.include_tokens = bool(include_tokens)

        if stopwords is not None:
            self.stopwords: Optional[List[str]] = [str(word) for word in stopwords]
            self._stopword_list: Iterable[str] = self.stopwords
        else:
            self.stopwords = None
            self._stopword_list = DEFAULT_CHROMA_BM25_STOPWORDS

        self._hasher = Murmur3AbsHasher()

    def _encode(self, text: str) -> SparseVector:
        stemmer = get_english_stemmer()
        tokenizer = Bm25Tokenizer(stemmer, self._stopword_list, self.token_max_length)
        tokens = tokenizer.tokenize(text)

        if not tokens:
            return SparseVector(indices=[], values=[])

        doc_len = float(len(tokens))
        counts = Counter(
            _HashedToken(
                self._hasher.hash(token), token if self.include_tokens else None
            )
            for token in tokens
        )

        sorted_keys = sorted(counts.keys())
        indices: List[int] = []
        values: List[float] = []
        labels: Optional[List[str]] = [] if self.include_tokens else None

        for key in sorted_keys:
            tf = float(counts[key])
            denominator = tf + self.k * (
                1 - self.b + (self.b * doc_len) / self.avg_doc_length
            )
            score = tf * (self.k + 1) / denominator

            indices.append(key.hash)
            values.append(score)
            if labels is not None and key.label is not None:
                labels.append(key.label)

        return SparseVector(indices=indices, values=values, labels=labels)

    def __call__(self, input: Documents) -> SparseVectors:
        sparse_vectors: SparseVectors = []

        if not input:
            return sparse_vectors

        for document in input:
            sparse_vectors.append(self._encode(document))

        return sparse_vectors

    def embed_query(self, input: Documents) -> SparseVectors:
        return self.__call__(input)

    @staticmethod
    def name() -> str:
        return NAME

    @staticmethod
    def build_from_config(
        config: Dict[str, Any],
    ) -> "SparseEmbeddingFunction[Documents]":
        return ChromaBm25EmbeddingFunction(
            k=config.get("k", DEFAULT_K),
            b=config.get("b", DEFAULT_B),
            avg_doc_length=config.get("avg_doc_length", DEFAULT_AVG_DOC_LENGTH),
            token_max_length=config.get("token_max_length", DEFAULT_TOKEN_MAX_LENGTH),
            stopwords=config.get("stopwords"),
            include_tokens=config.get("include_tokens", False),
        )

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "k": self.k,
            "b": self.b,
            "avg_doc_length": self.avg_doc_length,
            "token_max_length": self.token_max_length,
            "include_tokens": self.include_tokens,
        }

        if self.stopwords is not None:
            config["stopwords"] = list(self.stopwords)

        return config

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        mutable_keys = {
            "k",
            "b",
            "avg_doc_length",
            "token_max_length",
            "stopwords",
            "include_tokens",
        }
        for key in new_config:
            if key not in mutable_keys:
                raise ValueError(f"Updating '{key}' is not supported for {NAME}")

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, NAME)
