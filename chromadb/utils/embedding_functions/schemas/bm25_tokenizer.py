from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, List, Protocol, cast


DEFAULT_ENGLISH_STOPWORDS: List[str] = [
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "ain",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "aren",
    "aren't",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "couldn",
    "couldn't",
    "d",
    "did",
    "didn",
    "didn't",
    "do",
    "does",
    "doesn",
    "doesn't",
    "doing",
    "don",
    "don't",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "hadn",
    "hadn't",
    "has",
    "hasn",
    "hasn't",
    "have",
    "haven",
    "haven't",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "isn",
    "isn't",
    "it",
    "it's",
    "its",
    "itself",
    "just",
    "ll",
    "m",
    "ma",
    "me",
    "mightn",
    "mightn't",
    "more",
    "most",
    "mustn",
    "mustn't",
    "my",
    "myself",
    "needn",
    "needn't",
    "no",
    "nor",
    "not",
    "now",
    "o",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "re",
    "s",
    "same",
    "shan",
    "shan't",
    "she",
    "she's",
    "should",
    "should've",
    "shouldn",
    "shouldn't",
    "so",
    "some",
    "such",
    "t",
    "than",
    "that",
    "that'll",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "ve",
    "very",
    "was",
    "wasn",
    "wasn't",
    "we",
    "were",
    "weren",
    "weren't",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "won",
    "won't",
    "wouldn",
    "wouldn't",
    "y",
    "you",
    "you'd",
    "you'll",
    "you're",
    "you've",
    "your",
    "yours",
    "yourself",
    "yourselves",
]


DEFAULT_CHROMA_BM25_STOPWORDS: List[str] = list(DEFAULT_ENGLISH_STOPWORDS)


class SnowballStemmer(Protocol):
    def stem(self, token: str) -> str:  # pragma: no cover - protocol definition
        ...


class _SnowballStemmerAdapter:
    """Adapter that provides the uniform `stem` API used across languages."""

    def __init__(self) -> None:
        try:
            import snowballstemmer
        except ImportError:
            raise ValueError(
                "The snowballstemmer python package is not installed. Please install it with `pip install snowballstemmer`"
            )

        self._stemmer = snowballstemmer.stemmer("english")

    def stem(self, token: str) -> str:
        return cast(str, self._stemmer.stemWord(token))


@lru_cache(maxsize=1)
def get_english_stemmer() -> SnowballStemmer:
    """Return a cached Snowball stemmer for English."""

    return _SnowballStemmerAdapter()


class Bm25Tokenizer:
    """Tokenizer with stopword filtering and stemming used by BM25 embeddings."""

    def __init__(
        self,
        stemmer: SnowballStemmer,
        stopwords: Iterable[str],
        token_max_length: int,
    ) -> None:
        self._stemmer = stemmer
        self._stopwords = {word.lower() for word in stopwords}
        self._token_max_length = token_max_length
        self._non_alphanumeric_pattern = re.compile(r"[^\w\s]+", flags=re.UNICODE)

    def _remove_non_alphanumeric(self, text: str) -> str:
        return self._non_alphanumeric_pattern.sub(" ", text)

    @staticmethod
    def _simple_tokenize(text: str) -> List[str]:
        return [token for token in text.lower().split() if token]

    def tokenize(self, text: str) -> List[str]:
        cleaned = self._remove_non_alphanumeric(text)
        raw_tokens = self._simple_tokenize(cleaned)

        tokens: List[str] = []
        for token in raw_tokens:
            if token in self._stopwords:
                continue

            if len(token) > self._token_max_length:
                continue

            stemmed = self._stemmer.stem(token).strip()
            if stemmed:
                tokens.append(stemmed)

        return tokens


class Murmur3AbsHasher:
    def __init__(self, seed: int = 0) -> None:
        try:
            import mmh3
        except ImportError:
            raise ValueError(
                "The murmurhash3 python package is not installed. Please install it with `pip install murmurhash3`"
            )
        self.hasher = mmh3.hash
        self.seed = seed

    def hash(self, token: str) -> int:
        return cast(int, abs(self.hasher(token, seed=self.seed)))


__all__ = [
    "Bm25Tokenizer",
    "DEFAULT_CHROMA_BM25_STOPWORDS",
    "DEFAULT_ENGLISH_STOPWORDS",
    "SnowballStemmer",
    "get_english_stemmer",
    "Murmur3AbsHasher",
]
