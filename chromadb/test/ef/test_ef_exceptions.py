import logging
import os
import random
import string
from typing import Generator, Optional

import pytest
from _pytest.logging import LogCaptureFixture

from chromadb import EmbeddingFunction
from chromadb.api import API
from chromadb.utils.embedding_functions import (
    SentenceTransformerEmbeddingFunction,
    DefaultEmbeddingFunction,
)


def generate_random_sentence(word_count: int = 1025) -> str:
    words = [
        "".join(random.choices(string.ascii_lowercase, k=random.randint(3, 10)))
        for _ in range(word_count)
    ]
    sentence = " ".join(words) + "."
    return sentence


def sentence_transformers_ef() -> Optional[EmbeddingFunction]:
    return SentenceTransformerEmbeddingFunction(model_name="all-mpnet-base-v2")


def default_ef() -> Optional[EmbeddingFunction]:
    return DefaultEmbeddingFunction()


@pytest.fixture(scope="function", params=[sentence_transformers_ef, default_ef])
def ef(request: pytest.FixtureRequest) -> Generator[EmbeddingFunction, None, None]:
    yield request.param()


def test_sentence_transformers_raise_exception(api: API, ef: EmbeddingFunction) -> None:
    api.reset()
    os.environ["CHROMA_STRICT_MODE"] = "true"
    col = api.get_or_create_collection("test", embedding_function=ef)
    with pytest.raises(ValueError) as e:
        col.add(
            ids=["1", "2"],
            documents=[generate_random_sentence(), "short doc"],
            metadatas=[{"test": "test"}, {"test": "test"}],
        )
    assert "The following documents exceed" in str(e)
    assert "[0]" in str(e)


def test_sentence_transformers_warning(
    api: API, ef: EmbeddingFunction, caplog: LogCaptureFixture
) -> None:
    api.reset()
    caplog.set_level(logging.DEBUG)
    os.environ["CHROMA_STRICT_MODE"] = "false"
    col = api.get_or_create_collection("test", embedding_function=ef)
    col.add(
        ids=["1", "2"],
        documents=[generate_random_sentence(), "short doc"],
        metadatas=[{"test": "test"}, {"test": "test"}],
    )
    print(caplog.text)
    assert "The following documents exceed" in caplog.text
