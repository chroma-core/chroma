import logging
import os
import random
import string
from typing import Generator, Optional

import pytest
from _pytest.logging import LogCaptureFixture
from dotenv import load_dotenv

from chromadb import EmbeddingFunction
from chromadb.utils.embedding_functions import (
    SentenceTransformerEmbeddingFunction,
    DefaultEmbeddingFunction,
    OpenAIEmbeddingFunction,
    HuggingFaceEmbeddingFunction,
    InstructorEmbeddingFunction,
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


def instructor_ef() -> Optional[EmbeddingFunction]:
    return InstructorEmbeddingFunction()


def default_ef() -> Optional[EmbeddingFunction]:
    return DefaultEmbeddingFunction()


def openai_ef() -> Optional[EmbeddingFunction]:
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key is None:
        return None
    return OpenAIEmbeddingFunction(api_key=api_key)


def huggingface_ef() -> Optional[EmbeddingFunction]:
    load_dotenv()
    api_key = os.getenv("HUGGINGFACE_API_KEY")
    if api_key is None:
        return None
    return HuggingFaceEmbeddingFunction(
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        api_key=api_key,
    )


@pytest.fixture(
    scope="function",
    params=[
        sentence_transformers_ef,
        default_ef,
        huggingface_ef,
        instructor_ef,
    ],
)
def ef(request: pytest.FixtureRequest) -> Generator[EmbeddingFunction, None, None]:
    try:
        if request.param() is None:
            pytest.skip("No API key provided for this embedding function")
        yield request.param()
    except Exception as e:
        pytest.skip(
            f"Unable to instantiate embedding function, probably due to missing dependencies: {e}"
        )


def test_sentence_transformers_warning(
    ef: EmbeddingFunction, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    text = generate_random_sentence()
    if isinstance(ef, OpenAIEmbeddingFunction):
        text = generate_random_sentence(8000)
    try:
        ef([text])
    except Exception as e:
        # ignore OAI max context length error
        if "This model's maximum context length is 8192 tokens" not in str(e):
            raise e
    assert "WARNING: The following document exceed" in caplog.text
