import shutil
import os
from typing import List, Hashable

import hypothesis.strategies as st
import onnxruntime
import pytest
from hypothesis import given, settings

from chromadb.utils.embedding_functions import (
    ONNXMiniLM_L6_V2,
    _verify_sha256,
    OpenAIEmbeddingFunction,
)


def test_with_embedding_dimensions() -> None:
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip("OPENAI_API_KEY not set")
    ef = OpenAIEmbeddingFunction(
        api_key=os.environ["OPENAI_API_KEY"],
        model_name="text-embedding-3-small",
        dimensions=64,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 64


def test_with_embedding_dimensions_not_working_with_old_model() -> None:
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip("OPENAI_API_KEY not set")
    ef = OpenAIEmbeddingFunction(api_key=os.environ["OPENAI_API_KEY"], dimensions=64)
    with pytest.raises(
        Exception, match="This model does not support specifying dimensions"
    ):
        ef(["hello world"])
