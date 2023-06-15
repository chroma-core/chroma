import logging
import sys
from unittest import mock

import pytest

from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

logger = logging.getLogger(__name__)


class TestSentenceTransformerEmbeddingFunction:
    error_message_require_sentence_transformers = (
        "The sentence_transformers python package is not installed."
        " Please install it with `pip install sentence_transformers`"
    )
    good_model_name = "all-MiniLM-L6-v2"

    def test__init__require_sentence_transformers(self) -> None:
        with mock.patch.dict("sys.modules", sentence_transformers=None):
            with pytest.raises(ValueError) as exc_info:
                SentenceTransformerEmbeddingFunction(model_name=self.good_model_name)
        assert self.error_message_require_sentence_transformers in str(exc_info.value)

    def test__init__with_good_model_name(self) -> None:
        if "sentence_transformers" in sys.modules:
            sent_trans_embed_func = SentenceTransformerEmbeddingFunction(
                model_name=self.good_model_name
            )
            assert sent_trans_embed_func.models.get(self.good_model_name) is not None
