import logging
import sys
from unittest import mock

import pytest

from chromadb.utils.embedding_functions import (
    SentenceTransformerEmbeddingFunction,
    Text2VecEmbeddingFunction,
)

logger = logging.getLogger(__name__)


class TestSentenceTransformerEmbeddingFunction:
    error_message_require_sentence_transformers = (
        "The sentence_transformers python package is not installed."
        " Please install it with `pip install sentence_transformers`"
    )
    good_model_name = "all-MiniLM-L6-v2"
    documents = ["document 1", "document 2", "document 3"]
    embedding_dim = 384

    # this is to add sentence_transformers to sys.modules
    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError:
        pass

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

    def test_callable_instances(self) -> None:
        if "sentence_transformers" in sys.modules:
            sent_trans_embed_func = SentenceTransformerEmbeddingFunction(
                model_name=self.good_model_name
            )
            assert callable(sent_trans_embed_func)
            embeddings = sent_trans_embed_func(self.documents)
            assert len(embeddings) == len(self.documents)
            for embedding in embeddings:
                assert len(embedding) == self.embedding_dim


class TestText2VecEmbeddingFunction:
    error_message_require_text2vec = (
        "The text2vec python package is not installed. "
        "Please install it with `pip install text2vec`"
    )
    good_model_name = "shibing624/text2vec-base-chinese"
    documents = ["如何更换花呗绑定银行卡", "花呗更改绑定银行卡"]
    embedding_dim = 768

    try:
        from text2vec import SentenceModel
    except ModuleNotFoundError:
        pass

    def test__init__require_text2vec(self) -> None:
        with mock.patch.dict("sys.modules", text2vec=None):
            with pytest.raises(ValueError) as exc_info:
                Text2VecEmbeddingFunction(model_name=self.good_model_name)
        assert self.error_message_require_text2vec in str(exc_info.value)

    def test__init__with_good_model_name(self) -> None:
        if "text2vec" in sys.modules:
            text2vec_embed_func = Text2VecEmbeddingFunction(
                model_name=self.good_model_name
            )
            assert text2vec_embed_func._model.model_name_or_path == self.good_model_name
            assert (
                text2vec_embed_func._model.get_sentence_embedding_dimension()
                == self.embedding_dim
            )

    def test_callable_instances(self) -> None:
        if "text2vec" in sys.modules:
            text2vec_embed_func = Text2VecEmbeddingFunction(
                model_name=self.good_model_name
            )
            assert callable(text2vec_embed_func)
            embeddings = text2vec_embed_func(self.documents)
            assert len(embeddings) == len(self.documents)
            for embedding in embeddings:
                assert len(embedding) == self.embedding_dim
