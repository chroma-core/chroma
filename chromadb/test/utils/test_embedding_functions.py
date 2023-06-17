import logging
import os
import sys
from unittest import mock

import pytest

from chromadb.utils.embedding_functions import (
    OpenAIEmbeddingFunction,
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


class TestOpenAIEmbeddingFunction:
    error_message_require_openai = (
        "The openai python package is not installed. "
        "Please install it with `pip install openai`"
    )
    error_message_require_openai_api_key = (
        "Please provide an OpenAI API key. "
        "You can get one at https://platform.openai.com/account/api-keys"
    )
    api_key = os.getenv("OPENAI_API_KEY", default="thisisanapikey")
    organization_id = "organization_id"
    api_base = "api_base"
    api_type = "azure"
    embedding_dim = 1536

    documents = ["document \n 2"]
    openai_response = {
        "object": "list",
        "data": [
            {
                "object": "embedding",
                "embedding": embedding_dim
                * [
                    0.00230642,
                ],
                "index": 0,
            }
        ],
        "model": "text-embedding-ada-002",
        "usage": {"prompt_tokens": 2, "total_tokens": 2},
    }

    try:
        import openai
    except ModuleNotFoundError:
        pass

    def test__init__require_openai(self) -> None:
        with mock.patch.dict("sys.modules", openai=None):
            with pytest.raises(ValueError) as exc_info:
                OpenAIEmbeddingFunction()
        assert self.error_message_require_openai in str(exc_info.value)

    def test_openai_api_key_is_not_set(self) -> None:
        if "openai" in sys.modules:
            with pytest.raises(ValueError) as exc_info:
                OpenAIEmbeddingFunction()
            assert self.error_message_require_openai_api_key in str(exc_info.value)

    def test_openai_api_args_are_set(self) -> None:
        if "openai" in sys.modules:
            import openai

            OpenAIEmbeddingFunction(
                api_key=self.api_key,
                organization_id=self.organization_id,
                api_base=self.api_base,
                api_type=self.api_type,
            )

            assert openai.api_key == self.api_key
            assert openai.organization == self.organization_id
            assert openai.api_type == self.api_type
            assert openai.api_base == self.api_base

    def test_callable_instances(self) -> None:
        if "openai" in sys.modules:
            with mock.patch("openai.Embedding.create") as patched_call:
                patched_call.return_value = self.openai_response
                openai_embed_func = OpenAIEmbeddingFunction(api_key=self.api_key)
                assert callable(openai_embed_func)
                embeddings = openai_embed_func(texts=self.documents)
                assert len(embeddings) == len(self.documents)
                for embedding in embeddings:
                    assert len(embedding) == self.embedding_dim
