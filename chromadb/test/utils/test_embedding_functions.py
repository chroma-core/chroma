import json
import os
import sys
from pathlib import Path
from typing import Any, Type
from unittest import mock

import numpy as np
import pytest

from chromadb.api.types import Documents, EmbeddingFunction

from chromadb.utils.embedding_functions import (
    CohereEmbeddingFunction,
    GooglePalmEmbeddingFunction,
    GoogleVertexEmbeddingFunction,
    HuggingFaceEmbeddingFunction,
    InstructorEmbeddingFunction,
    ONNXMiniLM_L6_V2,
    OpenAIEmbeddingFunction,
    SentenceTransformerEmbeddingFunction,
    Text2VecEmbeddingFunction,
)


class _TestEmbeddingFunction:
    required_package: str
    good_model_name: str
    documents: Documents
    embedding_dim: int
    embedding_function: Type[EmbeddingFunction]
    patched_method: str
    patched_response: Any
    api_key: str = ""

    def coin_required_package_error_message(self) -> str:
        return (
            f"The {self.required_package} python package is not installed. "
            f"Please install it with `pip install {self.required_package}`"
        )

    def _test__init__requires_package(self) -> None:
        with mock.patch.dict("sys.modules", {self.required_package: None}):
            with pytest.raises(ValueError) as exc_info:
                if self.api_key:
                    self.embedding_function(
                        model_name=self.good_model_name, api_key=self.api_key  # type: ignore
                    )
                else:
                    self.embedding_function(model_name=self.good_model_name)  # type: ignore
        assert self.coin_required_package_error_message() in str(exc_info.value)

    def _test_callable_instances(self) -> None:
        if self.required_package in sys.modules:
            with mock.patch(self.patched_method) as patched_call:
                patched_call.return_value = self.patched_response
                if self.api_key:
                    embed_func = self.embedding_function(
                        model_name=self.good_model_name, api_key=self.api_key  # type: ignore
                    )
                else:
                    embed_func = self.embedding_function(
                        model_name=self.good_model_name  # type: ignore
                    )
                assert callable(embed_func)
                embeddings = embed_func(self.documents)
                assert len(embeddings) == len(self.documents)
                for embedding in embeddings:
                    assert len(embedding) == self.embedding_dim


@pytest.mark.requires("sentence_transformers")
class TestSentenceTransformerEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "sentence_transformers"
    # By flushing this attribute there will be no model to download. And therefore no need to mock SentenceTransformer
    good_model_name = ""  # "all-MiniLM-L6-v2"
    documents = ["document 1", "document 2", "document 3"]
    embedding_dim = 42
    embedding_function = SentenceTransformerEmbeddingFunction
    patched_method = f"{required_package}.SentenceTransformer.encode"
    patched_response = 0.05082 * np.ones((len(documents), embedding_dim))

    # this is to add sentence_transformers to sys.modules
    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError:
        pass

    def test__init__requires_sentence_transformers(self) -> None:
        self._test__init__requires_package()

    def test__init__with_good_model_name(self) -> None:
        if self.required_package in sys.modules:
            sent_trans_embed_func = self.embedding_function(
                model_name=self.good_model_name
            )
            assert sent_trans_embed_func.models.get(self.good_model_name) is not None

    def test_embeddings(self) -> None:
        self._test_callable_instances()


@pytest.mark.requires("text2vec")
class TestText2VecEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "text2vec"
    good_model_name = "shibing624/text2vec-base-chinese"
    documents = ["如何更换花呗绑定银行卡", "花呗更改绑定银行卡"]
    embedding_dim = 15
    embedding_function = Text2VecEmbeddingFunction
    patched_method = f"{required_package}.SentenceModel.encode"
    patched_response = -0.74025 * np.ones((len(documents), embedding_dim))

    try:
        from text2vec import SentenceModel
    except ModuleNotFoundError:
        pass

    def test__init__requires_text2vec(self) -> None:
        self._test__init__requires_package()

    def test__init__with_good_model_name(self) -> None:
        if self.required_package in sys.modules:
            # This is to avoid the need of downloading or pre-loading models and with this, tests run fast
            # Unit tests should run isolated
            # One can argue that it's more suitable to consider these tests as integration tests
            with mock.patch("text2vec.SentenceModel") as patched_call:
                patched_call.return_value = mock.MagicMock(
                    model_name_or_path=self.good_model_name,
                    get_sentence_embedding_dimension=lambda: self.embedding_dim,
                )
                text2vec_embed_func = self.embedding_function(
                    model_name=self.good_model_name
                )
                assert (
                    text2vec_embed_func._model.model_name_or_path
                    == self.good_model_name
                )
                assert (
                    text2vec_embed_func._model.get_sentence_embedding_dimension()
                    == self.embedding_dim
                )

    def test_embeddings(self) -> None:
        with mock.patch("text2vec.SentenceModel") as patched_call_:
            patched_call_.return_value = mock.MagicMock(
                encode=lambda *args, **kwargs: self.patched_response
            )
            self._test_callable_instances()


@pytest.mark.requires("openai")
class TestOpenAIEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "openai"
    error_message_requires_openai_api_key = (
        "Please provide an OpenAI API key. "
        "You can get one at https://platform.openai.com/account/api-keys"
    )
    api_key = os.getenv("OPENAI_API_KEY", default="thisisanapikey")
    organization_id = "organization_id"
    api_base = "api_base"
    api_type = "azure"

    good_model_name = "text-embedding-ada-002"
    embedding_function = OpenAIEmbeddingFunction
    embedding_dim = 17
    documents = ["document \n 2"]
    patched_method = f"{required_package}.Embedding.create"
    patched_response = {
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

    def test__init__requires_openai(self) -> None:
        self._test__init__requires_package()

    def test_openai_api_key_is_not_set(self) -> None:
        if self.required_package in sys.modules:
            with pytest.raises(ValueError) as exc_info:
                self.embedding_function()
            assert self.error_message_requires_openai_api_key in str(exc_info.value)

    def test_openai_api_attribs_are_set(self) -> None:
        if self.required_package in sys.modules:
            import openai

            self.embedding_function(
                api_key=self.api_key,
                organization_id=self.organization_id,
                api_base=self.api_base,
                api_type=self.api_type,
            )

            assert openai.api_key == self.api_key
            assert openai.organization == self.organization_id
            assert openai.api_type == self.api_type
            assert openai.api_base == self.api_base

    def test_embeddings(self) -> None:
        self._test_callable_instances()


@pytest.mark.requires("cohere")
class TestCohereEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "cohere"
    good_model_name = "large"
    # TODO: May be it's more suitable to throw an error with a custom message
    #   if the `api_key` was not provided
    api_key = os.getenv("COHERE_TOKEN", default="thisisanapikey")
    documents = ["document 1", "document 2"]
    embedding_dim = 13
    embedding_function = CohereEmbeddingFunction
    cohere_api_response = {
        "id": "7f37d160-945a-4aba-a3cd-3da4d3552eac",
        "texts": documents,
        "embeddings": [
            embedding_dim * [0.71386],
            embedding_dim * [-0.40307],
        ],
        "meta": {"api_version": {"version": "1"}},
    }
    patched_method = f"{required_package}.Client.embed"
    patched_response = cohere_api_response.get("embeddings")

    try:
        import cohere
    except ModuleNotFoundError:
        pass

    def test__init__requires_cohere(self) -> None:
        self._test__init__requires_package()

    # TODO: getting cohore embeddings can be achieved by :
    #  `self._client.embed(texts=texts, model=self._model_name).embeddings` without adding a new loop.
    #   I am using cohere 4.11.2.
    #   May be embedding_functions.py needs some refactoring.
    #   It will be very useful if someone can add a requirements file concerning :
    #   openai, cohere, sentence_transformers, etc.
    def test_embeddings(self) -> None:
        self._test_callable_instances()


@pytest.mark.requires("requests")
class TestHuggingFaceEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "requests"
    api_key = os.getenv("HUGGINGFACE_TOKEN", default="thisisanapikey")
    good_model_name = "sentence-transformers/all-MiniLM-L6-v2"
    api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{good_model_name}"
    headers = {"Authorization": f"Bearer {api_key}"}
    documents = ["document 1", "document 2"]
    embedding_function = HuggingFaceEmbeddingFunction
    embedding_dim = 10
    embeddings = [
        embedding_dim * [0.020755],
        embedding_dim * [-0.00542279],
    ]

    try:
        import requests

        huggingface_api_response = requests.models.Response()
        huggingface_api_response._content = f"{embeddings}".encode("ascii")
        patched_method = f"{required_package}.Session.post"
        patched_response = huggingface_api_response

    except ModuleNotFoundError:
        pass

    def test__init__requires_requests(self) -> None:
        self._test__init__requires_package()

    def test_huggingface_api_attribs_are_set(self) -> None:
        if self.required_package in sys.modules:
            huggingface_embed_func = self.embedding_function(api_key=self.api_key)
            assert huggingface_embed_func._api_url == self.api_url
            assert huggingface_embed_func._session.headers.get(
                "Authorization"
            ) == self.headers.get("Authorization")

    def test_embeddings(self) -> None:
        self._test_callable_instances()


@pytest.mark.requires("InstructorEmbedding")
class TestInstructorEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "InstructorEmbedding"
    good_model_name = ""
    instruction = "instruction"
    documents = ["document 1", "document 2"]
    embedding_dim = 11
    embedding_function = InstructorEmbeddingFunction
    patched_method = f"{required_package}.INSTRUCTOR.encode"
    patched_response = 0.750026 * np.ones((len(documents), embedding_dim))

    try:
        from InstructorEmbedding import INSTRUCTOR
    except ModuleNotFoundError:
        pass

    def test__init__requires_instructor(self) -> None:
        self._test__init__requires_package()

    def test_instructor_attribs_are_set(self) -> None:
        if self.required_package in sys.modules:
            from InstructorEmbedding import INSTRUCTOR

            instructor_embed_func = self.embedding_function(
                model_name=self.good_model_name, instruction=self.instruction
            )
            assert isinstance(instructor_embed_func._model, INSTRUCTOR)
            assert instructor_embed_func._instruction == self.instruction

    @pytest.mark.parametrize("instruction", [None, instruction])
    def test_embeddings(self, instruction: str) -> None:
        if self.required_package in sys.modules:
            with mock.patch(self.patched_method) as patched_call:
                patched_call.return_value = self.patched_response
                embed_func = self.embedding_function(
                    model_name=self.good_model_name, instruction=instruction
                )
                assert callable(embed_func)
                embeddings = embed_func(self.documents)
                assert len(embeddings) == len(self.documents)
                for embedding in embeddings:
                    assert len(embedding) == self.embedding_dim


@pytest.mark.requires(["onnxruntime", "tokenizers", "tqdm"])
class TestONNXMiniLM_L6_V2(_TestEmbeddingFunction):
    required_packages = ["onnxruntime", "tokenizers", "tqdm"]
    good_model_name = "all-MiniLM-L6-v2"
    documents = ["The cat sat on the mat", "The dog sits on the mat"]
    embedding_function = ONNXMiniLM_L6_V2
    embedding_dim = 384
    extracted_filenames = {
        "config.json",
        "model.onnx",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "vocab.txt",
    }

    def test__init__requires_packages(self) -> None:
        for package in self.required_packages:
            self.required_package = package
            with mock.patch.dict("sys.modules", {self.required_package: None}):
                with pytest.raises(ValueError) as exc_info:
                    self.embedding_function()
            assert self.coin_required_package_error_message() in str(exc_info.value)

    def test_model_is_downloaded(self) -> None:
        model = self.embedding_function()
        model._download_model_if_not_exists()
        assert Path(model.DOWNLOAD_PATH / model.ARCHIVE_FILENAME).exists()
        extracted_files = list(
            Path(model.DOWNLOAD_PATH / model.EXTRACTED_FOLDER_NAME).glob("**/*")
        )
        assert len(extracted_files) == len(self.extracted_filenames)

        for extracted_file in extracted_files:
            assert extracted_file.name in self.extracted_filenames

    def test_embeddings_are_normalized(self) -> None:
        model = self.embedding_function()
        model._init_model_and_tokenizer()
        embeddings = model._forward(self.documents)
        for embedding in embeddings:
            assert abs(np.linalg.norm(embedding) - 1) < 0.0001

    def test_embeddings(self) -> None:
        model = self.embedding_function()
        model._init_model_and_tokenizer()
        embeddings = model(self.documents)
        assert len(embeddings) == len(self.documents)
        for embedding in embeddings:
            assert len(embedding) == self.embedding_dim

    # Here we expect that the produced embeddings are capturing some semantics
    # We will just compute the cosine similarity and we are expecting a score greater than 0.5
    # The cosine similarity, in our case is just the dot product, because the vectors are normalized
    def test_embeddings_quality(self) -> None:
        model = self.embedding_function()
        model._init_model_and_tokenizer()
        embeddings = model._forward(self.documents)
        assert embeddings[0] @ embeddings[1] > 0.5


@pytest.mark.requires(["google-generativeai"])
class TestGooglePalmEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "google.generativeai"
    error_message_requires_api_key = "Please provide a PaLM API key."
    error_message_requires_model_name = "Please provide the model name."
    error_message_requires_package = (
        "The Google Generative AI python package is not installed. "
        "Please install it with `pip install google-generativeai`"
    )
    good_model_name = ""
    api_key = os.getenv("PALM_TOKEN", default="thisisanapikey")
    embedding_function = GooglePalmEmbeddingFunction
    documents = ["document 1", "document 2"]
    embedding_dim = 15
    patched_method = f"{required_package}.generate_embeddings"
    patched_response = dict(embedding=embedding_dim * [0.020755])

    try:
        import google.generativeai as palm
    except ModuleNotFoundError:
        pass

    def test_api_key_is_not_set(self) -> None:
        if self.required_package in sys.modules:
            with pytest.raises(ValueError) as exc_info:
                self.embedding_function(api_key="")
            assert self.error_message_requires_api_key in str(exc_info.value)

    def test_model_name_is_not_provided(self) -> None:
        if self.required_package in sys.modules:
            with pytest.raises(ValueError) as exc_info:
                self.embedding_function(
                    api_key=self.api_key, model_name=self.good_model_name
                )
            assert self.error_message_requires_model_name in str(exc_info.value)

    def test__init__requires_palm(self) -> None:
        with mock.patch.dict("sys.modules", {self.required_package: None}):
            with pytest.raises(ValueError) as exc_info:
                self.embedding_function(api_key=self.api_key)
        assert self.error_message_requires_package in str(exc_info.value)

    def test_embeddings(self) -> None:
        self.good_model_name = "models/embedding-gecko-001"
        self._test_callable_instances()


class TestGoogleVertexEmbeddingFunction(_TestEmbeddingFunction):
    required_package = "requests"
    api_key = os.getenv("VORTEX_TOKEN", default="thisisanapikey")
    good_model_name = "textembedding-gecko-001"
    project_id = "cloud-large-language-models"
    region = "us-central1"
    api_url = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{region}/endpoints/{good_model_name}:predict"
    headers = {"Authorization": f"Bearer {api_key}"}
    documents = ["document 1", "document 2"]
    embedding_function = GoogleVertexEmbeddingFunction
    embedding_dim = 10
    embeddings = {
        "predictions": [embedding_dim * [0.020755], embedding_dim * [-0.00542279]]
    }

    try:
        import requests

        vortex_api_response = requests.models.Response()
        vortex_api_response._content = json.dumps(embeddings).encode(
            "ascii"
        )  # to parse correctly json
        patched_method = f"{required_package}.Session.post"
        patched_response = vortex_api_response

    except ModuleNotFoundError:
        pass

    def test_vortex_api_attribs_are_set(self) -> None:
        if self.required_package in sys.modules:
            vortex_embed_func = self.embedding_function(api_key=self.api_key)
            assert vortex_embed_func._api_url == self.api_url
            assert vortex_embed_func._session.headers.get(
                "Authorization"
            ) == self.headers.get("Authorization")

    def test_embeddings(self) -> None:
        self._test_callable_instances()
        # if predictions are None
        if self.required_package in sys.modules:
            self.vortex_api_response._content = json.dumps("").encode("ascii")
            self.documents = []
            self.embedding_dim = 0
            self._test_callable_instances()
