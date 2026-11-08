import os
import sys
from typing import Any, Dict, List
from unittest.mock import MagicMock

import numpy as np
import pytest
from pytest import MonkeyPatch

from chromadb.utils.embedding_functions.oci_genai_embedding_function import (
    OCIGenAIEmbeddingFunction,
)

COMPARTMENT_ID = "ocid1.compartment.oc1..exampleuniqueid"


def _fake_response(n: int, dims: int = 4) -> MagicMock:
    response = MagicMock()
    response.data.embeddings = [[float(i)] * dims for i in range(n)]
    return response


@pytest.fixture
def mock_oci(monkeypatch: MonkeyPatch, tmp_path: Any) -> MagicMock:
    """Install a fake ``oci`` module so the EF can be built without credentials."""
    oci = MagicMock()
    token_file = tmp_path / "token"
    token_file.write_text("session-token\n")

    def embed_text(details: Any, **kwargs: Any) -> MagicMock:
        return _fake_response(len(details.inputs))

    client = MagicMock()
    client.embed_text.side_effect = embed_text
    oci.generative_ai_inference.GenerativeAiInferenceClient.return_value = client

    def embed_text_details(**kwargs: Any) -> MagicMock:
        details = MagicMock()
        for key, value in kwargs.items():
            setattr(details, key, value)
        return details

    oci.generative_ai_inference.models.EmbedTextDetails.side_effect = embed_text_details
    oci.config.from_file.return_value = {
        "region": "us-chicago-1",
        "key_file": "/tmp/key.pem",
        "security_token_file": str(token_file),
    }
    monkeypatch.setitem(sys.modules, "oci", oci)
    return oci


def _calls(mock_oci: MagicMock) -> List[Dict[str, Any]]:
    return [
        call.kwargs
        for call in mock_oci.generative_ai_inference.models.EmbedTextDetails.call_args_list
    ]


def test_missing_oci_package_gives_install_hint(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "oci", None)
    with pytest.raises(ValueError, match="pip install oci"):
        OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID)


def test_compartment_id_is_required(mock_oci: MagicMock) -> None:
    with pytest.raises(ValueError, match="compartment_id is required"):
        OCIGenAIEmbeddingFunction()


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"auth_type": "PASSWORD"}, "Unsupported auth_type"),
        ({"truncate": "MIDDLE"}, "Unsupported truncate"),
        ({"input_type": "RERANK"}, "Unsupported input_type"),
        ({"output_dimensions": 0}, "output_dimensions must be a positive integer"),
    ],
)
def test_invalid_arguments_are_rejected(
    mock_oci: MagicMock, kwargs: Dict[str, Any], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID, **kwargs)


def test_default_config_roundtrip_and_schema(mock_oci: MagicMock) -> None:
    ef = OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID)
    assert ef.name() == "oci_genai"
    assert ef.default_space() == "cosine"

    config = ef.get_config()
    assert config == {
        "model_name": "cohere.embed-v4.0",
        "compartment_id": COMPARTMENT_ID,
        "service_endpoint": None,
        "auth_type": "API_KEY",
        "auth_profile": "DEFAULT",
        "auth_file_location": "~/.oci/config",
        "truncate": "END",
        "input_type": None,
        "output_dimensions": None,
    }
    OCIGenAIEmbeddingFunction.validate_config(config)

    rebuilt = OCIGenAIEmbeddingFunction.build_from_config(config)
    assert rebuilt.get_config() == config


def test_custom_config_roundtrip_and_schema(mock_oci: MagicMock) -> None:
    ef = OCIGenAIEmbeddingFunction(
        model_name="cohere.embed-multilingual-v3.0",
        compartment_id=COMPARTMENT_ID,
        service_endpoint="https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
        auth_type="security_token",
        auth_profile="MY_PROFILE",
        auth_file_location="/etc/oci/config",
        truncate="start",
        input_type="clustering",
        output_dimensions=512,
    )
    config = ef.get_config()
    assert config["auth_type"] == "SECURITY_TOKEN"
    assert config["truncate"] == "START"
    assert config["input_type"] == "CLUSTERING"
    OCIGenAIEmbeddingFunction.validate_config(config)

    rebuilt = OCIGenAIEmbeddingFunction.build_from_config(config)
    assert rebuilt.get_config() == config


def test_schema_rejects_unknown_and_invalid_values() -> None:
    from jsonschema import ValidationError

    base = {"model_name": "cohere.embed-v4.0", "compartment_id": COMPARTMENT_ID}
    with pytest.raises(ValidationError):
        OCIGenAIEmbeddingFunction.validate_config({**base, "api_key": "secret"})
    with pytest.raises(ValidationError):
        OCIGenAIEmbeddingFunction.validate_config({**base, "auth_type": "PASSWORD"})
    with pytest.raises(ValidationError):
        OCIGenAIEmbeddingFunction.validate_config({"model_name": "cohere.embed-v4.0"})


def test_build_from_config_requires_model_and_compartment(mock_oci: MagicMock) -> None:
    with pytest.raises(ValueError, match="requires 'model_name' and 'compartment_id'"):
        OCIGenAIEmbeddingFunction.build_from_config({"model_name": "cohere.embed-v4.0"})


def test_validate_config_update(mock_oci: MagicMock) -> None:
    ef = OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID)
    with pytest.raises(ValueError, match="model name cannot be changed"):
        ef.validate_config_update(ef.get_config(), {"model_name": "other"})
    with pytest.raises(ValueError, match="output dimensions cannot be changed"):
        ef.validate_config_update(ef.get_config(), {"output_dimensions": 256})
    ef.validate_config_update(ef.get_config(), {"truncate": "START"})


def test_documents_and_queries_use_different_input_types(mock_oci: MagicMock) -> None:
    ef = OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID)

    docs = ef(["doc one", "doc two"])
    assert len(docs) == 2
    for embedding in docs:
        assert isinstance(embedding, np.ndarray)
        assert np.asarray(embedding).dtype == np.float32

    queries = ef.embed_query(["question?"])
    assert len(queries) == 1

    calls = _calls(mock_oci)
    assert [c["input_type"] for c in calls] == ["SEARCH_DOCUMENT", "SEARCH_QUERY"]
    assert calls[0]["inputs"] == ["doc one", "doc two"]
    assert calls[0]["compartment_id"] == COMPARTMENT_ID
    assert calls[0]["truncate"] == "END"
    mock_oci.generative_ai_inference.models.OnDemandServingMode.assert_called_with(
        model_id="cohere.embed-v4.0"
    )


def test_pinned_input_type_applies_to_documents_and_queries(
    mock_oci: MagicMock,
) -> None:
    ef = OCIGenAIEmbeddingFunction(
        compartment_id=COMPARTMENT_ID, input_type="CLASSIFICATION"
    )
    ef(["a"])
    ef.embed_query(["b"])
    assert [c["input_type"] for c in _calls(mock_oci)] == [
        "CLASSIFICATION",
        "CLASSIFICATION",
    ]


def test_inputs_are_batched_to_the_service_limit(mock_oci: MagicMock) -> None:
    ef = OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID, output_dimensions=256)
    texts = [f"text {i}" for i in range(200)]

    embeddings = ef(texts)

    assert len(embeddings) == 200
    calls = _calls(mock_oci)
    assert [len(c["inputs"]) for c in calls] == [96, 96, 8]
    assert calls[0]["inputs"][0] == "text 0"
    assert calls[2]["inputs"][-1] == "text 199"
    # output_dimensions is set on every request
    client = mock_oci.generative_ai_inference.GenerativeAiInferenceClient.return_value
    for call in client.embed_text.call_args_list:
        assert call.args[0].output_dimensions == 256


def test_api_key_auth_uses_config_file(mock_oci: MagicMock) -> None:
    OCIGenAIEmbeddingFunction(
        compartment_id=COMPARTMENT_ID,
        auth_profile="MY_PROFILE",
        auth_file_location="/etc/oci/config",
        service_endpoint="https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
    )
    mock_oci.config.from_file.assert_called_once_with(
        file_location="/etc/oci/config", profile_name="MY_PROFILE"
    )
    kwargs = (
        mock_oci.generative_ai_inference.GenerativeAiInferenceClient.call_args.kwargs
    )
    assert "signer" not in kwargs
    assert (
        kwargs["service_endpoint"]
        == "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com"
    )
    assert kwargs["retry_strategy"] is mock_oci.retry.DEFAULT_RETRY_STRATEGY


def test_security_token_auth_builds_token_signer(mock_oci: MagicMock) -> None:
    OCIGenAIEmbeddingFunction(compartment_id=COMPARTMENT_ID, auth_type="SECURITY_TOKEN")

    mock_oci.signer.load_private_key_from_file.assert_called_once_with(
        "/tmp/key.pem", None
    )
    mock_oci.auth.signers.SecurityTokenSigner.assert_called_once_with(
        "session-token", mock_oci.signer.load_private_key_from_file.return_value
    )
    kwargs = (
        mock_oci.generative_ai_inference.GenerativeAiInferenceClient.call_args.kwargs
    )
    assert kwargs["signer"] is mock_oci.auth.signers.SecurityTokenSigner.return_value


def test_security_token_auth_requires_token_profile(mock_oci: MagicMock) -> None:
    mock_oci.config.from_file.return_value = {"region": "us-chicago-1"}
    with pytest.raises(ValueError, match="has no security_token_file"):
        OCIGenAIEmbeddingFunction(
            compartment_id=COMPARTMENT_ID, auth_type="SECURITY_TOKEN"
        )


def test_principal_auth_types_use_principal_signers(mock_oci: MagicMock) -> None:
    OCIGenAIEmbeddingFunction(
        compartment_id=COMPARTMENT_ID, auth_type="INSTANCE_PRINCIPAL"
    )
    mock_oci.auth.signers.InstancePrincipalsSecurityTokenSigner.assert_called_once()

    OCIGenAIEmbeddingFunction(
        compartment_id=COMPARTMENT_ID, auth_type="RESOURCE_PRINCIPAL"
    )
    mock_oci.auth.signers.get_resource_principals_signer.assert_called_once()

    # principals never read the config file
    mock_oci.config.from_file.assert_not_called()


# --- Live tests: run only when OCI credentials and a compartment are configured. ---
#
#   OCI_COMPARTMENT_ID          OCID of the compartment allowed to call Generative AI
#   OCI_CONFIG_PROFILE          profile in ~/.oci/config (default: DEFAULT)
#   OCI_AUTH_TYPE               API_KEY (default) or SECURITY_TOKEN
#   OCI_GENAI_SERVICE_ENDPOINT  optional inference endpoint override


def _live_ef(**kwargs: Any) -> OCIGenAIEmbeddingFunction:
    pytest.importorskip("oci", reason="oci not installed")
    if os.environ.get("OCI_COMPARTMENT_ID") is None:
        pytest.skip("OCI_COMPARTMENT_ID not set")
    return OCIGenAIEmbeddingFunction(
        compartment_id=os.environ["OCI_COMPARTMENT_ID"],
        auth_profile=os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT"),
        auth_type=os.environ.get("OCI_AUTH_TYPE", "API_KEY"),
        service_endpoint=os.environ.get("OCI_GENAI_SERVICE_ENDPOINT"),
        **kwargs,
    )


def test_live_embed_v4_default_dimensions() -> None:
    ef = _live_ef()
    embeddings = ef(["hello world", "how are you?"])
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 1536
    assert not np.allclose(embeddings[0], embeddings[1])


def test_live_embed_v4_output_dimensions() -> None:
    ef = _live_ef(output_dimensions=256)
    embeddings = ef(["hello world"])
    assert len(embeddings[0]) == 256


def test_live_embed_multilingual_v3() -> None:
    ef = _live_ef(model_name="cohere.embed-multilingual-v3.0")
    embeddings = ef(["Hola desde OCI", "Bonjour depuis OCI"])
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 1024


def test_live_query_embeddings_differ_from_document_embeddings() -> None:
    ef = _live_ef()
    text = ["what is a vector database?"]
    doc_embedding = ef(text)[0]
    query_embedding = ef.embed_query(text)[0]
    assert len(doc_embedding) == len(query_embedding)
    assert not np.allclose(doc_embedding, query_embedding)
