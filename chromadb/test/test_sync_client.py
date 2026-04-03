from unittest.mock import MagicMock

import httpx
import pytest

from chromadb.errors import InvalidArgumentError
from chromadb.sync_client import (
    SyncClient,
    _parse_github_repository,
    _parse_s3_bucket_name,
    _validate_starting_url,
)
from typing import Dict, Literal

from chromadb.sync_types import (
    CreateGitHubInvocationArgs,
    CreateGitHubSourceArgs,
    CreateS3InvocationArgs,
    CreateS3SourceArgs,
    CreateWebInvocationArgs,
    CreateWebSourceArgs,
    DenseEmbeddingModel,
    GitHubSourceConfig,
    InvocationStatus,
    ListInvocationsOptions,
    OrderBy,
    S3SourceConfig,
    SparseEmbeddingModel,
    SyncEmbeddingConfig,
    DenseEmbeddingConfig,
    WebSourceConfig,
)


# --- Constructor ---


class TestConstructor:
    def test_throws_if_no_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHROMA_API_KEY", raising=False)
        with pytest.raises(InvalidArgumentError, match="Missing API key"):
            SyncClient()

    def test_accepts_api_key_via_constructor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CHROMA_API_KEY", raising=False)
        client = SyncClient(api_key="test-key")
        assert client is not None

    def test_accepts_api_key_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHROMA_API_KEY", "env-test-key")
        client = SyncClient()
        assert client is not None

    def test_accepts_custom_host(self) -> None:
        client = SyncClient(api_key="test-key", host="custom-sync.example.com")
        assert client is not None


# --- Method existence ---


class TestMethodsExist:
    @pytest.fixture(autouse=True)
    def _client(self) -> None:
        self.client = SyncClient(api_key="test-key")

    def test_source_methods(self) -> None:
        assert callable(self.client.list_sources)
        assert callable(self.client.create_github_source)
        assert callable(self.client.create_s3_source)
        assert callable(self.client.create_web_source)
        assert callable(self.client.get_source)
        assert callable(self.client.delete_source)

    def test_invocation_methods(self) -> None:
        assert callable(self.client.list_invocations)
        assert callable(self.client.get_invocation)
        assert callable(self.client.cancel_invocation)
        assert callable(self.client.create_invocation)
        assert callable(self.client.get_latest_invocations_by_keys)

    def test_system_methods(self) -> None:
        assert callable(self.client.health)


# --- GitHub repository parsing ---


class TestGitHubRepoParsing:
    def test_accepts_owner_repo_format(self) -> None:
        assert _parse_github_repository("chroma-core/chroma") == "chroma-core/chroma"

    def test_parses_github_url(self) -> None:
        assert (
            _parse_github_repository("https://github.com/chroma-core/chroma")
            == "chroma-core/chroma"
        )

    def test_parses_github_url_with_git_suffix(self) -> None:
        assert (
            _parse_github_repository("https://github.com/chroma-core/chroma.git")
            == "chroma-core/chroma"
        )

    def test_rejects_invalid_format(self) -> None:
        with pytest.raises(InvalidArgumentError, match='Expected "owner/repo"'):
            _parse_github_repository("not-valid")

    def test_rejects_non_github_urls(self) -> None:
        with pytest.raises(InvalidArgumentError):
            _parse_github_repository("https://gitlab.com/owner/repo")


# --- S3 bucket name parsing ---


class TestS3BucketNameParsing:
    def test_accepts_plain_bucket_name(self) -> None:
        assert _parse_s3_bucket_name("my-bucket") == "my-bucket"

    def test_parses_s3_uri(self) -> None:
        assert _parse_s3_bucket_name("s3://my-bucket/some/prefix") == "my-bucket"

    def test_parses_s3_arn(self) -> None:
        assert _parse_s3_bucket_name("arn:aws:s3:::my-bucket") == "my-bucket"


# --- Web URL validation ---


class TestWebUrlValidation:
    def test_accepts_valid_https_url(self) -> None:
        result = _validate_starting_url("https://docs.trychroma.com")
        assert result == "https://docs.trychroma.com"

    def test_rejects_invalid_url(self) -> None:
        with pytest.raises(InvalidArgumentError, match="Invalid starting URL"):
            _validate_starting_url("not a url")

    def test_rejects_non_http_protocols(self) -> None:
        with pytest.raises(InvalidArgumentError, match="Only http and https"):
            _validate_starting_url("ftp://example.com")


# --- Embedding model enums ---


class TestEmbeddingModelEnums:
    def test_dense_model_value(self) -> None:
        assert (
            DenseEmbeddingModel.QWEN3_EMBEDDING_06B.value == "Qwen/Qwen3-Embedding-0.6B"
        )

    def test_sparse_model_values(self) -> None:
        assert SparseEmbeddingModel.BM25.value == "Chroma/BM25"
        assert SparseEmbeddingModel.SPLADE_V1.value == "prithivida/Splade_PP_en_v1"


# --- API call integration (mocked HTTP) ---


def _mock_response(status_code: int = 200, json_data: object = None) -> httpx.Response:
    """Create a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = {"content-type": "application/json"}
    resp.json.return_value = json_data if json_data is not None else {}
    resp.raise_for_status = MagicMock()
    return resp


class TestSourceAPICalls:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.client = SyncClient(api_key="test-key")
        self.mock_request = MagicMock(
            return_value=_mock_response(200, {"source_id": "src-123"})
        )
        self.client._client.request = self.mock_request

    def test_create_github_source(self) -> None:
        result = self.client.create_github_source(
            CreateGitHubSourceArgs(
                database_name="test-db",
                github=GitHubSourceConfig(repository="chroma-core/chroma"),
                embedding=SyncEmbeddingConfig(
                    dense=DenseEmbeddingConfig(
                        model=DenseEmbeddingModel.QWEN3_EMBEDDING_06B
                    )
                ),
            )
        )
        assert result == {"source_id": "src-123"}
        self.mock_request.assert_called_once()
        call_args = self.mock_request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1] == "/api/v1/sources"

    def test_create_s3_source(self) -> None:
        result = self.client.create_s3_source(
            CreateS3SourceArgs(
                database_name="test-db",
                s3=S3SourceConfig(
                    bucket_name="my-bucket",
                    region="us-east-1",
                    collection_name="docs",
                    aws_credential_id=1,
                ),
            )
        )
        assert result == {"source_id": "src-123"}

    def test_create_web_source(self) -> None:
        result = self.client.create_web_source(
            CreateWebSourceArgs(
                database_name="test-db",
                web=WebSourceConfig(starting_url="https://docs.trychroma.com"),
            )
        )
        assert result == {"source_id": "src-123"}

    def test_list_sources(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.list_sources()
        assert result == []

    def test_get_source(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"id": "src-123", "database_name": "test-db"}
        )
        result = self.client.get_source("src-123")
        assert result["id"] == "src-123"

    def test_delete_source(self) -> None:
        self.mock_request.return_value = _mock_response(204)
        self.client.delete_source("src-123")
        call_args = self.mock_request.call_args
        assert call_args[0][0] == "DELETE"


class TestInvocationAPICalls:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.client = SyncClient(api_key="test-key")
        self.mock_request = MagicMock(
            return_value=_mock_response(200, {"invocation_id": "inv-456"})
        )
        self.client._client.request = self.mock_request

    def test_create_github_invocation(self) -> None:
        ref: Dict[Literal["branch"], str] = {"branch": "main"}
        result = self.client.create_invocation(
            "src-123",
            CreateGitHubInvocationArgs(
                target_collection_name="my-collection",
                ref_identifier=ref,
            ),
        )
        assert result == {"invocation_id": "inv-456"}
        call_args = self.mock_request.call_args
        assert "/api/v1/sources/src-123/invocations" in call_args[0][1]

    def test_create_s3_invocation(self) -> None:
        result = self.client.create_invocation(
            "src-123",
            CreateS3InvocationArgs(
                object_key="path/to/file.txt",
                custom_id="my-custom-id",
                metadata={"key": "value"},
            ),
        )
        assert result == {"invocation_id": "inv-456"}

    def test_create_web_invocation(self) -> None:
        result = self.client.create_invocation(
            "src-123",
            CreateWebInvocationArgs(target_collection_name="web-collection"),
        )
        assert result == {"invocation_id": "inv-456"}

    def test_list_invocations(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.list_invocations()
        assert result == []

    def test_list_invocations_with_filters(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.list_invocations(
            ListInvocationsOptions(
                source_id="src-123",
                status=InvocationStatus.PENDING,
                limit=10,
                order_by=OrderBy.ASC,
            )
        )
        assert result == []
        call_args = self.mock_request.call_args
        params = call_args[1]["params"]
        assert params["source_id"] == "src-123"
        assert params["status"] == "pending"
        assert params["limit"] == 10
        assert params["order_by"] == "ASC"

    def test_get_invocation(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"id": "inv-456", "status": "pending"}
        )
        result = self.client.get_invocation("inv-456")
        assert result["id"] == "inv-456"

    def test_cancel_invocation(self) -> None:
        self.mock_request.return_value = _mock_response(202, {})
        self.client.cancel_invocation("inv-456")
        call_args = self.mock_request.call_args
        assert call_args[0][0] == "PUT"

    def test_get_latest_invocations_by_keys(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"key1": {"id": "inv-1"}, "key2": {"id": "inv-2"}}
        )
        result = self.client.get_latest_invocations_by_keys("src-123", ["key1", "key2"])
        assert "key1" in result
        assert "key2" in result


class TestHealthCheck:
    def test_health(self) -> None:
        client = SyncClient(api_key="test-key")
        client._client.request = MagicMock(return_value=_mock_response(200))
        client.health()
