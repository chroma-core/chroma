from typing import Dict, Literal, Optional
from unittest.mock import MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from chromadb import CloudClient, _CloudClient
from chromadb.auth import UserIdentity
from chromadb.config import Settings
from chromadb.errors import InvalidArgumentError
from chromadb.sync_client import (
    _parse_github_repository,
    _parse_s3_bucket_name,
    _validate_starting_url,
)
from chromadb.sync_types import (
    CreateGitHubInvocationArgs,
    CreateGitHubSourceArgs,
    CreateS3InvocationArgs,
    CreateS3SourceArgs,
    CreateWebInvocationArgs,
    CreateWebSourceArgs,
    DenseEmbeddingConfig,
    DenseEmbeddingModel,
    GitHubSourceConfig,
    InvocationStatus,
    ListInvocationsOptions,
    OrderBy,
    S3SourceConfig,
    SparseEmbeddingModel,
    SyncEmbeddingConfig,
    WebSourceConfig,
)
from chromadb.types import Database, Tenant


def make_cloud_client(
    tenant: Optional[str] = None,
    database: Optional[str] = None,
    api_key: Optional[str] = None,
    settings: Optional[Settings] = None,
    *,
    cloud_host: str = "api.trychroma.com",
    cloud_port: int = 443,
    enable_ssl: bool = True,
    sync_host: str = "sync.trychroma.com",
) -> _CloudClient:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        database_name = str(database or "test-db")
        tenant_name = str(tenant or "default_tenant")

        mock_get_user_identity.return_value = UserIdentity(
            user_id="test-user",
            tenant=tenant_name,
            databases=[database_name],
        )
        mock_get_tenant.return_value = Tenant(name=tenant_name)
        mock_get_database.return_value = Database(
            id=uuid4(),
            name=database_name,
            tenant=tenant_name,
        )

        return CloudClient(
            tenant=tenant,
            database=database,
            api_key=api_key,
            settings=settings,
            cloud_host=cloud_host,
            cloud_port=cloud_port,
            enable_ssl=enable_ssl,
            sync_host=sync_host,
        )


def _mock_response(status_code: int = 200, json_data: object = None) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = {"content-type": "application/json"}
    resp.json.return_value = json_data if json_data is not None else {}
    resp.raise_for_status = MagicMock()
    return resp


class TestConstructor:
    def test_throws_if_no_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHROMA_API_KEY", raising=False)
        with pytest.raises(ValueError, match="Missing required arguments: api_key"):
            make_cloud_client(database="test-db")

    def test_accepts_api_key_via_constructor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CHROMA_API_KEY", raising=False)
        client = make_cloud_client(api_key="test-key", database="test-db")
        assert client is not None
        assert client.sync is not None

    def test_accepts_api_key_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHROMA_API_KEY", "env-test-key")
        client = make_cloud_client(database="test-db")
        assert client is not None
        assert client.sync is not None

    def test_accepts_custom_sync_host(self) -> None:
        client = make_cloud_client(
            api_key="test-key",
            database="test-db",
            sync_host="custom-sync.example.com",
        )
        assert client is not None
        assert str(client.sync._client.base_url).startswith(
            "https://custom-sync.example.com"
        )

    def test_forwards_settings_to_sync_client(self) -> None:
        settings = Settings(
            chroma_server_headers={"x-test-header": "test-value"},
            chroma_server_ssl_verify="/tmp/test-cert.pem",
        )

        with patch("chromadb.sync_client.httpx.Client") as mock_httpx_client:
            make_cloud_client(
                api_key="test-key",
                database="test-db",
                settings=settings,
            )

        kwargs = mock_httpx_client.call_args.kwargs
        assert kwargs["headers"]["x-test-header"] == "test-value"
        assert kwargs["headers"]["x-chroma-token"] == "test-key"
        assert kwargs["verify"] == "/tmp/test-cert.pem"


class TestMethodsExist:
    @pytest.fixture(autouse=True)
    def _client(self) -> None:
        self.client = make_cloud_client(api_key="test-key", database="test-db")

    def test_source_methods(self) -> None:
        assert callable(self.client.sync.list_sources)
        assert callable(self.client.sync.create_github_source)
        assert callable(self.client.sync.create_s3_source)
        assert callable(self.client.sync.create_web_source)
        assert callable(self.client.sync.get_source)
        assert callable(self.client.sync.delete_source)

    def test_invocation_methods(self) -> None:
        assert callable(self.client.sync.list_invocations)
        assert callable(self.client.sync.get_invocation)
        assert callable(self.client.sync.cancel_invocation)
        assert callable(self.client.sync.create_invocation)
        assert callable(self.client.sync.get_latest_invocations_by_keys)

    def test_system_methods(self) -> None:
        assert callable(self.client.sync.health)


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


class TestS3BucketNameParsing:
    def test_accepts_plain_bucket_name(self) -> None:
        assert _parse_s3_bucket_name("my-bucket") == "my-bucket"

    def test_parses_s3_uri(self) -> None:
        assert _parse_s3_bucket_name("s3://my-bucket/some/prefix") == "my-bucket"

    def test_parses_s3_arn(self) -> None:
        assert _parse_s3_bucket_name("arn:aws:s3:::my-bucket") == "my-bucket"


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


class TestEmbeddingModelEnums:
    def test_dense_model_value(self) -> None:
        assert (
            DenseEmbeddingModel.QWEN3_EMBEDDING_06B.value == "Qwen/Qwen3-Embedding-0.6B"
        )

    def test_sparse_model_values(self) -> None:
        assert SparseEmbeddingModel.BM25.value == "Chroma/BM25"
        assert SparseEmbeddingModel.SPLADE_V1.value == "prithivida/Splade_PP_en_v1"


class TestSourceAPICalls:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.client = make_cloud_client(api_key="test-key", database="test-db")
        self.mock_request = MagicMock(
            return_value=_mock_response(200, {"source_id": "src-123"})
        )
        self.client.sync._client.request = self.mock_request

    def test_create_github_source(self) -> None:
        result = self.client.sync.create_github_source(
            CreateGitHubSourceArgs(
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
        assert call_args[1]["json"]["database_name"] == "test-db"

    def test_create_s3_source(self) -> None:
        result = self.client.sync.create_s3_source(
            CreateS3SourceArgs(
                s3=S3SourceConfig(
                    bucket_name="my-bucket",
                    region="us-east-1",
                    collection_name="docs",
                    aws_credential_id=1,
                ),
            )
        )
        assert result == {"source_id": "src-123"}
        assert self.mock_request.call_args[1]["json"]["database_name"] == "test-db"

    def test_create_web_source(self) -> None:
        result = self.client.sync.create_web_source(
            CreateWebSourceArgs(
                web=WebSourceConfig(starting_url="https://docs.trychroma.com"),
            )
        )
        assert result == {"source_id": "src-123"}
        assert self.mock_request.call_args[1]["json"]["database_name"] == "test-db"

    def test_list_sources(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.sync.list_sources()
        assert result == []
        assert self.mock_request.call_args[1]["params"]["database_name"] == "test-db"

    def test_get_source(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"id": "src-123", "database_name": "test-db"}
        )
        result = self.client.sync.get_source("src-123")
        assert result["id"] == "src-123"

    def test_delete_source(self) -> None:
        self.mock_request.return_value = _mock_response(204)
        self.client.sync.delete_source("src-123")
        call_args = self.mock_request.call_args
        assert call_args[0][0] == "DELETE"


class TestInvocationAPICalls:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.client = make_cloud_client(api_key="test-key", database="test-db")
        self.mock_request = MagicMock(
            return_value=_mock_response(200, {"invocation_id": "inv-456"})
        )
        self.client.sync._client.request = self.mock_request

    def test_create_github_invocation(self) -> None:
        ref: Dict[Literal["branch"], str] = {"branch": "main"}
        result = self.client.sync.create_invocation(
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
        result = self.client.sync.create_invocation(
            "src-123",
            CreateS3InvocationArgs(
                object_key="path/to/file.txt",
                custom_id="my-custom-id",
                metadata={"key": "value"},
            ),
        )
        assert result == {"invocation_id": "inv-456"}

    def test_create_web_invocation(self) -> None:
        result = self.client.sync.create_invocation(
            "src-123",
            CreateWebInvocationArgs(target_collection_name="web-collection"),
        )
        assert result == {"invocation_id": "inv-456"}

    def test_list_invocations(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.sync.list_invocations()
        assert result == []
        assert self.mock_request.call_args[1]["params"]["database_name"] == "test-db"

    def test_list_invocations_with_filters(self) -> None:
        self.mock_request.return_value = _mock_response(200, [])
        result = self.client.sync.list_invocations(
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
        assert "database_name" not in params
        assert params["status"] == "pending"
        assert params["limit"] == 10
        assert params["order_by"] == "ASC"

    def test_get_invocation(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"id": "inv-456", "status": "pending"}
        )
        result = self.client.sync.get_invocation("inv-456")
        assert result["id"] == "inv-456"

    def test_cancel_invocation(self) -> None:
        self.mock_request.return_value = _mock_response(202, {})
        self.client.sync.cancel_invocation("inv-456")
        call_args = self.mock_request.call_args
        assert call_args[0][0] == "PUT"

    def test_get_latest_invocations_by_keys(self) -> None:
        self.mock_request.return_value = _mock_response(
            200, {"key1": {"id": "inv-1"}, "key2": {"id": "inv-2"}}
        )
        result = self.client.sync.get_latest_invocations_by_keys(
            "src-123", ["key1", "key2"]
        )
        assert "key1" in result
        assert "key2" in result


class TestHealthCheck:
    def test_health(self) -> None:
        client = make_cloud_client(api_key="test-key", database="test-db")
        client.sync._client.request = MagicMock(return_value=_mock_response(200))
        client.sync.health()
