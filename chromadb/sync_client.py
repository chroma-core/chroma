from __future__ import annotations

import os
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional, cast
from urllib.parse import urlparse

import httpx

from chromadb.errors import InvalidArgumentError
from chromadb.sync_types import (
    CreateGitHubInvocationArgs,
    CreateGitHubSourceArgs,
    CreateInvocationArgs,
    CreateS3InvocationArgs,
    CreateS3SourceArgs,
    CreateWebInvocationArgs,
    CreateWebSourceArgs,
    Invocation,
    InvocationsByKeysResult,
    ListInvocationsOptions,
    ListSourcesOptions,
    SyncEmbeddingConfig,
    SyncSource,
)

_GITHUB_REPO_RE = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")

DEFAULT_SYNC_HOST = "sync.trychroma.com"


def _parse_github_repository(input: str) -> str:
    """Parse a GitHub repository string into owner/repo format."""
    if _GITHUB_REPO_RE.match(input):
        return input

    try:
        parsed = urlparse(input)
        if parsed.hostname in ("github.com", "www.github.com"):
            path = parsed.path.lstrip("/")
            if path.endswith(".git"):
                path = path[:-4]
            parts = path.split("/")
            if len(parts) >= 2 and parts[0] and parts[1]:
                return f"{parts[0]}/{parts[1]}"
    except Exception:
        pass

    raise InvalidArgumentError(
        f'Invalid GitHub repository "{input}". '
        'Expected "owner/repo" format (e.g. "chroma-core/chroma").'
    )


def _parse_s3_bucket_name(input: str) -> str:
    """Parse an S3 bucket identifier into a plain bucket name."""
    if input.startswith("s3://"):
        without_scheme = input[5:]
        slash_idx = without_scheme.find("/")
        return without_scheme if slash_idx == -1 else without_scheme[:slash_idx]

    if input.startswith("arn:aws:s3:::"):
        after_arn = input[len("arn:aws:s3:::") :]
        slash_idx = after_arn.find("/")
        return after_arn if slash_idx == -1 else after_arn[:slash_idx]

    return input


def _validate_starting_url(input: str) -> str:
    """Validate that a URL is a valid HTTP/HTTPS URL."""
    parsed = urlparse(input)
    if not parsed.scheme or not parsed.netloc:
        raise InvalidArgumentError(
            f'Invalid starting URL "{input}". '
            'Must be a valid URL (e.g. "https://docs.trychroma.com").'
        )

    if parsed.scheme not in ("http", "https"):
        raise InvalidArgumentError(
            f'Invalid starting URL "{input}". '
            "Only http and https protocols are supported."
        )

    return input


def _embedding_config_to_api(
    config: Optional[SyncEmbeddingConfig],
) -> Optional[Dict[str, Any]]:
    """Convert a SyncEmbeddingConfig to the API payload format."""
    if config is None:
        return None

    result: Dict[str, Any] = {}

    if config.dense is not None:
        dense: Dict[str, Any] = {"model": config.dense.model.value}
        if config.dense.task is not None:
            dense["task"] = {
                "task_name": config.dense.task.task_name,
            }
            if config.dense.task.query_prompt is not None:
                dense["task"]["query_prompt"] = config.dense.task.query_prompt
            if config.dense.task.document_prompt is not None:
                dense["task"]["document_prompt"] = config.dense.task.document_prompt
        result["dense"] = dense

    if config.sparse is not None:
        sparse: Dict[str, Any] = {}
        if config.sparse.model is not None:
            sparse["model"] = config.sparse.model.value
        if config.sparse.key is not None:
            sparse["key"] = config.sparse.key
        result["sparse"] = sparse

    return result


def _chunking_config_to_api(config: Any) -> Optional[Dict[str, Any]]:
    """Convert a ChunkingConfig to the API payload format."""
    if config is None:
        return None
    d = asdict(config)
    return {k: v for k, v in d.items() if v is not None}


def _strip_none(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys with None values from a dict."""
    return {k: v for k, v in d.items() if v is not None}


class SyncClient:
    """Client for the Chroma Sync service.

    The Sync service manages syncing data from external sources (GitHub, S3, web)
    into Chroma collections.
    """

    _client: httpx.Client

    def __init__(
        self,
        api_key: Optional[str] = None,
        host: Optional[str] = None,
    ) -> None:
        api_key = api_key or os.environ.get("CHROMA_API_KEY")
        if not api_key:
            raise InvalidArgumentError(
                "Missing API key. Please provide it to the SyncClient constructor "
                "or set your CHROMA_API_KEY environment variable."
            )

        if host is not None:
            base_url = f"https://{host}"
        else:
            base_url = f"https://{DEFAULT_SYNC_HOST}"

        self._client = httpx.Client(
            base_url=base_url,
            headers={"x-chroma-token": api_key},
        )

    def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        self._client.close()

    def __enter__(self) -> "SyncClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make an HTTP request and return parsed JSON (or None for 204)."""
        response = self._client.request(method, path, params=params, json=json)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json()
        return None

    # --- Sources ---

    def list_sources(
        self, opts: Optional[ListSourcesOptions] = None
    ) -> List[SyncSource]:
        """List all sources, with optional filtering."""
        opts = opts or ListSourcesOptions()
        params = _strip_none(
            {
                "database_name": opts.database_name,
                "source_type": opts.source_type.value if opts.source_type else None,
                "limit": opts.limit,
                "offset": opts.offset,
                "order_by": opts.order_by.value if opts.order_by else None,
            }
        )
        return cast(
            List[SyncSource],
            self._request("GET", "/api/v1/sources", params=params),
        )

    def create_github_source(self, config: CreateGitHubSourceArgs) -> Dict[str, str]:
        """Create a GitHub source. Returns {"source_id": "..."}."""
        repository = _parse_github_repository(config.github.repository)

        body: Dict[str, Any] = {
            "database_name": config.database_name,
            "github": _strip_none(
                {
                    "repository": repository,
                    "app_id": config.github.app_id,
                    "include_globs": config.github.include_globs,
                }
            ),
        }

        embedding = _embedding_config_to_api(config.embedding)
        if embedding is not None:
            body["embedding"] = embedding

        chunking = _chunking_config_to_api(config.chunking)
        if chunking is not None:
            body["chunking"] = chunking

        return cast(
            Dict[str, str],
            self._request("POST", "/api/v1/sources", json=body),
        )

    def create_s3_source(self, config: CreateS3SourceArgs) -> Dict[str, str]:
        """Create an S3 source. Returns {"source_id": "..."}."""
        bucket_name = _parse_s3_bucket_name(config.s3.bucket_name)

        body: Dict[str, Any] = {
            "database_name": config.database_name,
            "s3": _strip_none(
                {
                    "bucket_name": bucket_name,
                    "region": config.s3.region,
                    "collection_name": config.s3.collection_name,
                    "aws_credential_id": config.s3.aws_credential_id,
                    "path_prefix": config.s3.path_prefix,
                    "auto_sync": (
                        config.s3.auto_sync.value if config.s3.auto_sync else None
                    ),
                }
            ),
        }

        embedding = _embedding_config_to_api(config.embedding)
        if embedding is not None:
            body["embedding"] = embedding

        chunking = _chunking_config_to_api(config.chunking)
        if chunking is not None:
            body["chunking"] = chunking

        return cast(
            Dict[str, str],
            self._request("POST", "/api/v1/sources", json=body),
        )

    def create_web_source(self, config: CreateWebSourceArgs) -> Dict[str, str]:
        """Create a web scrape source. Returns {"source_id": "..."}."""
        starting_url = _validate_starting_url(config.web.starting_url)

        body: Dict[str, Any] = {
            "database_name": config.database_name,
            "web_scrape": _strip_none(
                {
                    "starting_url": starting_url,
                    "max_depth": config.web.max_depth,
                    "page_limit": config.web.page_limit,
                    "include_path_regexes": config.web.include_path_regexes,
                    "exclude_path_regexes": config.web.exclude_path_regexes,
                }
            ),
        }

        embedding = _embedding_config_to_api(config.embedding)
        if embedding is not None:
            body["embedding"] = embedding

        chunking = _chunking_config_to_api(config.chunking)
        if chunking is not None:
            body["chunking"] = chunking

        return cast(
            Dict[str, str],
            self._request("POST", "/api/v1/sources", json=body),
        )

    def get_source(self, source_id: str) -> SyncSource:
        """Get a source by ID."""
        return cast(
            SyncSource,
            self._request("GET", f"/api/v1/sources/{source_id}"),
        )

    def delete_source(self, source_id: str) -> None:
        """Delete a source by ID."""
        self._request("DELETE", f"/api/v1/sources/{source_id}")

    # --- Invocations ---

    def list_invocations(
        self, opts: Optional[ListInvocationsOptions] = None
    ) -> List[Invocation]:
        """List invocations, with optional filtering."""
        opts = opts or ListInvocationsOptions()
        params = _strip_none(
            {
                "source_id": opts.source_id,
                "database_name": opts.database_name,
                "source_type": opts.source_type.value if opts.source_type else None,
                "status": opts.status.value if opts.status else None,
                "limit": opts.limit,
                "offset": opts.offset,
                "order_by": opts.order_by.value if opts.order_by else None,
            }
        )
        return cast(
            List[Invocation],
            self._request("GET", "/api/v1/invocations", params=params),
        )

    def get_invocation(self, invocation_id: str) -> Invocation:
        """Get an invocation by ID."""
        return cast(
            Invocation,
            self._request("GET", f"/api/v1/invocations/{invocation_id}"),
        )

    def cancel_invocation(self, invocation_id: str) -> None:
        """Cancel a pending invocation."""
        self._request("PUT", f"/api/v1/invocations/{invocation_id}")

    def create_invocation(
        self, source_id: str, config: CreateInvocationArgs
    ) -> Dict[str, str]:
        """Create an invocation for a source. Returns {"invocation_id": "..."}."""
        body: Dict[str, Any] = {}

        if isinstance(config, CreateGitHubInvocationArgs):
            body["target_collection_name"] = config.target_collection_name
            body["ref_identifier"] = config.ref_identifier
        elif isinstance(config, CreateS3InvocationArgs):
            body["object_key"] = config.object_key
            if config.target_collection_name is not None:
                body["target_collection_name"] = config.target_collection_name
            if config.custom_id is not None:
                body["custom_id"] = config.custom_id
            if config.metadata is not None:
                body["metadata"] = config.metadata
        elif isinstance(config, CreateWebInvocationArgs):
            body["target_collection_name"] = config.target_collection_name

        return cast(
            Dict[str, str],
            self._request(
                "POST", f"/api/v1/sources/{source_id}/invocations", json=body
            ),
        )

    def get_latest_invocations_by_keys(
        self, source_id: str, object_keys: List[str]
    ) -> InvocationsByKeysResult:
        """Get the latest invocations for a set of object keys."""
        return cast(
            InvocationsByKeysResult,
            self._request(
                "POST",
                f"/api/v1/sources/{source_id}/invocations/latest-by-keys",
                json={"object_keys": object_keys},
            ),
        )

    # --- System ---

    def health(self) -> None:
        """Check if the sync service is healthy."""
        self._request("GET", "/health")
