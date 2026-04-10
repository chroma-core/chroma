from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union


# --- Enums ---


class DenseEmbeddingModel(str, enum.Enum):
    QWEN3_EMBEDDING_06B = "Qwen/Qwen3-Embedding-0.6B"


class SparseEmbeddingModel(str, enum.Enum):
    BM25 = "Chroma/BM25"
    SPLADE_V1 = "prithivida/Splade_PP_en_v1"


class InvocationStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SourceTypeFilter(str, enum.Enum):
    GITHUB = "github"
    WEB_SCRAPE = "web_scrape"
    S3 = "s3"


class OrderBy(str, enum.Enum):
    ASC = "ASC"
    DESC = "DESC"


class AutoSyncMode(str, enum.Enum):
    NONE = "none"
    DIRECT = "direct"
    METADATA = "metadata"


# --- Embedding config ---


@dataclass
class EmbeddingTask:
    task_name: str
    query_prompt: Optional[str] = None
    document_prompt: Optional[str] = None


@dataclass
class DenseEmbeddingConfig:
    model: DenseEmbeddingModel
    task: Optional[EmbeddingTask] = None


@dataclass
class SparseEmbeddingConfig:
    model: Optional[SparseEmbeddingModel] = None
    key: Optional[str] = None


@dataclass
class SyncEmbeddingConfig:
    dense: Optional[DenseEmbeddingConfig] = None
    sparse: Optional[SparseEmbeddingConfig] = None


# --- Chunking config ---


@dataclass
class TreeSitterChunking:
    type: Literal["tree_sitter"] = "tree_sitter"
    max_size_bytes: Optional[int] = None


@dataclass
class LinesChunking:
    type: Literal["lines"] = "lines"
    max_lines: Optional[int] = None
    max_size_bytes: Optional[int] = None


ChunkingConfig = Union[TreeSitterChunking, LinesChunking]


# --- Source configs ---


@dataclass
class GitHubSourceConfig:
    repository: str
    app_id: Optional[str] = None
    include_globs: Optional[List[str]] = None


@dataclass
class S3SourceConfig:
    bucket_name: str
    region: str
    collection_name: str
    aws_credential_id: int
    path_prefix: Optional[str] = None
    auto_sync: Optional[AutoSyncMode] = None


@dataclass
class WebSourceConfig:
    starting_url: str
    max_depth: Optional[int] = None
    page_limit: Optional[int] = None
    include_path_regexes: Optional[List[str]] = None
    exclude_path_regexes: Optional[List[str]] = None


# --- Create source args ---


@dataclass
class CreateGitHubSourceArgs:
    github: GitHubSourceConfig
    embedding: Optional[SyncEmbeddingConfig] = None
    chunking: Optional[ChunkingConfig] = None


@dataclass
class CreateS3SourceArgs:
    s3: S3SourceConfig
    embedding: Optional[SyncEmbeddingConfig] = None
    chunking: Optional[ChunkingConfig] = None


@dataclass
class CreateWebSourceArgs:
    web: WebSourceConfig
    embedding: Optional[SyncEmbeddingConfig] = None
    chunking: Optional[ChunkingConfig] = None


# --- Git ref identifier ---

GitRefIdentifier = Union[Dict[Literal["sha"], str], Dict[Literal["branch"], str]]


# --- Create invocation args ---


@dataclass
class CreateGitHubInvocationArgs:
    target_collection_name: str
    ref_identifier: GitRefIdentifier


@dataclass
class CreateS3InvocationArgs:
    object_key: str
    target_collection_name: Optional[str] = None
    custom_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class CreateWebInvocationArgs:
    target_collection_name: str


CreateInvocationArgs = Union[
    CreateGitHubInvocationArgs,
    CreateS3InvocationArgs,
    CreateWebInvocationArgs,
]


# --- List options ---


@dataclass
class ListSourcesOptions:
    source_type: Optional[SourceTypeFilter] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[OrderBy] = None


@dataclass
class ListInvocationsOptions:
    source_id: Optional[str] = None
    source_type: Optional[SourceTypeFilter] = None
    status: Optional[InvocationStatus] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order_by: Optional[OrderBy] = None


# --- Response types ---
# These are returned as dicts from the API. We define type aliases for clarity.

SyncSource = Dict[str, Any]
Invocation = Dict[str, Any]
InvocationsByKeysResult = Dict[str, Any]
