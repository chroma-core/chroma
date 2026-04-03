import type {
  Source as GeneratedSource,
  Job as GeneratedJob,
  JobStatus as GeneratedJobStatus,
  SourceChunkingConfig as GeneratedChunkingConfig,
  GitRefIdentifier as GeneratedGitRefIdentifier,
  SourceTypeFilter as GeneratedSourceTypeFilter,
  OrderBy as GeneratedOrderBy,
  AutoSync as GeneratedAutoSync,
  SparseEmbeddingModel as GeneratedSparseEmbeddingModel,
  HashMap as GeneratedHashMap,
} from "./sync-api";

// Re-export generated types that are already well-shaped
export type SyncSource = GeneratedSource;
export type Invocation = GeneratedJob;
export type InvocationStatus = GeneratedJobStatus;

/**
 * Only the string variants of InvocationStatus are valid as query parameter
 * filters. Object variants like `{ complete: ... }` cannot be serialized as
 * URL query parameters.
 */
export type InvocationStatusFilter = Extract<InvocationStatus, string>;
export type SyncChunkingConfig = GeneratedChunkingConfig;
export type GitRefIdentifier = GeneratedGitRefIdentifier;
export type SourceTypeFilter = GeneratedSourceTypeFilter;
export type SyncOrderBy = GeneratedOrderBy;
export type AutoSyncMode = GeneratedAutoSync;
export type SparseEmbeddingModel = GeneratedSparseEmbeddingModel;
export type InvocationsByKeysResult = GeneratedHashMap;

export const DenseEmbeddingModel = {
  Qwen3Embedding06B: "Qwen/Qwen3-Embedding-0.6B",
} as const;
export type DenseEmbeddingModelValue =
  (typeof DenseEmbeddingModel)[keyof typeof DenseEmbeddingModel];

export const SparseEmbeddingModel = {
  BM25: "Chroma/BM25",
  SpladeV1: "prithivida/Splade_PP_en_v1",
} as const;
export type SparseEmbeddingModelValue =
  (typeof SparseEmbeddingModel)[keyof typeof SparseEmbeddingModel];

export interface SyncEmbeddingConfig {
  dense?: {
    model: DenseEmbeddingModelValue;
    task?: { taskName: string; queryPrompt?: string; documentPrompt?: string };
  };
  sparse?: {
    model?: SparseEmbeddingModelValue;
    key?: string;
  } | null;
}

export interface GitHubSourceConfig {
  repository: string;
  appId?: string;
  includeGlobs?: string[];
}

export interface S3SourceConfig {
  bucketName: string;
  region: string;
  collectionName: string;
  awsCredentialId: number;
  pathPrefix?: string;
  autoSync?: AutoSyncMode;
}

export interface WebSourceConfig {
  startingUrl: string;
  maxDepth?: number;
  pageLimit?: number;
  includePathRegexes?: string[];
  excludePathRegexes?: string[];
}

export interface CreateSourceBase {
  databaseName: string;
  embedding?: SyncEmbeddingConfig;
  chunking?: SyncChunkingConfig;
}

export interface CreateGitHubSourceArgs extends CreateSourceBase {
  github: GitHubSourceConfig;
}

export interface CreateS3SourceArgs extends CreateSourceBase {
  s3: S3SourceConfig;
}

export interface CreateWebSourceArgs extends CreateSourceBase {
  web: WebSourceConfig;
}

export interface CreateGitHubInvocationArgs {
  targetCollectionName: string;
  refIdentifier: GitRefIdentifier;
}

export interface CreateS3InvocationArgs {
  objectKey: string;
  targetCollectionName?: string;
  customId?: string;
  metadata?: Record<string, unknown>;
}

export interface CreateWebInvocationArgs {
  targetCollectionName: string;
}

export type CreateInvocationArgs =
  | CreateGitHubInvocationArgs
  | CreateS3InvocationArgs
  | CreateWebInvocationArgs;

export interface ListOptions {
  limit?: number;
  offset?: number;
  orderBy?: SyncOrderBy;
}

export interface ListSourcesOptions extends ListOptions {
  databaseName?: string;
  sourceType?: SourceTypeFilter;
}

export interface ListInvocationsOptions extends ListOptions {
  sourceId?: string;
  databaseName?: string;
  sourceType?: SourceTypeFilter;
  status?: InvocationStatusFilter;
}

export interface SyncClientArgs {
  apiKey?: string;
  host?: string;
  fetchOptions?: Omit<RequestInit, "headers"> & {
    headers?: Record<string, string>;
  };
}
