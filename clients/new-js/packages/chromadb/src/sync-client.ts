import { createClient, createConfig } from "@hey-api/client-fetch";
import {
  SourceService,
  InvocationService,
  HandlersJobsService,
  SystemService,
} from "./sync-api";
import type { CreateSourcePayload, CreateJobPayload } from "./sync-api";
import { chromaFetch } from "./chroma-fetch";
import { ChromaValueError } from "./errors";
import * as process from "node:process";
import type {
  SyncClientArgs,
  SyncSource,
  Invocation,
  InvocationsByKeysResult,
  CreateGitHubSourceArgs,
  CreateS3SourceArgs,
  CreateWebSourceArgs,
  CreateInvocationArgs,
  CreateGitHubInvocationArgs,
  CreateS3InvocationArgs,
  ListSourcesOptions,
  ListInvocationsOptions,
  SyncEmbeddingConfig,
} from "./sync-types";

const GITHUB_REPO_RE = /^[a-zA-Z0-9._-]+\/[a-zA-Z0-9._-]+$/;

function parseGitHubRepository(input: string): string {
  // Already in owner/repo format
  if (GITHUB_REPO_RE.test(input)) {
    return input;
  }

  // Try parsing as a GitHub URL
  try {
    const url = new URL(input);
    if (url.hostname === "github.com" || url.hostname === "www.github.com") {
      const parts = url.pathname
        .replace(/^\//, "")
        .replace(/\.git$/, "")
        .split("/");
      if (parts.length >= 2 && parts[0] && parts[1]) {
        return `${parts[0]}/${parts[1]}`;
      }
    }
  } catch {
    // Not a URL, fall through
  }

  throw new ChromaValueError(
    `Invalid GitHub repository "${input}". Expected "owner/repo" format (e.g. "chroma-core/chroma").`,
  );
}

function parseS3BucketName(input: string): string {
  // s3://bucket-name/optional/prefix -> bucket-name
  if (input.startsWith("s3://")) {
    const withoutScheme = input.slice(5);
    const slashIndex = withoutScheme.indexOf("/");
    return slashIndex === -1
      ? withoutScheme
      : withoutScheme.slice(0, slashIndex);
  }

  // arn:aws:s3:::bucket-name -> bucket-name
  if (input.startsWith("arn:aws:s3:::")) {
    const afterArn = input.slice("arn:aws:s3:::".length);
    const slashIndex = afterArn.indexOf("/");
    return slashIndex === -1 ? afterArn : afterArn.slice(0, slashIndex);
  }

  return input;
}

function validateStartingUrl(input: string): string {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new ChromaValueError(
      `Invalid starting URL "${input}". Must be a valid URL (e.g. "https://docs.trychroma.com").`,
    );
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new ChromaValueError(
      `Invalid starting URL "${input}". Only http and https protocols are supported.`,
    );
  }

  return url.toString();
}

function toApiEmbeddingConfig(config?: SyncEmbeddingConfig) {
  if (!config) return undefined;
  return {
    dense: config.dense
      ? {
          model: config.dense.model as "Qwen/Qwen3-Embedding-0.6B",
          task: config.dense.task
            ? {
                task_name: config.dense.task.taskName,
                query_prompt: config.dense.task.queryPrompt,
                document_prompt: config.dense.task.documentPrompt,
              }
            : undefined,
        }
      : undefined,
    sparse: config.sparse
      ? {
          model: config.sparse.model as
            | "Chroma/BM25"
            | "prithivida/Splade_PP_en_v1"
            | undefined,
          key: config.sparse.key,
        }
      : undefined,
  };
}

export class SyncClient {
  private readonly apiClient: ReturnType<typeof createClient>;

  constructor(args: SyncClientArgs = {}) {
    const apiKey = args.apiKey || process.env.CHROMA_API_KEY;
    if (!apiKey) {
      throw new ChromaValueError(
        "Missing API key. Please provide it to the SyncClient constructor or set your CHROMA_API_KEY environment variable",
      );
    }

    this.apiClient = createClient(
      createConfig({
        baseUrl: args.host
          ? `https://${args.host}`
          : "https://sync.trychroma.com",
        throwOnError: true,
        fetch: chromaFetch,
        headers: {
          "x-chroma-token": apiKey,
          ...args.fetchOptions?.headers,
        },
      }),
    );
  }

  // --- Sources ---

  async listSources(opts: ListSourcesOptions = {}): Promise<SyncSource[]> {
    const { data } = await SourceService.listSources({
      client: this.apiClient,
      query: {
        database_name: opts.databaseName,
        source_type: opts.sourceType,
        limit: opts.limit,
        offset: opts.offset,
        order_by: opts.orderBy,
      },
    });
    return data;
  }

  async createGitHubSource(
    config: CreateGitHubSourceArgs,
  ): Promise<{ sourceId: string }> {
    const repository = parseGitHubRepository(config.github.repository);

    const body: CreateSourcePayload = {
      database_name: config.databaseName,
      embedding: toApiEmbeddingConfig(config.embedding),
      chunking: config.chunking,
      github: {
        repository,
        app_id: config.github.appId,
        include_globs: config.github.includeGlobs,
      },
    };

    const { data } = await SourceService.createSource({
      client: this.apiClient,
      body,
    });
    return { sourceId: data.source_id };
  }

  async createS3Source(
    config: CreateS3SourceArgs,
  ): Promise<{ sourceId: string }> {
    const bucketName = parseS3BucketName(config.s3.bucketName);

    const body: CreateSourcePayload = {
      database_name: config.databaseName,
      embedding: toApiEmbeddingConfig(config.embedding),
      chunking: config.chunking,
      s3: {
        bucket_name: bucketName,
        region: config.s3.region,
        collection_name: config.s3.collectionName,
        aws_credential_id: config.s3.awsCredentialId,
        path_prefix: config.s3.pathPrefix,
        auto_sync: config.s3.autoSync,
      },
    };

    const { data } = await SourceService.createSource({
      client: this.apiClient,
      body,
    });
    return { sourceId: data.source_id };
  }

  async createWebSource(
    config: CreateWebSourceArgs,
  ): Promise<{ sourceId: string }> {
    const startingUrl = validateStartingUrl(config.web.startingUrl);

    const body: CreateSourcePayload = {
      database_name: config.databaseName,
      embedding: toApiEmbeddingConfig(config.embedding),
      chunking: config.chunking,
      web_scrape: {
        starting_url: startingUrl,
        max_depth: config.web.maxDepth,
        page_limit: config.web.pageLimit,
        include_path_regexes: config.web.includePathRegexes,
        exclude_path_regexes: config.web.excludePathRegexes,
      },
    };

    const { data } = await SourceService.createSource({
      client: this.apiClient,
      body,
    });
    return { sourceId: data.source_id };
  }

  async getSource(sourceId: string): Promise<SyncSource> {
    const { data } = await SourceService.getSource({
      client: this.apiClient,
      path: { source_id: sourceId },
    });
    return data;
  }

  async deleteSource(sourceId: string): Promise<void> {
    await SourceService.deleteSource({
      client: this.apiClient,
      path: { source_id: sourceId },
    });
  }

  // --- Invocations ---

  async listInvocations(
    opts: ListInvocationsOptions = {},
  ): Promise<Invocation[]> {
    const { data } = await InvocationService.listJobs({
      client: this.apiClient,
      query: {
        source_id: opts.sourceId,
        database_name: opts.databaseName,
        source_type: opts.sourceType,
        status: opts.status,
        limit: opts.limit,
        offset: opts.offset,
        order_by: opts.orderBy,
      },
    });
    return data;
  }

  async getInvocation(invocationId: string): Promise<Invocation> {
    const { data } = await InvocationService.getJob({
      client: this.apiClient,
      path: { invocation_id: invocationId },
    });
    return data;
  }

  async cancelInvocation(invocationId: string): Promise<void> {
    await InvocationService.cancelPendingJob({
      client: this.apiClient,
      path: { invocation_id: invocationId },
    });
  }

  async createInvocation(
    sourceId: string,
    config: CreateInvocationArgs,
  ): Promise<{ invocationId: string }> {
    const body: CreateJobPayload = {
      target_collection_name:
        "targetCollectionName" in config
          ? config.targetCollectionName
          : undefined,
    };

    if ("refIdentifier" in config) {
      body.ref_identifier = (
        config as CreateGitHubInvocationArgs
      ).refIdentifier;
    }

    if ("objectKey" in config) {
      const s3Config = config as CreateS3InvocationArgs;
      body.object_key = s3Config.objectKey;
      body.custom_id = s3Config.customId;
      body.metadata = s3Config.metadata;
    }

    const { data } = await InvocationService.createJob({
      client: this.apiClient,
      path: { source_id: sourceId },
      body,
    });
    return { invocationId: data.invocation_id };
  }

  async getLatestInvocationsByKeys(
    sourceId: string,
    objectKeys: string[],
  ): Promise<InvocationsByKeysResult> {
    const { data } = await HandlersJobsService.latestInvocationsByKeys({
      client: this.apiClient,
      path: { source_id: sourceId },
      body: { object_keys: objectKeys },
    });
    return data;
  }

  // --- System ---

  async health(): Promise<void> {
    await SystemService.healthCheck({
      client: this.apiClient,
    });
  }
}
