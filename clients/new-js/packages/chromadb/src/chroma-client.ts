import { createClient, createConfig } from "@hey-api/client-fetch";
import {
  defaultChromaClientArgs as defaultArgs,
  HttpMethod,
  normalizeMethod,
} from "./utils";
import { CollectionConfiguration, DefaultService as Api } from "./api";
import { CollectionMetadata, UserIdentity } from "./types";
import { Collection, CollectionImpl } from "./collection";
import {
  EmbeddingFunction,
  getEmbeddingFunction,
  serializeEmbeddingFunction,
} from "./embedding-function";

export interface ChromaClientArgs {
  host?: string;
  port?: number;
  ssl?: boolean;
  tenant?: string;
  database?: string;
  headers?: Record<string, string>;
  fetchOptions?: RequestInit;
}

export class ChromaClient {
  public readonly tenant: string;
  public readonly database: string;
  private readonly apiClient: ReturnType<typeof createClient>;

  constructor(args: Partial<ChromaClientArgs> = {}) {
    const {
      host = defaultArgs.host,
      port = defaultArgs.port,
      ssl = defaultArgs.ssl,
      tenant = defaultArgs.tenant,
      database = defaultArgs.database,
      headers = defaultArgs.headers,
      fetchOptions = defaultArgs.fetchOptions,
    } = args;

    const baseUrl = `${ssl ? "https" : "http"}://${host}:${port}`;

    this.tenant = tenant as string;
    this.database = database as string;

    const configOptions = {
      ...fetchOptions,
      method: normalizeMethod(fetchOptions?.method) as HttpMethod,
      baseUrl,
      headers,
    };

    this.apiClient = createClient(createConfig(configOptions));
  }

  private path(): { tenant: string; database: string } {
    return { tenant: this.tenant, database: this.database };
  }

  public async getUserIdentity(): Promise<UserIdentity> {
    const { data } = await Api.getUserIdentity({
      client: this.apiClient,
    });
    return data;
  }

  public async heartbeat(): Promise<number> {
    const { data } = await Api.heartbeat({
      client: this.apiClient,
    });
    return data["nanosecond heartbeat"];
  }

  public async listCollections({
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  }): Promise<Collection[]> {
    const { data } = await Api.listCollections({
      client: this.apiClient,
      path: this.path(),
      query: { limit, offset },
    });

    return data.map(
      (collection) =>
        new CollectionImpl({
          chromaClient: this,
          apiClient: this.apiClient,
          name: collection.name,
          id: collection.id,
          embeddingFunction: getEmbeddingFunction(
            collection.name,
            collection.configuration_json.embedding_function ?? undefined,
          ),
          configuration: collection.configuration_json,
          metadata: collection.metadata ?? undefined,
        }),
    );
  }

  public async countCollections(): Promise<number> {
    const { data } = await Api.countCollections({
      client: this.apiClient,
      path: this.path(),
    });

    return data;
  }

  public async createCollection({
    name,
    configuration,
    metadata,
    embeddingFunction,
  }: {
    name: string;
    configuration?: CollectionConfiguration;
    metadata?: CollectionMetadata;
    embeddingFunction?: EmbeddingFunction;
  }): Promise<Collection> {
    const collectionConfig: CollectionConfiguration = {
      ...(configuration || {}),
      embedding_function: embeddingFunction
        ? serializeEmbeddingFunction(embeddingFunction)
        : undefined,
    };

    const { data } = await Api.createCollection({
      client: this.apiClient,
      path: this.path(),
      body: {
        name,
        // configuration: undefined,
        metadata,
        get_or_create: false,
      },
    });

    return new CollectionImpl({
      chromaClient: this,
      apiClient: this.apiClient,
      name,
      configuration: data.configuration_json,
      metadata,
      embeddingFunction:
        embeddingFunction ??
        getEmbeddingFunction(
          data.name,
          data.configuration_json.embedding_function ?? undefined,
        ),
      id: data.id,
    });
  }

  public async getCollection({ name }: { name: string }): Promise<Collection> {
    const { data } = await Api.getCollection({
      client: this.apiClient,
      path: { ...this.path(), collection_id: name },
    });

    return new CollectionImpl({
      chromaClient: this,
      apiClient: this.apiClient,
      name,
      configuration: data.configuration_json,
      metadata: data.metadata ?? undefined,
      embeddingFunction: getEmbeddingFunction(
        data.name,
        data.configuration_json.embedding_function ?? undefined,
      ),
      id: data.id,
    });
  }

  public async getOrCreateCollection({
    name,
    configuration,
    metadata,
    embeddingFunction,
  }: {
    name: string;
    configuration?: CollectionConfiguration;
    metadata?: CollectionMetadata;
    embeddingFunction?: EmbeddingFunction;
  }): Promise<Collection> {
    const collectionConfig: CollectionConfiguration = {
      ...(configuration || {}),
      embedding_function: embeddingFunction
        ? serializeEmbeddingFunction(embeddingFunction)
        : undefined,
    };

    const { data } = await Api.createCollection({
      client: this.apiClient,
      path: this.path(),
      body: {
        name,
        configuration: collectionConfig,
        metadata,
        get_or_create: true,
      },
    });

    return new CollectionImpl({
      chromaClient: this,
      apiClient: this.apiClient,
      name,
      configuration: data.configuration_json,
      metadata,
      embeddingFunction:
        embeddingFunction ??
        getEmbeddingFunction(
          name,
          data.configuration_json.embedding_function ?? undefined,
        ),
      id: data.id,
    });
  }

  public async deleteCollection({ name }: { name: string }): Promise<void> {
    await Api.deleteCollection({
      client: this.apiClient,
      path: { ...this.path(), collection_id: name },
    });
  }

  public async reset(): Promise<void> {
    await Api.reset({
      client: this.apiClient,
    });
  }

  public async version(): Promise<string> {
    const { data } = await Api.version({
      client: this.apiClient,
    });
    return data;
  }
}
