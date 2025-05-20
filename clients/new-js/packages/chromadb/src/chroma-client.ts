import { createClient, createConfig } from "@hey-api/client-fetch";
import {
  defaultChromaClientArgs as defaultArgs,
  HttpMethod,
  normalizeMethod,
} from "./utils";
import { CollectionConfiguration, DefaultService as Api } from "./api";
import { CollectionMetadata, UserIdentity } from "./types";
import {
  Collection,
  CollectionAPI,
  CollectionAPIImpl,
  CollectionImpl,
} from "./collection";
import {
  EmbeddingFunction,
  getDefaultEFConfig,
  getEmbeddingFunction,
  serializeEmbeddingFunction,
} from "./embedding-function";
import { chromaFetch } from "./chroma-fetch";
import { d } from "@hey-api/openapi-ts/dist/types.d-C5lgdIHG";
import * as process from "node:process";
import { ChromaUnauthorizedError, ChromaValueError } from "./errors";

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
  private _tenant: string | undefined;
  private _database: string | undefined;
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

    this._tenant = tenant || process.env.CHROMA_TENANT;
    this._database = database || process.env.CHROMA_DATABASE;

    const configOptions = {
      ...fetchOptions,
      method: normalizeMethod(fetchOptions?.method) as HttpMethod,
      baseUrl,
      headers,
    };

    this.apiClient = createClient(createConfig(configOptions));
    this.apiClient.setConfig({ fetch: chromaFetch });
  }

  public get tenant(): string | undefined {
    return this._tenant;
  }

  protected set tenant(tenant: string | undefined) {
    this._tenant = tenant;
  }

  public get database(): string | undefined {
    return this._database;
  }

  protected set database(database: string | undefined) {
    this._database = database;
  }

  /** @ignore */
  public async _path(): Promise<{ tenant: string; database: string }> {
    if (!this._tenant || !this._database) {
      const { tenant, databases } = await this.getUserIdentity();
      this._tenant = tenant;
      if (databases.length === 0) {
        throw new ChromaUnauthorizedError(
          `Your API key does not have access to any DBs for tenant ${this._tenant}`,
        );
      }
      if (databases.length > 1 || databases[0] === "*") {
        throw new ChromaValueError(
          "Your API key is scoped to more than 1 DB. Please provide a DB name to the CloudClient constructor",
        );
      }
      this._database = databases[0];
    }
    return { tenant: this._tenant, database: this._database };
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

  public async listCollections(
    args?: Partial<{
      limit: number;
      offset: number;
    }>,
  ): Promise<Collection[]> {
    const { limit = 100, offset = 0 } = args || {};

    const { data } = await Api.listCollections({
      client: this.apiClient,
      path: await this._path(),
      query: { limit, offset },
    });

    return Promise.all(
      data.map(
        async (collection) =>
          new CollectionImpl({
            chromaClient: this,
            apiClient: this.apiClient,
            name: collection.name,
            id: collection.id,
            embeddingFunction: await getEmbeddingFunction(
              collection.name,
              collection.configuration_json.embedding_function ?? undefined,
            ),
            configuration: collection.configuration_json,
            metadata: collection.metadata ?? undefined,
          }),
      ),
    );
  }

  public async countCollections(): Promise<number> {
    const { data } = await Api.countCollections({
      client: this.apiClient,
      path: await this._path(),
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
        : await getDefaultEFConfig(),
    };

    const { data } = await Api.createCollection({
      client: this.apiClient,
      path: await this._path(),
      body: {
        name,
        configuration: collectionConfig,
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
        (await getEmbeddingFunction(
          data.name,
          data.configuration_json.embedding_function ?? undefined,
        )),
      id: data.id,
    });
  }

  public async getCollection({
    name,
    embeddingFunction,
  }: {
    name: string;
    embeddingFunction?: EmbeddingFunction;
  }): Promise<Collection> {
    const { data } = await Api.getCollection({
      client: this.apiClient,
      path: { ...(await this._path()), collection_id: name },
    });

    return new CollectionImpl({
      chromaClient: this,
      apiClient: this.apiClient,
      name,
      configuration: data.configuration_json,
      metadata: data.metadata ?? undefined,
      embeddingFunction: embeddingFunction
        ? embeddingFunction
        : await getEmbeddingFunction(
            data.name,
            data.configuration_json.embedding_function ?? undefined,
          ),
      id: data.id,
    });
  }

  public async getCollections(
    items: string[] | { name: string; embeddingFunction?: EmbeddingFunction }[],
  ) {
    if (items.length === 0) return [];

    let requestedCollections = items;
    if (typeof items[0] === "string") {
      requestedCollections = (items as string[]).map((item) => {
        return { name: item, embeddingFunction: undefined };
      });
    }

    let collections = requestedCollections as {
      name: string;
      embeddingFunction?: EmbeddingFunction;
    }[];

    return Promise.all(
      collections.map(async (collection) => {
        await this.getCollection({ ...collection });
      }),
    );
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
      path: await this._path(),
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
      metadata: data.metadata ?? undefined,
      embeddingFunction:
        embeddingFunction ??
        (await getEmbeddingFunction(
          name,
          data.configuration_json.embedding_function ?? undefined,
        )),
      id: data.id,
    });
  }

  public async deleteCollection({ name }: { name: string }): Promise<void> {
    await Api.deleteCollection({
      client: this.apiClient,
      path: { ...(await this._path()), collection_id: name },
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

  public collection({
    id,
    embeddingFunction,
  }: {
    id: string;
    embeddingFunction?: EmbeddingFunction;
  }): CollectionAPI {
    return new CollectionAPIImpl({
      chromaClient: this,
      apiClient: this.apiClient,
      id,
      embeddingFunction,
    });
  }

  public collections(
    items: string[] | { id: string; embeddingFunction?: EmbeddingFunction }[],
  ) {
    if (items.length === 0) return [];

    let requestedCollections = items;
    if (typeof items[0] === "string") {
      requestedCollections = (items as string[]).map((item) => {
        return { id: item, embeddingFunction: undefined };
      });
    }

    return (
      requestedCollections as {
        id: string;
        embeddingFunction?: EmbeddingFunction;
      }[]
    ).map(
      (requestedCollection) =>
        new CollectionAPIImpl({
          chromaClient: this,
          apiClient: this.apiClient,
          id: requestedCollection.id,
          embeddingFunction: requestedCollection.embeddingFunction,
        }),
    );
  }
}
