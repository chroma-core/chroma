import { version } from "../package.json";
import { AdminClient } from "./AdminClient";
import { authOptionsToAuthProvider, ClientAuthProvider } from "./auth";
import { chromaFetch } from "./ChromaFetch";
import { Collection } from "./Collection";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { Configuration, ApiApi as DefaultApi, Api } from "./generated";
import type {
  ChromaClientParams,
  CollectionMetadata,
  CollectionParams,
  ConfigOptions,
  CreateCollectionParams,
  DeleteCollectionParams,
  GetCollectionParams,
  GetOrCreateCollectionParams,
  ListCollectionsParams,
  UserIdentity,
} from "./types";
import { validateTenantDatabase, wrapCollection } from "./utils";
import {
  loadApiCollectionConfigurationFromCreateCollectionConfiguration,
  loadCollectionConfigurationFromJson,
  hasEmbeddingFunctionConflict,
} from "./CollectionConfiguration";
import { warn } from "console";
const DEFAULT_TENANT = "default_tenant";
const DEFAULT_DATABASE = "default_database";

export class ChromaClient {
  /**
   * @ignore
   */
  public api: DefaultApi & ConfigOptions;
  /**
   * @ignore
   */
  public tenant: string;
  /**
   * @ignore
   */
  public database: string;
  /**
   * @ignore
   */
  private _adminClient: AdminClient;
  /**
   * @ignore
   */
  private authProvider: ClientAuthProvider | undefined;
  /**
   * @ignore
   */
  private _initPromise: Promise<void> | undefined;

  /**
   * Creates a new ChromaClient instance.
   * @param {Object} params - The parameters for creating a new client
   * @param {string} [params.path] - The base path for the Chroma API.
   * @returns {ChromaClient} A new ChromaClient instance.
   *
   * @example
   * ```typescript
   * const client = new ChromaClient({
   *   path: "http://localhost:8000"
   * });
   * ```
   */
  constructor({
    path = "http://localhost:8000",
    fetchOptions,
    auth,
    tenant = DEFAULT_TENANT,
    database = DEFAULT_DATABASE,
  }: ChromaClientParams = {}) {
    this.tenant = tenant;
    this.database = database;
    this.authProvider = undefined;

    const apiConfig: Configuration = new Configuration({
      basePath: path,
    });

    this.api = new DefaultApi(apiConfig, undefined, chromaFetch);
    this.api.options = fetchOptions ?? {};

    this.api.options.headers = {
      ...this.api.options.headers,
      "user-agent": `Chroma Javascript Client v${version} (https://github.com/chroma-core/chroma)`,
    };

    if (auth !== undefined) {
      this.authProvider = authOptionsToAuthProvider(auth);
      this.api.options.headers = {
        ...this.api.options.headers,
        ...this.authProvider.authenticate(),
      };
    }

    this._adminClient = new AdminClient({
      path,
      fetchOptions,
      auth,
      tenant,
      database,
    });
  }

  /** @ignore */
  async init(): Promise<void> {
    if (!this._initPromise) {
      if (this.authProvider !== undefined) {
        await this.getUserIdentity();
      }

      this._initPromise = validateTenantDatabase(
        this._adminClient,
        this.tenant,
        this.database,
      );
    }

    return this._initPromise;
  }

  /**
   * Tries to set the tenant and database for the client.
   *
   * @returns {Promise<void>} A promise that resolves when the tenant/database is resolved.
   * @throws {Error} If there is an issue resolving the tenant and database.
   *
   */
  async getUserIdentity(): Promise<void> {
    const user_identity = (await this.api.getUserIdentity(
      this.api.options,
    )) as UserIdentity;
    const user_tenant = user_identity.tenant;
    const user_databases = user_identity.databases;

    if (
      user_tenant !== null &&
      user_tenant !== undefined &&
      user_tenant !== "*" &&
      this.tenant == DEFAULT_TENANT
    ) {
      this.tenant = user_tenant;
    }

    if (
      user_databases !== null &&
      user_databases !== undefined &&
      user_databases.length == 1 &&
      user_databases[0] !== "*" &&
      this.database == DEFAULT_DATABASE
    ) {
      this.database = user_databases[0];
    }
  }

  /**
   * Resets the state of the object by making an API call to the reset endpoint.
   *
   * @returns {Promise<boolean>} A promise that resolves when the reset operation is complete.
   * @throws {ChromaConnectionError} If the client is unable to connect to the server.
   * @throws {ChromaServerError} If the server experienced an error while the state.
   *
   * @example
   * ```typescript
   * await client.reset();
   * ```
   */
  async reset(): Promise<boolean> {
    await this.init();
    return await this.api.reset(this.api.options);
  }

  /**
   * Returns the version of the Chroma API.
   * @returns {Promise<string>} A promise that resolves to the version of the Chroma API.
   * @throws {ChromaConnectionError} If the client is unable to connect to the server.
   *
   * @example
   * ```typescript
   * const version = await client.version();
   * ```
   */
  async version(): Promise<string> {
    return await this.api.version(this.api.options);
  }

  /**
   * Returns a heartbeat from the Chroma API.
   * @returns {Promise<number>} A promise that resolves to the heartbeat from the Chroma API.
   * @throws {ChromaConnectionError} If the client is unable to connect to the server.
   *
   * @example
   * ```typescript
   * const heartbeat = await client.heartbeat();
   * ```
   */
  async heartbeat(): Promise<number> {
    const response = await this.api.heartbeat(this.api.options);
    return response["nanosecond heartbeat"];
  }

  /**
   * Creates a new collection with the specified properties.
   *
   * @param {Object} params - The parameters for creating a new collection.
   * @param {string} params.name - The name of the collection.
   * @param {CollectionMetadata} [params.metadata] - Optional metadata associated with the collection.
   * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
   *
   * @returns {Promise<Collection>} A promise that resolves to the created collection.
   * @throws {ChromaConnectionError} If the client is unable to connect to the server.
   * @throws {ChromaServerError} If there is an issue creating the collection.
   *
   * @example
   * ```typescript
   * const collection = await client.createCollection({
   *   name: "my_collection",
   *   metadata: {
   *     "description": "My first collection"
   *   }
   * });
   * ```
   */
  async createCollection({
    name,
    metadata,
    embeddingFunction = new DefaultEmbeddingFunction(),
    configuration,
  }: CreateCollectionParams): Promise<Collection> {
    await this.init();
    if (!configuration) {
      configuration = {};
    }
    if (
      hasEmbeddingFunctionConflict(
        embeddingFunction,
        configuration.embedding_function,
      )
    ) {
      throw new Error(
        "Multiple embedding functions provided. Please provide only one.",
      );
    }
    if (embeddingFunction && !configuration.embedding_function) {
      configuration.embedding_function = embeddingFunction;
    }
    let collectionConfiguration: Api.CollectionConfiguration | undefined =
      undefined;
    if (configuration) {
      collectionConfiguration =
        loadApiCollectionConfigurationFromCreateCollectionConfiguration(
          configuration,
        );
    }
    const newCollection = await this.api.createCollection(
      this.tenant,
      this.database,
      {
        name,
        configuration: collectionConfiguration,
        metadata: metadata,
      },
      this.api.options,
    );

    let config: Api.CollectionConfiguration = {};
    try {
      config = newCollection.configuration_json;
    } catch {
      warn(
        "Server does not respond with configuration_json. Please update server",
      );
    }

    return wrapCollection(this, {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata as CollectionMetadata | undefined,
      embeddingFunction,
      configuration: config,
    });
  }

  /**
   * Gets or creates a collection with the specified properties.
   *
   * @param {Object} params - The parameters for creating a new collection.
   * @param {string} params.name - The name of the collection.
   * @param {CollectionMetadata} [params.metadata] - Optional metadata associated with the collection.
   * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
   *
   * @returns {Promise<Collection>} A promise that resolves to the got or created collection.
   * @throws {Error} If there is an issue getting or creating the collection.
   *
   * @example
   * ```typescript
   * const collection = await client.getOrCreateCollection({
   *   name: "my_collection",
   *   metadata: {
   *     "description": "My first collection"
   *   }
   * });
   * ```
   */
  async getOrCreateCollection({
    name,
    metadata,
    embeddingFunction = new DefaultEmbeddingFunction(),
    configuration,
  }: GetOrCreateCollectionParams): Promise<Collection> {
    await this.init();
    if (!configuration) {
      configuration = {};
    }
    if (
      hasEmbeddingFunctionConflict(
        embeddingFunction,
        configuration.embedding_function,
      )
    ) {
      throw new Error(
        "Multiple embedding functions provided. Please provide only one.",
      );
    }
    if (embeddingFunction && !configuration.embedding_function) {
      configuration.embedding_function = embeddingFunction;
    }
    let collectionConfiguration: Api.CollectionConfiguration | undefined =
      undefined;
    if (configuration) {
      collectionConfiguration =
        loadApiCollectionConfigurationFromCreateCollectionConfiguration(
          configuration,
        );
    }

    const newCollection = await this.api.createCollection(
      this.tenant,
      this.database,
      {
        name,
        configuration: collectionConfiguration,
        metadata: metadata,
        get_or_create: true,
      },
      this.api.options,
    );

    let config: Api.CollectionConfiguration = {};
    try {
      config = newCollection.configuration_json;
    } catch {
      warn(
        "Server does not respond with configuration_json. Please update server",
      );
    }

    return wrapCollection(this, {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata as CollectionMetadata | undefined,
      embeddingFunction,
      configuration: config,
    });
  }

  /**
   * Get all collection names.
   *
   * @returns {Promise<string[]>} A promise that resolves to a list of collection names.
   * @param {PositiveInteger} [params.limit] - Optional limit on the number of items to get.
   * @param {PositiveInteger} [params.offset] - Optional offset on the items to get.
   * @throws {Error} If there is an issue listing the collections.
   *
   * @example
   * ```typescript
   * const collections = await client.listCollections({
   *     limit: 10,
   *     offset: 0,
   * });
   * ```
   */
  async listCollections({ limit, offset }: ListCollectionsParams = {}): Promise<
    string[]
  > {
    await this.init();

    const response = (await this.api.listCollections(
      this.tenant,
      this.database,
      limit,
      offset,
      this.api.options,
    )) as { name: string; tenant: string; database: string }[];

    return response.map((collection) => collection.name);
  }

  /**
   * List collection names, IDs, and metadata.
   *
   * @param {PositiveInteger} [params.limit] - Optional limit on the number of items to get.
   * @param {PositiveInteger} [params.offset] - Optional offset on the items to get.
   * @throws {Error} If there is an issue listing the collections.
   * @returns {Promise<{ name: string, id: string, metadata?: CollectionMetadata }[]>} A promise that resolves to a list of collection names, IDs, and metadata.
   *
   * @example
   * ```typescript
   * const collections = await client.listCollectionsAndMetadata({
   *    limit: 10,
   *    offset: 0,
   * });
   */
  async listCollectionsAndMetadata({
    limit,
    offset,
  }: ListCollectionsParams = {}): Promise<
    {
      name: string;
      id: string;
      metadata?: CollectionMetadata;
    }[]
  > {
    await this.init();
    const results = (await this.api.listCollections(
      this.tenant,
      this.database,
      limit,
      offset,
      this.api.options,
    )) as { name: string; id: string; metadata?: CollectionMetadata }[];

    return results ?? [];
  }

  /**
   * Counts all collections.
   *
   * @returns {Promise<number>} A promise that resolves to the number of collections.
   * @throws {Error} If there is an issue counting the collections.
   *
   * @example
   * ```typescript
   * const collections = await client.countCollections();
   * ```
   */
  async countCollections(): Promise<number> {
    await this.init();
    const response = (await this.api.countCollections(
      this.tenant,
      this.database,
      this.api.options,
    )) as number;

    return response;
  }

  /**
   * Gets a collection with the specified name.
   * @param {Object} params - The parameters for getting a collection.
   * @param {string} params.name - The name of the collection.
   * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
   * @returns {Promise<Collection>} A promise that resolves to the collection.
   * @throws {Error} If there is an issue getting the collection.
   *
   * @example
   * ```typescript
   * const collection = await client.getCollection({
   *   name: "my_collection"
   * });
   * ```
   */
  async getCollection({
    name,
    embeddingFunction,
  }: GetCollectionParams): Promise<Collection> {
    await this.init();
    const response = await this.api.getCollection(
      this.tenant,
      this.database,
      name,
      this.api.options,
    );

    let config: Api.CollectionConfiguration = {};
    try {
      config = response.configuration_json;
    } catch {
      warn(
        "Server does not respond with configuration_json. Please update server",
      );
    }

    const configObj = loadCollectionConfigurationFromJson(config);
    if (
      hasEmbeddingFunctionConflict(
        embeddingFunction,
        configObj.embedding_function,
      )
    ) {
      throw new Error(
        "Multiple embedding functions provided. Please provide only one.",
      );
    }

    const ef = configObj.embedding_function ?? embeddingFunction;

    return wrapCollection(this, {
      id: response.id,
      name: response.name,
      metadata: response.metadata as CollectionMetadata | undefined,
      embeddingFunction: ef ?? new DefaultEmbeddingFunction(),
      configuration: config,
    });
  }

  /**
   * Deletes a collection with the specified name.
   * @param {Object} params - The parameters for deleting a collection.
   * @param {string} params.name - The name of the collection.
   * @returns {Promise<void>} A promise that resolves when the collection is deleted.
   * @throws {Error} If there is an issue deleting the collection.
   *
   * @example
   * ```typescript
   * await client.deleteCollection({
   *  name: "my_collection"
   * });
   * ```
   */
  async deleteCollection({ name }: DeleteCollectionParams): Promise<void> {
    await this.init();

    await this.api.deleteCollection(
      this.tenant,
      this.database,
      name,
      this.api.options,
    );
  }
}
