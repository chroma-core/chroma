import { Configuration, ApiApi as DefaultApi } from "./generated";
import { handleSuccess, validateTenantDatabase } from "./utils";
import { Collection } from "./Collection";
import {
  ChromaClientParams,
  CollectionType,
  ConfigOptions,
  CreateCollectionParams,
  DeleteCollectionParams,
  GetCollectionParams,
  GetOrCreateCollectionParams,
  ListCollectionsParams,
} from "./types";
import { authOptionsToAuthProvider, ClientAuthProvider } from "./auth";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { AdminClient } from "./AdminClient";
import { chromaFetch } from "./ChromaFetch";
import { ChromaConnectionError, ChromaServerError } from "./Errors";

const DEFAULT_TENANT = "default_tenant";
const DEFAULT_DATABASE = "default_database";

export class ChromaClient {
  /**
   * @ignore
   */
  private api: DefaultApi & ConfigOptions;
  private tenant: string;
  private database: string;
  private _adminClient: AdminClient;
  private authProvider: ClientAuthProvider | undefined;
  private initPromise: Promise<void>;

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

    this.initPromise = this.init();
  }

  /** @ignore */
  private async init() {
    await validateTenantDatabase(this._adminClient, this.tenant, this.database);
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
    await this.initPromise;
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
    const response = await this.api.version(this.api.options);
    return await handleSuccess(response);
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
    let ret = await handleSuccess(response);
    return ret["nanosecond heartbeat"];
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
  }: CreateCollectionParams): Promise<Collection> {
    await this.initPromise;
    const newCollection = await this.api
      .createCollection(
        this.tenant,
        this.database,
        {
          name,
          configuration: null, //TODO: Configuration type in JavaScript
          metadata,
        },
        this.api.options,
      )
      .then(handleSuccess);

    if (newCollection.error) {
      throw newCollection.error instanceof Error
        ? newCollection.error
        : new Error(newCollection.error);
    }

    return new Collection(
      name,
      newCollection.id,
      this.api,
      metadata,
      embeddingFunction,
    );
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
  }: GetOrCreateCollectionParams): Promise<Collection> {
    await this.initPromise;
    const newCollection = await this.api
      .createCollection(
        this.tenant,
        this.database,
        {
          name,
          metadata,
          configuration: null,
          get_or_create: true,
        },
        this.api.options,
      )
      .then(handleSuccess);

    return new Collection(
      name,
      newCollection.id,
      this.api,
      newCollection.metadata,
      embeddingFunction,
    );
  }

  /**
   * Lists all collections.
   *
   * @returns {Promise<CollectionType[]>} A promise that resolves to a list of collection names.
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
    CollectionType[]
  > {
    await this.initPromise;
    const response = await this.api.listCollections(
      limit,
      offset,
      this.tenant,
      this.database,
      this.api.options,
    );
    return handleSuccess(response);
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
    await this.initPromise;

    const response = await this.api.countCollections(
      this.tenant,
      this.database,
      this.api.options,
    );
    return handleSuccess(response);
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
    await this.initPromise;

    const response = await this.api
      .getCollection(name, this.tenant, this.database, this.api.options)
      .then(handleSuccess);

    return new Collection(
      response.name,
      response.id,
      this.api,
      response.metadata,
      embeddingFunction,
    );
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
    await this.initPromise;

    return await this.api
      .deleteCollection(name, this.tenant, this.database, this.api.options)
      .then(handleSuccess);
  }
}
