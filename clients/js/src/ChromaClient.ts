import { Configuration, ApiApi as DefaultApi } from "./generated";
import { handleSuccess } from "./utils";
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
  private tenant: string = DEFAULT_TENANT;
  private database: string = DEFAULT_DATABASE;
  private _adminClient?: AdminClient;
  private authProvider: ClientAuthProvider | undefined;

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
    path,
    fetchOptions,
    auth,
    tenant = DEFAULT_TENANT,
    database = DEFAULT_DATABASE,
  }: ChromaClientParams = {}) {
    if (path === undefined) path = "http://localhost:8000";
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
      path: path,
      fetchOptions: fetchOptions,
      auth: auth,
      tenant: tenant,
      database: database,
    });

    // TODO: Validate tenant and database on client creation
    // this got tricky because:
    // - the constructor is sync but the generated api is async
    // - we need to inject auth information so a simple rewrite/fetch does not work
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
  public async reset(): Promise<boolean> {
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
  public async version(): Promise<string> {
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
  public async heartbeat(): Promise<number> {
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
  public async createCollection({
    name,
    metadata,
    embeddingFunction,
  }: CreateCollectionParams): Promise<Collection> {
    if (embeddingFunction === undefined) {
      embeddingFunction = new DefaultEmbeddingFunction();
    }

    const newCollection = await this.api
      .createCollection(
        this.tenant,
        this.database,
        {
          name,
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
  public async getOrCreateCollection({
    name,
    metadata,
    embeddingFunction,
  }: GetOrCreateCollectionParams): Promise<Collection> {
    if (embeddingFunction === undefined) {
      embeddingFunction = new DefaultEmbeddingFunction();
    }

    const newCollection = await this.api
      .createCollection(
        this.tenant,
        this.database,
        {
          name,
          metadata,
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
  public async listCollections({
    limit,
    offset,
  }: ListCollectionsParams = {}): Promise<CollectionType[]> {
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
  public async countCollections(): Promise<number> {
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
  public async getCollection({
    name,
    embeddingFunction,
  }: GetCollectionParams): Promise<Collection> {
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
  public async deleteCollection({
    name,
  }: DeleteCollectionParams): Promise<void> {
    return await this.api
      .deleteCollection(name, this.tenant, this.database, this.api.options)
      .then(handleSuccess);
  }
}
