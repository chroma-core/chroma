import { AdminClient } from "./AdminClient";
import { authOptionsToAuthProvider, ClientAuthProvider } from "./auth";
import { chromaFetch } from "./ChromaFetch";
import { Collection } from "./Collection";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { Configuration, ApiApi as DefaultApi } from "./generated";
import type {
  ChromaClientParams,
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
    await this.getUserIdentity();

    if (!this._initPromise) {
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
      user_tenant !== "*"
    ) {
      this.tenant = user_tenant;
    }

    if (
      user_databases !== null &&
      user_databases !== undefined &&
      user_databases.length == 1 &&
      user_databases[0] !== "*"
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
    return await this.api.postV2Reset(this.api.options);
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
    return await this.api.getV2Version(this.api.options);
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
    const response = await this.api.getV2Heartbeat(this.api.options);
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
  }: CreateCollectionParams): Promise<Collection> {
    await this.init();
    const newCollection = (await this.api.createCollection(
      this.tenant,
      this.database,
      {
        name,
        // @ts-ignore: we need to generate the client libraries again
        configuration: null, //TODO: Configuration type in JavaScript
        metadata,
      },
      this.api.options,
    )) as CollectionParams;

    return wrapCollection(this, {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata,
      embeddingFunction,
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
  }: GetOrCreateCollectionParams): Promise<Collection> {
    await this.init();
    const newCollection = (await this.api.createCollection(
      this.tenant,
      this.database,
      {
        name,
        // @ts-ignore: we need to generate the client libraries again
        configuration: null, //TODO: Configuration type in JavaScript
        metadata,
        get_or_create: true,
      },
      this.api.options,
    )) as CollectionParams;

    return wrapCollection(this, {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata,
      embeddingFunction,
    });
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
    CollectionParams[]
  > {
    await this.init();
    return (await this.api.listCollections(
      this.tenant,
      this.database,
      limit,
      offset,
      this.api.options,
    )) as CollectionParams[];
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

    return (await this.api.countCollections(
      this.tenant,
      this.database,
      this.api.options,
    )) as number;
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

    const response = (await this.api.getCollection(
      this.tenant,
      this.database,
      name,
      this.api.options,
    )) as CollectionParams;

    return wrapCollection(this, {
      name: response.name,
      id: response.id,
      metadata: response.metadata,
      embeddingFunction,
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
      name,
      this.tenant,
      this.database,
      this.api.options,
    );
  }
}
