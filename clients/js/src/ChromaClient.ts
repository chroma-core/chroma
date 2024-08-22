import { AdminClient } from "./AdminClient";
import { authOptionsToAuthProvider, ClientAuthProvider } from "./auth";
import { chromaFetch } from "./ChromaFetch";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import {
  Configuration,
  ApiApi as DefaultApi,
  Api as GeneratedApi,
} from "./generated";
import type {
  AddRecordsParams,
  AddResponse,
  BaseGetParams,
  ChromaClientParams,
  Collection,
  ConfigOptions,
  CreateCollectionParams,
  DeleteCollectionParams,
  DeleteParams,
  Embeddings,
  GetCollectionParams,
  GetOrCreateCollectionParams,
  GetResponse,
  ListCollectionsParams,
  MultiGetResponse,
  MultiQueryResponse,
  PeekParams,
  QueryRecordsParams,
  UpdateRecordsParams,
  UpsertRecordsParams,
} from "./types";
import {
  prepareRecordRequest,
  toArray,
  toArrayOfArrays,
  validateTenantDatabase,
  wrapCollection,
} from "./utils";

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
  private init(): Promise<void> {
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
    )) as Collection;

    return wrapCollection({
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
    )) as Collection;

    return wrapCollection({
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
    Collection[]
  > {
    await this.init();
    return (await this.api.listCollections(
      limit,
      offset,
      this.tenant,
      this.database,
      this.api.options,
    )) as Collection[];
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
      name,
      this.tenant,
      this.database,
      this.api.options,
    )) as Collection;

    return wrapCollection({
      name: response.name,
      id: response.id,
      metadata: response.metadata,
      embeddingFunction,
    });
  }

  /**
   * Modify the collection name or metadata
   * @param {Object} params - The parameters for the query.
   * @param {string} [params.name] - Optional new name for the collection.
   * @param {CollectionMetadata} [params.metadata] - Optional new metadata for the collection.
   * @returns {Promise<void>} - The response from the API.
   *
   * @example
   * ```typescript
   * const response = await client.updateCollection({
   *   name: "new name",
   *   metadata: { "key": "value" },
   * });
   * ```
   */
  async updateCollection(collection: Collection): Promise<Collection> {
    await this.init();
    return (await this.api.updateCollection(
      collection.id,
      {
        new_name: collection.name,
        new_metadata: collection.metadata,
      },
      this.api.options,
    )) as Collection;
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

  /**
   * Add items to the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - IDs of the items to add.
   * @param {Embedding | Embeddings} [params.embeddings] - Optional embeddings of the items to add.
   * @param {Metadata | Metadatas} [params.metadatas] - Optional metadata of the items to add.
   * @param {Document | Documents} [params.documents] - Optional documents of the items to add.
   * @returns {Promise<AddResponse>} - The response from the API. True if successful.
   *
   * @example
   * ```typescript
   * const response = await client.addRecords(collection, {
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"]
   * });
   * ```
   */
  async addRecords(
    collection: Collection,
    params: AddRecordsParams,
  ): Promise<AddResponse> {
    await this.init();

    const resp = (await this.api.add(
      collection.id,
      // TODO: For some reason the auto generated code requires metadata to be defined here.
      (await prepareRecordRequest(
        params,
        collection.embeddingFunction,
      )) as GeneratedApi.AddEmbedding,
      this.api.options,
    )) as AddResponse;

    return resp;
  }

  /**
   * Upsert items to the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - IDs of the items to add.
   * @param {Embedding | Embeddings} [params.embeddings] - Optional embeddings of the items to add.
   * @param {Metadata | Metadatas} [params.metadatas] - Optional metadata of the items to add.
   * @param {Document | Documents} [params.documents] - Optional documents of the items to add.
   * @returns {Promise<void>}
   *
   * @example
   * ```typescript
   * const response = await client.upsertRecords(collection, {
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"],
   * });
   * ```
   */
  async upsertRecords(collection: Collection, params: UpsertRecordsParams) {
    await this.init();

    await this.api.upsert(
      collection.id,
      // TODO: For some reason the auto generated code requires metadata to be defined here.
      (await prepareRecordRequest(
        params,
        collection.embeddingFunction,
      )) as GeneratedApi.AddEmbedding,
      this.api.options,
    );
  }

  /**
   * Update items in the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - IDs of the items to add.
   * @param {Embedding | Embeddings} [params.embeddings] - Optional embeddings of the items to add.
   * @param {Metadata | Metadatas} [params.metadatas] - Optional metadata of the items to add.
   * @param {Document | Documents} [params.documents] - Optional documents of the items to add.
   * @returns {Promise<void>}
   *
   * @example
   * ```typescript
   * const response = await client.updateRecords(collection, {
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"],
   * });
   * ```
   */
  async updateRecords(collection: Collection, params: UpdateRecordsParams) {
    await this.init();

    await this.api.update(
      collection.id,
      await prepareRecordRequest(params, collection.embeddingFunction, true),
      this.api.options,
    );
  }

  /**
   * Get items from the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - Optional IDs of the items to get.
   * @param {Where} [params.where] - Optional where clause to filter items by.
   * @param {PositiveInteger} [params.limit] - Optional limit on the number of items to get.
   * @param {PositiveInteger} [params.offset] - Optional offset on the items to get.
   * @param {IncludeEnum[]} [params.include] - Optional list of items to include in the response.
   * @param {WhereDocument} [params.whereDocument] - Optional where clause to filter items by.
   * @returns {Promise<GetResponse>} - The response from the server.
   *
   * @example
   * ```typescript
   * const response = await client.getRecords(collection, {
   *   ids: ["id1", "id2"],
   *   where: { "key": "value" },
   *   limit: 10,
   *   offset: 0,
   *   include: ["embeddings", "metadatas", "documents"],
   *   whereDocument: { $contains: "value" },
   * });
   * ```
   */
  async getRecords(
    collection: Collection,
    { ids, where, limit, offset, include, whereDocument }: BaseGetParams = {},
  ): Promise<GetResponse> {
    await this.init();

    const idsArray = ids ? toArray(ids) : undefined;

    const resp = (await this.api.aGet(
      collection.id,
      {
        ids: idsArray,
        where,
        limit,
        offset,
        include,
        where_document: whereDocument,
      },
      this.api.options,
    )) as MultiGetResponse;

    return resp;
  }

  /**
   * Performs a query on the collection using the specified parameters.
   *
   * @param {Object} params - The parameters for the query.
   * @param {Embedding | Embeddings} [params.queryEmbeddings] - Optional query embeddings to use for the search.
   * @param {PositiveInteger} [params.nResults] - Optional number of results to return (default is 10).
   * @param {Where} [params.where] - Optional query condition to filter results based on metadata values.
   * @param {string | string[]} [params.queryTexts] - Optional query text(s) to search for in the collection.
   * @param {WhereDocument} [params.whereDocument] - Optional query condition to filter results based on document content.
   * @param {IncludeEnum[]} [params.include] - Optional array of fields to include in the result, such as "metadata" and "document".
   *
   * @returns {Promise<QueryResponse>} A promise that resolves to the query results.
   * @throws {Error} If there is an issue executing the query.
   * @example
   * // Query the collection using embeddings
   * const results = await client.queryRecords(collection, {
   *   queryEmbeddings: [[0.1, 0.2, ...], ...],
   *   nResults: 10,
   *   where: {"name": {"$eq": "John Doe"}},
   *   include: ["metadata", "document"]
   * });
   * @example
   * ```js
   * // Query the collection using query text
   * const results = await client.queryRecords(collection, {
   *   queryTexts: "some text",
   *   nResults: 10,
   *   where: {"name": {"$eq": "John Doe"}},
   *   include: ["metadata", "document"]
   * });
   * ```
   *
   */
  async queryRecords(
    collection: Collection,
    {
      nResults = 10,
      where,
      whereDocument,
      include,
      queryTexts,
      queryEmbeddings,
    }: QueryRecordsParams,
  ): Promise<MultiQueryResponse> {
    if ((queryTexts && queryEmbeddings) || (!queryTexts && !queryEmbeddings)) {
      throw new Error(
        "You must supply exactly one of queryTexts or queryEmbeddings.",
      );
    }

    await this.init();

    const arrayQueryEmbeddings: Embeddings =
      queryTexts !== undefined
        ? await collection.embeddingFunction.generate(toArray(queryTexts))
        : toArrayOfArrays<number>(queryEmbeddings);

    return (await this.api.getNearestNeighbors(
      collection.id,
      {
        query_embeddings: arrayQueryEmbeddings,
        where,
        n_results: nResults,
        where_document: whereDocument,
        include,
      },
      this.api.options,
    )) as MultiQueryResponse;
  }

  async countRecords(collection: Collection): Promise<number> {
    await this.init();
    return (await this.api.count(collection.id, this.api.options)) as number;
  }

  /**
   * Deletes items from the collection.
   * @param {Object} params - The parameters for deleting items from the collection.
   * @param {ID | IDs} [params.ids] - Optional ID or array of IDs of items to delete.
   * @param {Where} [params.where] - Optional query condition to filter items to delete based on metadata values.
   * @param {WhereDocument} [params.whereDocument] - Optional query condition to filter items to delete based on document content.
   * @returns {Promise<string[]>} A promise that resolves to the IDs of the deleted items.
   * @throws {Error} If there is an issue deleting items from the collection.
   *
   * @example
   * ```typescript
   * const results = await client.deleteRecords(collection, {
   *   ids: "some_id",
   *   where: {"name": {"$eq": "John Doe"}},
   *   whereDocument: {"$contains":"search_string"}
   * });
   * ```
   */
  async deleteRecords(
    collection: Collection,
    { ids, where, whereDocument }: DeleteParams = {},
  ): Promise<string[]> {
    await this.init();
    let idsArray = undefined;
    if (ids !== undefined) idsArray = toArray(ids);
    return (await this.api.aDelete(
      collection.id,
      { ids: idsArray, where: where, where_document: whereDocument },
      this.api.options,
    )) as string[];
  }

  /**
   * Peek inside the collection
   * @param {Object} params - The parameters for the query.
   * @param {PositiveInteger} [params.limit] - Optional number of results to return (default is 10).
   * @returns {Promise<GetResponse>} A promise that resolves to the query results.
   * @throws {Error} If there is an issue executing the query.
   *
   * @example
   * ```typescript
   * const results = await client.peekRecords(collection, {
   *   limit: 10
   * });
   * ```
   */
  async peekRecords(
    collection: Collection,
    { limit = 10 }: PeekParams = {},
  ): Promise<MultiGetResponse> {
    await this.init();
    return (await this.api.aGet(
      collection.id,
      {
        limit,
      },
      this.api.options,
    )) as MultiGetResponse;
  }
}
