import {
  Configuration,
  ApiApi as DefaultApi,
  Api as GeneratedApi,
} from "./generated";
import {
  handleSuccess,
  prepareDocumentRequest,
  toArray,
  toQueryArray,
  validateTenantDatabase,
} from "./utils";
import {
  ChromaClientParams,
  ConfigOptions,
  CreateCollectionParams,
  DeleteCollectionParams,
  GetParams,
  GetResponse,
  ListCollectionsParams,
  QueryDocumentsParams,
  Collection,
  SingleQueryParams,
  MultiQueryParams,
  GetOrCreateCollectionParams,
  GetCollectionParams,
  AddDocumentsParams,
  AddResponse,
  UpsertDocumentsParams,
  Embedding,
  Embeddings,
  SingleQueryResponse,
  MultiQueryResponse,
  DeleteParams,
  PeekParams,
  SingleGetResponse,
  SingleGetParams,
  MultiGetResponse,
  MultiGetParams,
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
      path: path,
      fetchOptions: fetchOptions,
      auth: auth,
      tenant: tenant,
      database: database,
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
    await this.initPromise;
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
    await this.initPromise;
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

    return {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata,
      embeddingFunction,
    };
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

    return {
      name: newCollection.name,
      id: newCollection.id,
      metadata: newCollection.metadata,
      embeddingFunction,
    };
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
    const response = await this.api
      .getCollection(name, this.tenant, this.database, this.api.options)
      .then(handleSuccess);

    return {
      name: response.name,
      id: response.id,
      metadata: response.metadata,
      embeddingFunction,
    };
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
   * const response = await collection.modify({
   *   name: "new name",
   *   metadata: { "key": "value" },
   * });
   * ```
   */
  async updateCollection(collection: Collection): Promise<Collection> {
    await this.initPromise;
    const response = await this.api
      .updateCollection(
        collection.id,
        {
          new_name: collection.name,
          new_metadata: collection.metadata,
        },
        this.api.options,
      )
      .then(handleSuccess);

    return response;
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
    await this.initPromise;
    return await this.api
      .deleteCollection(name, this.tenant, this.database, this.api.options)
      .then(handleSuccess);
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
   * const response = await collection.add({
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"]
   * });
   * ```
   */
  async addDocuments(
    collection: Collection,
    params: AddDocumentsParams,
  ): Promise<AddResponse> {
    await this.initPromise;
    return await this.api
      .add(
        collection.id,
        // TODO: For some reason the auto generated code requires metadata to be defined here.
        (await prepareDocumentRequest(
          params,
          collection.embeddingFunction,
        )) as GeneratedApi.AddEmbedding,
        this.api.options,
      )
      .then(handleSuccess);
  }

  /**
   * Upsert items to the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - IDs of the items to add.
   * @param {Embedding | Embeddings} [params.embeddings] - Optional embeddings of the items to add.
   * @param {Metadata | Metadatas} [params.metadatas] - Optional metadata of the items to add.
   * @param {Document | Documents} [params.documents] - Optional documents of the items to add.
   * @returns {Promise<boolean>} - The response from the API. True if successful.
   *
   * @example
   * ```typescript
   * const response = await collection.upsert({
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"],
   * });
   * ```
   */
  async setDocuments(collection: Collection, params: UpsertDocumentsParams) {
    await this.initPromise;
    return await this.api
      .upsert(
        collection.id,
        // TODO: For some reason the auto generated code requires metadata to be defined here.
        (await prepareDocumentRequest(
          params,
          collection.embeddingFunction,
        )) as GeneratedApi.AddEmbedding,
        this.api.options,
      )
      .then(handleSuccess);
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
   * const response = await collection.get({
   *   ids: ["id1", "id2"],
   *   where: { "key": "value" },
   *   limit: 10,
   *   offset: 0,
   *   include: ["embeddings", "metadatas", "documents"],
   *   whereDocument: { $contains: "value" },
   * });
   * ```
   */
  async getDocuments(
    collection: Collection,
    params: SingleGetParams,
  ): Promise<SingleGetResponse>;
  async getDocuments(
    collection: Collection,
    params?: MultiGetParams,
  ): Promise<MultiGetResponse>;
  async getDocuments(
    collection: Collection,
    { ids, where, limit, offset, include, whereDocument }: GetParams = {},
  ): Promise<GetResponse> {
    await this.initPromise;
    const idsArray = ids ? toArray(ids) : undefined;

    const resp = (await this.api
      .aGet(
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
      )
      .then(handleSuccess)) as MultiGetResponse;

    if (Array.isArray(ids)) {
      return resp;
    }

    return {
      ids: resp.ids?.[0] ?? null,
      embeddings: resp.embeddings?.[0] ?? null,
      documents: resp.documents?.[0] ?? null,
      metadatas: resp.metadatas?.[0] ?? null,
      included: resp.included,
      error: resp.error,
    };
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
   * const results = await collection.query({
   *   queryEmbeddings: [[0.1, 0.2, ...], ...],
   *   nResults: 10,
   *   where: {"name": {"$eq": "John Doe"}},
   *   include: ["metadata", "document"]
   * });
   * @example
   * ```js
   * // Query the collection using query text
   * const results = await collection.query({
   *   queryTexts: "some text",
   *   nResults: 10,
   *   where: {"name": {"$eq": "John Doe"}},
   *   include: ["metadata", "document"]
   * });
   * ```
   *
   */
  queryDocuments(
    collection: Collection,
    params: SingleQueryParams,
  ): Promise<SingleQueryResponse>;
  queryDocuments(
    collection: Collection,
    params: MultiQueryParams,
  ): Promise<MultiQueryResponse>;
  public async queryDocuments(
    collection: Collection,
    {
      nResults = 10,
      where,
      whereDocument,
      include,
      query,
    }: QueryDocumentsParams,
  ): Promise<SingleQueryResponse | MultiQueryResponse> {
    await this.initPromise;
    const [arrayQuery, wasSingular] = toQueryArray(query);

    if (arrayQuery.length === 0) {
      throw new Error("Query must contain at least one document or embedding.");
    }

    const embeddings =
      typeof arrayQuery[0] === "string"
        ? await collection.embeddingFunction.generate(arrayQuery as string[])
        : (arrayQuery as Embeddings);

    const response = (await this.api
      .getNearestNeighbors(
        collection.id,
        {
          query_embeddings: embeddings,
          where,
          n_results: nResults,
          where_document: whereDocument,
          include,
        },
        this.api.options,
      )
      .then(handleSuccess)) as MultiQueryResponse;

    return wasSingular
      ? {
          ids: response.ids[0],
          embeddings: response.embeddings?.[0] ?? null,
          documents: response.documents?.[0] ?? null,
          metadatas: response.metadatas?.[0] ?? null,
          distances: response.distances?.[0] ?? null,
          included: response.included,
        }
      : response;
  }

  async countDocuments(collection: Collection): Promise<number> {
    await this.initPromise;
    return await this.api
      .count(collection.id, this.api.options)
      .then(handleSuccess);
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
   * const results = await collection.delete({
   *   ids: "some_id",
   *   where: {"name": {"$eq": "John Doe"}},
   *   whereDocument: {"$contains":"search_string"}
   * });
   * ```
   */
  public async deleteDocuments(
    collection: Collection,
    { ids, where, whereDocument }: DeleteParams = {},
  ): Promise<string[]> {
    let idsArray = undefined;
    if (ids !== undefined) idsArray = toArray(ids);
    return await this.api
      .aDelete(
        collection.id,
        { ids: idsArray, where: where, where_document: whereDocument },
        this.api.options,
      )
      .then(handleSuccess);
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
   * const results = await collection.peek({
   *   limit: 10
   * });
   * ```
   */
  async peekDocuments(
    collection: Collection,
    { limit }: PeekParams = {},
  ): Promise<GetResponse> {
    if (limit === undefined) limit = 10;
    const response = await this.api.aGet(
      collection.id,
      {
        limit,
      },
      this.api.options,
    );
    return handleSuccess(response);
  }
}
