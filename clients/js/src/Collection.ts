import { ChromaClient } from "./ChromaClient";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";
import {
  CollectionMetadata,
  AddRecordsParams,
  UpsertRecordsParams,
  BaseGetParams,
  GetResponse,
  AddResponse,
  UpdateRecordsParams,
  QueryRecordsParams,
  MultiQueryResponse,
  PeekParams,
  MultiGetResponse,
  DeleteParams,
  Embeddings,
  CollectionParams,
} from "./types";
import {
  prepareRecordRequestForUpsert,
  prepareRecordRequestForUpdate,
  prepareRecordRequestWithIDsOptional,
  toArray,
  toArrayOfArrays,
} from "./utils";
import { Api as GeneratedApi } from "./generated";

export class Collection {
  public name: string;
  public id: string;
  public metadata: CollectionMetadata | undefined;
  /**
   * @ignore
   */
  private client: ChromaClient;
  /**
   * @ignore
   */
  public embeddingFunction: IEmbeddingFunction;

  /**
   * @ignore
   */
  constructor(
    name: string,
    id: string,
    client: ChromaClient,
    embeddingFunction: IEmbeddingFunction,
    metadata?: CollectionMetadata,
  ) {
    this.name = name;
    this.id = id;
    this.metadata = metadata;
    this.client = client;
    this.embeddingFunction = embeddingFunction;
  }

  /**
   * Add items to the collection
   * @param {Object} params - The parameters for the query.
   * @param {ID | IDs} [params.ids] - Optional IDs of the items to add.
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
  async add(params: AddRecordsParams): Promise<AddResponse> {
    await this.client.init();

    return (await this.client.api.add(
      this.id,
      // TODO: For some reason the auto generated code requires metadata to be defined here.
      (await prepareRecordRequestWithIDsOptional(
        params,
        this.embeddingFunction,
      )) as GeneratedApi.AddEmbedding,
      this.client.api.options,
    )) as AddResponse;
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
   * const response = await collection.upsert({
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"],
   * });
   * ```
   */
  async upsert(params: UpsertRecordsParams): Promise<void> {
    await this.client.init();

    await this.client.api.upsert(
      this.id,
      // TODO: For some reason the auto generated code requires metadata to be defined here.
      (await prepareRecordRequestForUpsert(
        params,
        this.embeddingFunction,
      )) as GeneratedApi.AddEmbedding,
      this.client.api.options,
    );
  }

  /**
   * Count the number of items in the collection
   * @returns {Promise<number>} - The number of items in the collection.
   *
   * @example
   * ```typescript
   * const count = await collection.count();
   * ```
   */
  async count(): Promise<number> {
    await this.client.init();
    return (await this.client.api.count(
      this.id,
      this.client.api.options,
    )) as number;
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
  async get({
    ids,
    where,
    limit,
    offset,
    include,
    whereDocument,
  }: BaseGetParams = {}): Promise<GetResponse> {
    await this.client.init();

    const idsArray = ids ? toArray(ids) : undefined;

    const resp = (await this.client.api.aGet(
      this.id,
      {
        ids: idsArray,
        where,
        limit,
        offset,
        include,
        where_document: whereDocument,
      },
      this.client.api.options,
    )) as MultiGetResponse;

    return resp;
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
   * const response = await collection.update({
   *   ids: ["id1", "id2"],
   *   embeddings: [[1, 2, 3], [4, 5, 6]],
   *   metadatas: [{ "key": "value" }, { "key": "value" }],
   *   documents: ["document1", "document2"],
   * });
   * ```
   */
  async update(params: UpdateRecordsParams): Promise<void> {
    await this.client.init();

    await this.client.api.update(
      this.id,
      await prepareRecordRequestForUpdate(params, this.embeddingFunction),
      this.client.api.options,
    );
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
  async query({
    nResults = 10,
    where,
    whereDocument,
    include,
    queryTexts,
    queryEmbeddings,
  }: QueryRecordsParams): Promise<MultiQueryResponse> {
    if ((queryTexts && queryEmbeddings) || (!queryTexts && !queryEmbeddings)) {
      throw new Error(
        "You must supply exactly one of queryTexts or queryEmbeddings.",
      );
    }

    await this.client.init();

    const arrayQueryEmbeddings: Embeddings =
      queryTexts !== undefined
        ? await this.embeddingFunction.generate(toArray(queryTexts))
        : toArrayOfArrays<number>(queryEmbeddings);

    return (await this.client.api.getNearestNeighbors(
      this.id,
      {
        query_embeddings: arrayQueryEmbeddings,
        where,
        n_results: nResults,
        where_document: whereDocument,
        include,
      },
      this.client.api.options,
    )) as MultiQueryResponse;
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
  async modify({
    name,
    metadata,
  }: {
    name?: string;
    metadata?: CollectionMetadata;
  }): Promise<CollectionParams> {
    await this.client.init();
    return this.client.api
      .updateCollection(
        this.id,
        {
          new_name: name,
          new_metadata: metadata,
        },
        this.client.api.options,
      )
      .then(() => {
        if (name !== undefined) {
          this.name = name;
        }
        if (metadata !== undefined) {
          this.metadata = metadata;
        }
        return {
          name: this.name,
          metadata: this.metadata,
        } as CollectionParams;
      });
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
  async peek({ limit = 10 }: PeekParams = {}): Promise<MultiGetResponse> {
    await this.client.init();
    return (await this.client.api.aGet(
      this.id,
      {
        limit,
      },
      this.client.api.options,
    )) as MultiGetResponse;
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
  async delete({ ids, where, whereDocument }: DeleteParams = {}): Promise<
    string[]
  > {
    await this.client.init();
    let idsArray = undefined;
    if (ids !== undefined) idsArray = toArray(ids);
    return (await this.client.api.aDelete(
      this.id,
      { ids: idsArray, where: where, where_document: whereDocument },
      this.client.api.options,
    )) as string[];
  }
}
