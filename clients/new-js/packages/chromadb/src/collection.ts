import { ChromaClient } from "./chroma-client";
import {
  EmbeddingFunction,
  SparseEmbeddingFunction,
  getEmbeddingFunction,
} from "./embedding-function";
import {
  BaseRecordSet,
  CollectionMetadata,
  DeleteResult,
  GetResult,
  IndexingStatus,
  Metadata,
  PreparedRecordSet,
  PreparedInsertRecordSet,
  QueryRecordSet,
  QueryResult,
  ReadLevel,
  RecordSet,
  Where,
  WhereDocument,
} from "./types";
import { Include, SparseVector, SearchPayload } from "./api";
import { CollectionService, RecordService } from "./api";
import {
  validateRecordSetLengthConsistency,
  validateIDs,
  validateInclude,
  validateBaseRecordSet,
  validateWhere,
  validateWhereDocument,
  validateNResults,
  validateMetadata,
  validateMaxBatchSize,
  embeddingsToBase64Bytes,
  serializeMetadatas,
  serializeMetadata,
  deserializeMetadatas,
  deserializeMetadataMatrix,
  deserializeMetadata,
} from "./utils";
import { createClient } from "@hey-api/client-fetch";
import { ChromaValueError } from "./errors";
import {
  CollectionConfiguration,
  processUpdateCollectionConfig,
  UpdateCollectionConfiguration,
} from "./collection-configuration";
import { SearchLike, SearchResult, toSearch } from "./execution";
import { isPlainObject } from "./execution/expression/common";
import { Schema, EMBEDDING_KEY, DOCUMENT_KEY } from "./schema";
import type { SparseVectorIndexConfig } from "./schema";

/**
 * Interface for collection operations using collection ID.
 * Provides methods for adding, querying, updating, and deleting records.
 *
 * Collections created via `client.collection(id)` are "thin" — they hold only
 * the collection ID and client context. Accessing `name`, `metadata`, or
 * `configuration` on a thin collection will throw until the collection is
 * hydrated (either automatically by an operation that requires it, or by
 * using `client.getCollection()` instead).
 */
export interface Collection {
  /** Tenant name */
  readonly tenant: string;
  /** Database name */
  readonly database: string;
  /** Unique identifier for the collection */
  readonly id: string;
  /**
   * Name of the collection.
   * @throws ChromaValueError if the collection is not yet hydrated.
   */
  readonly name: string;
  /**
   * Collection-level metadata. Use `modify()` to update.
   * @throws ChromaValueError if the collection is not yet hydrated.
   */
  readonly metadata: CollectionMetadata | undefined;
  /**
   * Collection configuration settings. Use `modify()` to update.
   * @throws ChromaValueError if the collection is not yet hydrated.
   */
  readonly configuration: CollectionConfiguration;
  /** Optional embedding function. Must match the one used to create the collection. */
  readonly embeddingFunction?: EmbeddingFunction;
  /** Collection schema describing index configuration */
  readonly schema?: Schema;
  /**
   * Gets the total number of records in the collection.
   * @param options - Optional settings
   */
  count(options?: {
    /**
     * Controls whether to read from the write-ahead log.
     * - ReadLevel.INDEX_AND_WAL: Read from both index and WAL (default)
     * - ReadLevel.INDEX_ONLY: Read only from index, faster but recent writes may not be visible
     */
    readLevel?: ReadLevel;
  }): Promise<number>;
  /**
   * Adds new records to the collection.
   * @param args - Record data to add
   */
  add(args: {
    /** Unique identifiers for the records */
    ids: string[];
    /** Optional pre-computed embeddings */
    embeddings?: number[][];
    /** Optional metadata for each record */
    metadatas?: Metadata[];
    /** Optional document text (will be embedded if embeddings not provided) */
    documents?: string[];
    /** Optional URIs for the records */
    uris?: string[];
  }): Promise<void>;
  /**
   * Retrieves records from the collection based on filters.
   * @template TMeta - Type of metadata for type safety
   * @param args - Query parameters for filtering records
   * @returns Promise resolving to matching records
   */
  get<TMeta extends Metadata = Metadata>(args?: {
    /** Specific record IDs to retrieve */
    ids?: string[];
    /** Metadata-based filtering conditions */
    where?: Where;
    /** Maximum number of records to return */
    limit?: number;
    /** Number of records to skip */
    offset?: number;
    /** Document content-based filtering conditions */
    whereDocument?: WhereDocument;
    /** Fields to include in the response */
    include?: Include[];
  }): Promise<GetResult<TMeta>>;
  /**
   * Retrieves a preview of records from the collection.
   * @param args - Preview options
   * @returns Promise resolving to a sample of records
   */
  peek(args: { limit?: number }): Promise<GetResult>;
  /**
   * Performs similarity search on the collection.
   * @template TMeta - Type of metadata for type safety
   * @param args - Query parameters for similarity search
   * @returns Promise resolving to similar records ranked by distance
   */
  query<TMeta extends Metadata = Metadata>(args: {
    /** Pre-computed query embedding vectors */
    queryEmbeddings?: number[][];
    /** Query text to be embedded and searched */
    queryTexts?: string[];
    /** Query URIs to be processed */
    queryURIs?: string[];
    /** Filter to specific record IDs */
    ids?: string[];
    /** Maximum number of results per query (default: 10) */
    nResults?: number;
    /** Metadata-based filtering conditions */
    where?: Where;
    /** Full-text search conditions */
    whereDocument?: WhereDocument;
    /** Fields to include in the response */
    include?: Include[];
  }): Promise<QueryResult<TMeta>>;
  /**
   * Modifies collection properties like name, metadata, or configuration.
   * @param args - Properties to update
   */
  modify(args: {
    /** New name for the collection */
    name?: string;
    /** New metadata for the collection */
    metadata?: CollectionMetadata;
    /** New configuration settings */
    configuration?: UpdateCollectionConfiguration;
  }): Promise<void>;
  /**
   * Creates a copy of the collection with a new name.
   * @param args - Fork options
   * @returns Promise resolving to the new Collection instance
   */
  fork({ name }: { name: string }): Promise<Collection>;
  /**
   * Gets the number of forks for this collection.
   * @returns Promise resolving to the number of forks
   */
  forkCount(): Promise<number>;
  /**
   * Updates existing records in the collection.
   * @param args - Record data to update
   */
  update(args: {
    /** IDs of records to update */
    ids: string[];
    /** New embedding vectors */
    embeddings?: number[][];
    /** New metadata */
    metadatas?: Metadata[];
    /** New document text */
    documents?: string[];
    /** New URIs */
    uris?: string[];
  }): Promise<void>;
  /**
   * Inserts new records or updates existing ones (upsert operation).
   * @param args - Record data to upsert
   */
  upsert(args: {
    /** IDs of records to upsert */
    ids: string[];
    /** Embedding vectors */
    embeddings?: number[][];
    /** Metadata */
    metadatas?: Metadata[];
    /** Document text */
    documents?: string[];
    /** URIs */
    uris?: string[];
  }): Promise<void>;
  /**
   * Deletes records from the collection based on filters.
   * @param args - Deletion criteria
   */
  delete(args: {
    /** Specific record IDs to delete */
    ids?: string[];
    /** Metadata-based filtering for deletion */
    where?: Where;
    /** Document content-based filtering for deletion */
    whereDocument?: WhereDocument;
    /** Maximum number of records to delete. Can only be used with where or whereDocument filters. */
    limit?: number;
  }): Promise<DeleteResult>;
  /**
   * Performs hybrid search on the collection using expression builders.
   * @param searches - Single search payload or array of payloads
   * @param options
   * @returns Promise resolving to column-major search results
   */
  search(
    searches: SearchLike | SearchLike[],
    options?: {
      /**
       * Controls whether to read from the write-ahead log.
       * - ReadLevel.INDEX_AND_WAL: Read from both index and WAL (default)
       * - ReadLevel.INDEX_ONLY: Read only from index, faster but recent writes may not be visible
       */
      readLevel?: ReadLevel;
    },
  ): Promise<SearchResult>;
  /**
   * Gets the indexing status of the collection.
   * @returns Promise resolving to indexing status information
   */
  getIndexingStatus(): Promise<IndexingStatus>;
}

/**
 * Common arguments shared by all Collection construction paths.
 */
export interface CollectionBaseArgs {
  /** ChromaDB client instance */
  chromaClient: ChromaClient;
  /** HTTP API client */
  apiClient: ReturnType<typeof createClient>;
  /** Collection ID */
  id: string;
  /** Tenant name */
  tenant: string;
  /** Database name */
  database: string;
}

/**
 * Arguments for creating a fully hydrated Collection instance.
 */
export interface CollectionArgs extends CollectionBaseArgs {
  /** Collection name */
  name: string;
  /** Embedding function for the collection */
  embeddingFunction?: EmbeddingFunction;
  /** Collection configuration */
  configuration: CollectionConfiguration;
  /** Optional collection metadata */
  metadata?: CollectionMetadata;
  /** Optional schema returned by the server */
  schema?: Schema;
}

/**
 * Arguments for creating a thin (unhydrated) Collection instance.
 */
export type ThinCollectionArgs = CollectionBaseArgs;

/**
 * Abstract base class for Collection implementations.
 *
 * Contains all operation logic (add, get, query, search, etc.) shared between
 * the fully-hydrated `CollectionImpl` and the thin `ThinCollectionImpl`.
 *
 * Subclasses implement property getters (`name`, `metadata`, `configuration`)
 * and the `ensureHydrated()` hook.
 */
abstract class CollectionBase implements Collection {
  protected readonly chromaClient: ChromaClient;
  protected readonly apiClient: ReturnType<typeof createClient>;
  public readonly id: string;
  protected _tenant: string;
  protected _database: string;
  protected _name: string | undefined;
  protected _metadata: CollectionMetadata | undefined;
  protected _configuration: CollectionConfiguration | undefined;
  protected _embeddingFunction: EmbeddingFunction | undefined;
  protected _schema: Schema | undefined;

  constructor({
    chromaClient,
    apiClient,
    id,
    tenant,
    database,
  }: CollectionBaseArgs) {
    this.chromaClient = chromaClient;
    this.apiClient = apiClient;
    this.id = id;
    this._tenant = tenant;
    this._database = database;
  }

  public get tenant(): string {
    return this._tenant;
  }

  public get database(): string {
    return this._database;
  }

  abstract get name(): string;
  abstract get metadata(): CollectionMetadata | undefined;
  abstract get configuration(): CollectionConfiguration;

  public get embeddingFunction(): EmbeddingFunction | undefined {
    return this._embeddingFunction;
  }

  protected set embeddingFunction(
    embeddingFunction: EmbeddingFunction | undefined,
  ) {
    this._embeddingFunction = embeddingFunction;
  }

  public get schema(): Schema | undefined {
    return this._schema;
  }

  protected set schema(schema: Schema | undefined) {
    this._schema = schema;
  }

  /**
   * Hook for subclasses to ensure collection data is available.
   * No-op in `CollectionImpl` (always hydrated).
   * Fetches from server in `ThinCollectionImpl` (on first call).
   */
  protected async ensureHydrated(): Promise<void> {}

  protected async path(): Promise<{
    tenant: string;
    database: string;
    collection_id: string;
  }> {
    // Resolve tenant/database lazily if needed (e.g. CloudClient)
    if (!this._tenant || !this._database) {
      const resolved = await this.chromaClient._path();
      this._tenant = resolved.tenant;
      this._database = resolved.database;
    }
    return {
      tenant: this._tenant,
      database: this._database,
      collection_id: this.id,
    };
  }

  private async embed(
    inputs: string[],
    isQuery: boolean,
  ): Promise<number[][]> {
    await this.ensureHydrated();
    const embeddingFunction =
      this._embeddingFunction ?? this.getSchemaEmbeddingFunction();

    if (!embeddingFunction) {
      throw new ChromaValueError(
        `No embedding function found for collection '${this._name}'. ` +
          "You can either provide embeddings directly, or ensure the appropriate " +
          "embedding function package (e.g. @chroma-core/default-embed) is installed.",
      );
    }

    if (isQuery && embeddingFunction.generateForQueries) {
      return await embeddingFunction.generateForQueries(inputs);
    }

    return await embeddingFunction.generate(inputs);
  }

  private async sparseEmbed(
    sparseEmbeddingFunction: SparseEmbeddingFunction,
    inputs: string[],
    isQuery: boolean,
  ): Promise<SparseVector[]> {
    if (isQuery && sparseEmbeddingFunction.generateForQueries) {
      return await sparseEmbeddingFunction.generateForQueries(inputs);
    }

    return await sparseEmbeddingFunction.generate(inputs);
  }

  private getSparseEmbeddingTargets(): Record<
    string,
    SparseVectorIndexConfig
  > {
    const schema = this._schema;
    if (!schema) return {};

    const targets: Record<string, SparseVectorIndexConfig> = {};
    for (const [key, valueTypes] of Object.entries(schema.keys)) {
      const sparseVector = valueTypes.sparseVector;
      const sparseIndex = sparseVector?.sparseVectorIndex;
      if (!sparseIndex?.enabled) continue;

      const config = sparseIndex.config;
      if (!config.embeddingFunction || !config.sourceKey) continue;

      targets[key] = config;
    }

    return targets;
  }

  private async applySparseEmbeddingsToMetadatas(
    metadatas?: Metadata[],
    documents?: string[],
  ): Promise<Metadata[] | undefined> {
    // If documents are provided, we may need the schema for sparse embedding
    // targets. Ensure hydrated so schema is available (no-op if already hydrated).
    if (documents) {
      await this.ensureHydrated();
    }
    const sparseTargets = this.getSparseEmbeddingTargets();
    if (Object.keys(sparseTargets).length === 0) {
      return metadatas;
    }

    // If no metadatas provided, create empty objects based on documents length
    if (!metadatas) {
      if (!documents) {
        return undefined;
      }
      metadatas = Array(documents.length)
        .fill(null)
        .map(() => ({}));
    }

    // Create copies, converting null to empty object
    const updatedMetadatas = metadatas.map((metadata) =>
      metadata !== null && metadata !== undefined ? { ...metadata } : {},
    );
    const documentsList = documents ? [...documents] : undefined;

    for (const [targetKey, config] of Object.entries(sparseTargets)) {
      const sourceKey = config.sourceKey;
      const embeddingFunction = config.embeddingFunction;
      if (!sourceKey || !embeddingFunction) {
        continue;
      }

      const inputs: string[] = [];
      const positions: number[] = [];

      // Handle special case: source_key is "#document"
      if (sourceKey === DOCUMENT_KEY) {
        if (!documentsList) {
          continue;
        }

        // Collect documents that need embedding
        updatedMetadatas.forEach((metadata, index) => {
          // Skip if target already exists in metadata
          if (targetKey in metadata) {
            return;
          }

          // Get document at this position
          if (index < documentsList.length) {
            const doc = documentsList[index];
            inputs.push(doc);
            positions.push(index);
          }
        });

        // Generate embeddings for all collected documents
        if (inputs.length === 0) {
          continue;
        }

        const sparseEmbeddings = await this.sparseEmbed(
          embeddingFunction,
          inputs,
          false,
        );
        if (sparseEmbeddings.length !== positions.length) {
          throw new ChromaValueError(
            "Sparse embedding function returned unexpected number of embeddings.",
          );
        }

        positions.forEach((position, idx) => {
          updatedMetadatas[position][targetKey] = sparseEmbeddings[idx];
        });

        continue; // Skip the metadata-based logic below
      }

      // Handle normal case: source_key is a metadata field
      updatedMetadatas.forEach((metadata, index) => {
        if (targetKey in metadata) {
          return;
        }

        const sourceValue = metadata[sourceKey];
        if (typeof sourceValue !== "string") {
          return;
        }

        inputs.push(sourceValue);
        positions.push(index);
      });

      if (inputs.length === 0) {
        continue;
      }

      const sparseEmbeddings = await this.sparseEmbed(
        embeddingFunction,
        inputs,
        false,
      );
      if (sparseEmbeddings.length !== positions.length) {
        throw new ChromaValueError(
          "Sparse embedding function returned unexpected number of embeddings.",
        );
      }

      positions.forEach((position, idx) => {
        updatedMetadatas[position][targetKey] = sparseEmbeddings[idx];
      });
    }

    // Convert empty objects back to null
    const resultMetadatas = updatedMetadatas.map((metadata) =>
      Object.keys(metadata).length === 0 ? null : metadata,
    );

    return resultMetadatas as Metadata[];
  }

  private async embedKnnLiteral(
    knn: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const queryValue = knn.query as unknown;
    if (typeof queryValue !== "string") {
      return { ...knn };
    }

    // String query needs embedding function — ensure we're hydrated
    await this.ensureHydrated();

    const keyValue = knn.key as unknown;
    const key = typeof keyValue === "string" ? keyValue : EMBEDDING_KEY;

    if (key === EMBEDDING_KEY) {
      const embeddings = await this.embed([queryValue], true);
      if (!embeddings || embeddings.length !== 1) {
        throw new ChromaValueError(
          "Embedding function returned unexpected number of embeddings.",
        );
      }
      return { ...knn, query: embeddings[0] };
    }

    const schema = this._schema;
    if (!schema) {
      throw new ChromaValueError(
        `Cannot embed string query for key '${key}': schema is not available. Provide an embedded vector or configure an embedding function.`,
      );
    }

    const valueTypes = schema.keys[key];
    if (!valueTypes) {
      throw new ChromaValueError(
        `Cannot embed string query for key '${key}': key not found in schema. Provide an embedded vector or configure an embedding function.`,
      );
    }

    const sparseIndex = valueTypes.sparseVector?.sparseVectorIndex;
    if (sparseIndex?.enabled && sparseIndex.config.embeddingFunction) {
      const sparseEmbeddingFunction = sparseIndex.config.embeddingFunction;
      const sparseEmbeddings = await this.sparseEmbed(
        sparseEmbeddingFunction,
        [queryValue],
        true,
      );
      if (!sparseEmbeddings || sparseEmbeddings.length !== 1) {
        throw new ChromaValueError(
          "Sparse embedding function returned unexpected number of embeddings.",
        );
      }
      return { ...knn, query: sparseEmbeddings[0] };
    }

    const vectorIndex = valueTypes.floatList?.vectorIndex;
    if (vectorIndex?.enabled && vectorIndex.config.embeddingFunction) {
      const embeddingFunction = vectorIndex.config.embeddingFunction;
      const embeddings = embeddingFunction.generateForQueries
        ? await embeddingFunction.generateForQueries([queryValue])
        : await embeddingFunction.generate([queryValue]);

      if (!embeddings || embeddings.length !== 1) {
        throw new ChromaValueError(
          "Embedding function returned unexpected number of embeddings.",
        );
      }

      return { ...knn, query: embeddings[0] };
    }

    throw new ChromaValueError(
      `Cannot embed string query for key '${key}': no embedding function configured. Provide an embedded vector or configure an embedding function.`,
    );
  }

  private async embedRankLiteral(rank: unknown): Promise<unknown> {
    if (rank === null || rank === undefined) {
      return rank;
    }

    if (Array.isArray(rank)) {
      return Promise.all(rank.map((item) => this.embedRankLiteral(item)));
    }

    if (!isPlainObject(rank)) {
      return rank;
    }

    const entries = await Promise.all(
      Object.entries(rank).map(async ([key, value]) => {
        if (key === "$knn" && isPlainObject(value)) {
          return [key, await this.embedKnnLiteral(value)];
        }
        return [key, await this.embedRankLiteral(value)];
      }),
    );

    return Object.fromEntries(entries);
  }

  private async embedSearchPayload(
    payload: SearchPayload,
  ): Promise<SearchPayload> {
    if (!payload.rank) {
      return payload;
    }

    const embeddedRank = await this.embedRankLiteral(payload.rank);
    if (!isPlainObject(embeddedRank)) {
      return payload;
    }

    return {
      ...payload,
      rank: embeddedRank as SearchPayload["rank"],
    };
  }

  protected getSchemaEmbeddingFunction(): EmbeddingFunction | undefined {
    const schema = this._schema;
    if (!schema) return undefined;

    const schemaOverride = schema.keys[EMBEDDING_KEY];
    const overrideFunction =
      schemaOverride?.floatList?.vectorIndex?.config.embeddingFunction;
    if (overrideFunction) {
      return overrideFunction;
    }

    const defaultFunction =
      schema.defaults.floatList?.vectorIndex?.config.embeddingFunction;
    return defaultFunction ?? undefined;
  }

  private async prepareRecords<T extends boolean = false>({
    recordSet,
    update = false as T,
  }: {
    recordSet: RecordSet;
    update?: T;
  }): Promise<T extends true ? PreparedRecordSet : PreparedInsertRecordSet> {
    const maxBatchSize = await this.chromaClient.getMaxBatchSize();

    validateRecordSetLengthConsistency(recordSet);
    validateIDs(recordSet.ids);
    validateBaseRecordSet({ recordSet, update });
    validateMaxBatchSize(recordSet.ids.length, maxBatchSize);

    if (!recordSet.embeddings && recordSet.documents) {
      recordSet.embeddings = await this.embed(recordSet.documents, false);
    }

    const metadatasWithSparse = await this.applySparseEmbeddingsToMetadatas(
      recordSet.metadatas,
      recordSet.documents,
    );

    const preparedRecordSet: PreparedRecordSet = {
      ...recordSet,
      metadatas: metadatasWithSparse,
    };

    const base64Supported = await this.chromaClient.supportsBase64Encoding();
    if (base64Supported && recordSet.embeddings) {
      preparedRecordSet.embeddings = embeddingsToBase64Bytes(
        recordSet.embeddings,
      );
    }

    return preparedRecordSet as T extends true
      ? PreparedRecordSet
      : PreparedInsertRecordSet;
  }

  private validateGet(
    include: Include[],
    ids?: string[],
    where?: Where,
    whereDocument?: WhereDocument,
  ) {
    validateInclude({ include, exclude: ["distances"] });
    if (ids) validateIDs(ids);
    if (where) validateWhere(where);
    if (whereDocument) validateWhereDocument(whereDocument);
  }

  private async prepareQuery(
    recordSet: BaseRecordSet,
    include: Include[],
    ids?: string[],
    where?: Where,
    whereDocument?: WhereDocument,
    nResults?: number,
  ): Promise<QueryRecordSet> {
    validateBaseRecordSet({
      recordSet,
      embeddingsField: "queryEmbeddings",
      documentsField: "queryTexts",
    });
    validateInclude({ include });

    if (ids) validateIDs(ids);
    if (where) validateWhere(where);
    if (whereDocument) validateWhereDocument(whereDocument);
    if (nResults) validateNResults(nResults);

    let embeddings: number[][];
    if (!recordSet.embeddings) {
      embeddings = await this.embed(recordSet.documents!, true);
    } else {
      embeddings = recordSet.embeddings;
    }

    return {
      ...recordSet,
      ids,
      embeddings,
    };
  }

  private validateDelete(
    ids?: string[],
    where?: Where,
    whereDocument?: WhereDocument,
    limit?: number,
  ) {
    if (ids) validateIDs(ids);
    if (where) validateWhere(where);
    if (whereDocument) validateWhereDocument(whereDocument);
    if (limit !== undefined && (!Number.isInteger(limit) || limit < 0)) {
      throw new Error("limit must be a non-negative integer");
    }
    if (limit !== undefined && !where && !whereDocument) {
      throw new Error(
        "limit can only be specified when a where or whereDocument clause is provided",
      );
    }
  }

  public async count(options?: { readLevel?: ReadLevel }): Promise<number> {
    const { data } = await RecordService.collectionCount({
      client: this.apiClient,
      path: await this.path(),
      query: options?.readLevel ? { read_level: options.readLevel } : undefined,
    });

    return data;
  }

  public async add({
    ids,
    embeddings,
    metadatas,
    documents,
    uris,
  }: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }) {
    const recordSet: RecordSet = {
      ids,
      embeddings,
      documents,
      metadatas,
      uris,
    };

    const preparedRecordSet = await this.prepareRecords({ recordSet });

    await RecordService.collectionAdd({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        documents: preparedRecordSet.documents,
        metadatas: serializeMetadatas(preparedRecordSet.metadatas),
        uris: preparedRecordSet.uris,
      },
    });
  }

  public async get<TMeta extends Metadata = Metadata>(
    args: Partial<{
      ids?: string[];
      where?: Where;
      limit?: number;
      offset?: number;
      whereDocument?: WhereDocument;
      include?: Include[];
    }> = {},
  ): Promise<GetResult<TMeta>> {
    const {
      ids,
      where,
      limit,
      offset,
      whereDocument,
      include = ["documents", "metadatas"],
    } = args;

    this.validateGet(include, ids, where, whereDocument);

    const { data } = await RecordService.collectionGet({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids,
        where,
        limit,
        offset,
        where_document: whereDocument,
        include,
      },
    });

    const deserializedMetadatas = deserializeMetadatas(data.metadatas) ?? [];

    return new GetResult<TMeta>({
      documents: data.documents ?? [],
      embeddings: data.embeddings ?? [],
      ids: data.ids,
      include: data.include,
      metadatas: deserializedMetadatas as (TMeta | null)[],
      uris: data.uris ?? [],
    });
  }

  public async peek({ limit = 10 }: { limit?: number }): Promise<GetResult> {
    return this.get({ limit });
  }

  public async query<TMeta extends Metadata = Metadata>({
    queryEmbeddings,
    queryTexts,
    queryURIs,
    ids,
    nResults = 10,
    where,
    whereDocument,
    include = ["metadatas", "documents", "distances"],
  }: {
    queryEmbeddings?: number[][];
    queryTexts?: string[];
    queryURIs?: string[];
    ids?: string[];
    nResults?: number;
    where?: Where;
    whereDocument?: WhereDocument;
    include?: Include[];
  }): Promise<QueryResult<TMeta>> {
    const recordSet: BaseRecordSet = {
      embeddings: queryEmbeddings,
      documents: queryTexts,
      uris: queryURIs,
    };

    const queryRecordSet = await this.prepareQuery(
      recordSet,
      include,
      ids,
      where,
      whereDocument,
      nResults,
    );

    const { data } = await RecordService.collectionQuery({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: queryRecordSet.ids,
        include,
        n_results: nResults,
        query_embeddings: queryRecordSet.embeddings,
        where,
        where_document: whereDocument,
      },
    });

    const deserializedMetadatas =
      deserializeMetadataMatrix(data.metadatas) ?? [];

    return new QueryResult({
      distances: data.distances ?? [],
      documents: data.documents ?? [],
      embeddings: data.embeddings ?? [],
      ids: data.ids ?? [],
      include: data.include,
      metadatas: deserializedMetadatas as (TMeta | null)[][],
      uris: data.uris ?? [],
    });
  }

  public async search(
    searches: SearchLike | SearchLike[],
    options?: {
      readLevel?: ReadLevel;
    },
  ): Promise<SearchResult> {
    const items = Array.isArray(searches) ? searches : [searches];

    if (items.length === 0) {
      throw new ChromaValueError(
        "At least one search payload must be provided.",
      );
    }

    const payloads = await Promise.all(
      items.map(async (search) => {
        const payload = toSearch(search).toPayload();
        return this.embedSearchPayload(payload);
      }),
    );

    const { data } = await RecordService.collectionSearch({
      client: this.apiClient,
      path: await this.path(),
      body: {
        searches: payloads,
        read_level: options?.readLevel,
      },
    });

    return new SearchResult(data);
  }

  public async modify({
    name,
    metadata,
    configuration,
  }: {
    name?: string;
    metadata?: CollectionMetadata;
    configuration?: UpdateCollectionConfiguration;
  }): Promise<void> {
    await this.ensureHydrated();
    if (name) this._name = name;

    if (metadata) {
      validateMetadata(metadata);
      this._metadata = metadata;
    }

    const { updateConfiguration, updateEmbeddingFunction } = configuration
      ? await processUpdateCollectionConfig({
          collectionName: this.name,
          currentConfiguration: this.configuration,
          newConfiguration: configuration,
          currentEmbeddingFunction: this.embeddingFunction,
          client: this.chromaClient,
        })
      : {};

    if (updateEmbeddingFunction) {
      this._embeddingFunction = updateEmbeddingFunction;
    }

    if (updateConfiguration) {
      this._configuration = {
        hnsw: { ...this.configuration.hnsw, ...updateConfiguration.hnsw },
        spann: { ...this.configuration.spann, ...updateConfiguration.spann },
        embeddingFunction: updateConfiguration.embedding_function,
      };
    }

    await CollectionService.updateCollection({
      client: this.apiClient,
      path: await this.path(),
      body: {
        new_name: name,
        new_metadata: serializeMetadata(metadata),
        new_configuration: updateConfiguration,
      },
    });
  }

  public async fork({ name }: { name: string }): Promise<Collection> {
    await this.ensureHydrated();
    const { data } = await CollectionService.forkCollection({
      client: this.apiClient,
      path: await this.path(),
      body: { new_name: name },
    });

    return new CollectionImpl({
      chromaClient: this.chromaClient,
      apiClient: this.apiClient,
      name: data.name,
      tenant: this.tenant,
      database: this.database,
      id: data.id,
      embeddingFunction: this._embeddingFunction,
      metadata: deserializeMetadata(data.metadata ?? undefined) ?? undefined,
      configuration: data.configuration_json,
    });
  }

  public async forkCount(): Promise<number> {
    const { data } = await CollectionService.forkCount({
      client: this.apiClient,
      path: await this.path(),
    });

    return data.count;
  }

  public async update({
    ids,
    embeddings,
    metadatas,
    documents,
    uris,
  }: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }): Promise<void> {
    const recordSet: RecordSet = {
      ids,
      embeddings,
      documents,
      metadatas,
      uris,
    };

    const preparedRecordSet = await this.prepareRecords({
      recordSet,
      update: true,
    });

    await RecordService.collectionUpdate({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        metadatas: serializeMetadatas(preparedRecordSet.metadatas),
        uris: preparedRecordSet.uris,
        documents: preparedRecordSet.documents,
      },
    });
  }

  public async upsert({
    ids,
    embeddings,
    metadatas,
    documents,
    uris,
  }: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }): Promise<void> {
    const recordSet: RecordSet = {
      ids,
      embeddings,
      documents,
      metadatas,
      uris,
    };

    const preparedRecordSet = await this.prepareRecords({
      recordSet,
    });

    await RecordService.collectionUpsert({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        metadatas: serializeMetadatas(preparedRecordSet.metadatas),
        uris: preparedRecordSet.uris,
        documents: preparedRecordSet.documents,
      },
    });
  }

  public async delete({
    ids,
    where,
    whereDocument,
    limit,
  }: {
    ids?: string[];
    where?: Where;
    whereDocument?: WhereDocument;
    limit?: number;
  }): Promise<DeleteResult> {
    this.validateDelete(ids, where, whereDocument, limit);

    const { data } = await RecordService.collectionDelete({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids,
        where,
        where_document: whereDocument,
        limit,
      },
    });

    return { deleted: data?.deleted ?? 0 };
  }

  public async getIndexingStatus(): Promise<IndexingStatus> {
    const { data } = await RecordService.indexingStatus({
      client: this.apiClient,
      path: await this.path(),
    });

    return data;
  }
}

/**
 * Fully hydrated Collection implementation.
 *
 * Created by `getCollection`, `createCollection`, `getOrCreateCollection`, etc.
 * All properties are immediately available.
 */
export class CollectionImpl extends CollectionBase {
  constructor(args: CollectionArgs) {
    super(args);
    this._name = args.name;
    this._metadata = args.metadata;
    this._configuration = args.configuration;
    this._embeddingFunction = args.embeddingFunction;
    this._schema = args.schema;
  }

  public get name(): string {
    return this._name!;
  }

  public get metadata(): CollectionMetadata | undefined {
    return this._metadata;
  }

  public get configuration(): CollectionConfiguration {
    return this._configuration!;
  }
}

/**
 * Thin (unhydrated) Collection implementation.
 *
 * Created by `client.collection(id)`. Holds only the collection ID and client
 * context. Operations that don't require the collection's schema or embedding
 * function (e.g. `add` with pre-computed embeddings, `get`, `delete`, `count`)
 * work immediately. Operations that need the schema trigger automatic hydration
 * via a single `getCollection` call.
 *
 * Accessing `name`, `metadata`, or `configuration` before hydration throws
 * a `ChromaValueError`.
 */
export class ThinCollectionImpl extends CollectionBase {
  private _hydrated = false;
  private _hydrationPromise: Promise<void> | null = null;

  constructor(args: ThinCollectionArgs) {
    super(args);
  }

  public get name(): string {
    if (!this._hydrated) {
      throw new ChromaValueError(
        "Cannot access 'name' on a thin collection created via client.collection(id). " +
          "Use client.getCollection() instead, or perform an operation that triggers automatic hydration.",
      );
    }
    return this._name!;
  }

  public get metadata(): CollectionMetadata | undefined {
    if (!this._hydrated) {
      throw new ChromaValueError(
        "Cannot access 'metadata' on a thin collection created via client.collection(id). " +
          "Use client.getCollection() instead, or perform an operation that triggers automatic hydration.",
      );
    }
    return this._metadata;
  }

  public get configuration(): CollectionConfiguration {
    if (!this._hydrated) {
      throw new ChromaValueError(
        "Cannot access 'configuration' on a thin collection created via client.collection(id). " +
          "Use client.getCollection() instead, or perform an operation that triggers automatic hydration.",
      );
    }
    return this._configuration!;
  }

  protected override async ensureHydrated(): Promise<void> {
    if (this._hydrated) return;

    if (this._hydrationPromise) {
      return this._hydrationPromise;
    }

    this._hydrationPromise = this.doHydrate();
    try {
      await this._hydrationPromise;
    } finally {
      this._hydrationPromise = null;
    }
  }

  private async doHydrate(): Promise<void> {
    const path = await this.path();

    const { data } = await CollectionService.getCollection({
      client: this.apiClient,
      path,
    });

    const schema = await Schema.deserializeFromJSON(
      data.schema ?? null,
      this.chromaClient,
    );

    this._schema = schema;
    const schemaEmbeddingFunction = this.getSchemaEmbeddingFunction();
    const resolvedEmbeddingFunction =
      (await getEmbeddingFunction({
        client: this.chromaClient,
        efConfig: data.configuration_json.embedding_function ?? undefined,
      })) ?? schemaEmbeddingFunction;

    this._name = data.name;
    this._metadata =
      deserializeMetadata(data.metadata ?? undefined) ?? undefined;
    this._configuration = data.configuration_json;
    this._embeddingFunction = resolvedEmbeddingFunction;
    this._hydrated = true;
  }
}
