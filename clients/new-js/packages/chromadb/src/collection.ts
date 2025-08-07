import { ChromaClient } from "./chroma-client";
import { EmbeddingFunction } from "./embedding-function";
import {
  BaseRecordSet,
  CollectionMetadata,
  GetResult,
  Metadata,
  PreparedRecordSet,
  PreparedInsertRecordSet,
  QueryRecordSet,
  QueryResult,
  RecordSet,
  Where,
  WhereDocument,
} from "./types";
import { Include } from "./api";
import { DefaultService as Api } from "./api";
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
} from "./utils";
import { createClient } from "@hey-api/client-fetch";
import { ChromaValueError } from "./errors";
import {
  CollectionConfiguration,
  processUpdateCollectionConfig,
  UpdateCollectionConfiguration,
} from "./collection-configuration";

/**
 * Interface for collection operations using collection ID.
 * Provides methods for adding, querying, updating, and deleting records.
 */
export interface Collection {
  /** Unique identifier for the collection */
  id: string;
  /** Name of the collection */
  name: string;
  /** Collection-level metadata */
  metadata: CollectionMetadata | undefined;
  /** Collection configuration settings */
  configuration: CollectionConfiguration;
  /** Optional embedding function. Must match the one used to create the collection. */
  embeddingFunction?: EmbeddingFunction;
  /** Gets the total number of records in the collection */
  count(): Promise<number>;
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
  }): Promise<void>;
}

/**
 * Arguments for creating a Collection instance.
 */
export interface CollectionArgs {
  /** ChromaDB client instance */
  chromaClient: ChromaClient;
  /** HTTP API client */
  apiClient: ReturnType<typeof createClient>;
  /** Collection name */
  name: string;
  /** Collection ID */
  id: string;
  /** Embedding function for the collection */
  embeddingFunction?: EmbeddingFunction;
  /** Collection configuration */
  configuration: CollectionConfiguration;
  /** Optional collection metadata */
  metadata?: CollectionMetadata;
}

/**
 * Implementation of CollectionAPI for ID-based collection operations.
 * Provides core functionality for interacting with collections using their ID.
 */
export class CollectionImpl implements Collection {
  protected readonly chromaClient: ChromaClient;
  protected readonly apiClient: ReturnType<typeof createClient>;
  public readonly id: string;
  private _name: string;
  private _metadata: CollectionMetadata | undefined;
  private _configuration: CollectionConfiguration;
  protected _embeddingFunction: EmbeddingFunction | undefined;

  /**
   * Creates a new CollectionAPIImpl instance.
   * @param options - Configuration for the collection API
   */
  constructor({
    chromaClient,
    apiClient,
    id,
    name,
    metadata,
    configuration,
    embeddingFunction,
  }: CollectionArgs) {
    this.chromaClient = chromaClient;
    this.apiClient = apiClient;
    this.id = id;
    this._name = name;
    this._metadata = metadata;
    this._configuration = configuration;
    this._embeddingFunction = embeddingFunction;
  }

  public get name(): string {
    return this._name;
  }

  private set name(name: string) {
    this._name = name;
  }

  public get configuration(): CollectionConfiguration {
    return this._configuration;
  }

  private set configuration(configuration: CollectionConfiguration) {
    this._configuration = configuration;
  }

  public get metadata(): CollectionMetadata | undefined {
    return this._metadata;
  }

  private set metadata(metadata: CollectionMetadata | undefined) {
    this._metadata = metadata;
  }

  public get embeddingFunction(): EmbeddingFunction | undefined {
    return this._embeddingFunction;
  }

  protected set embeddingFunction(
    embeddingFunction: EmbeddingFunction | undefined,
  ) {
    this._embeddingFunction = embeddingFunction;
  }

  protected async path(): Promise<{
    tenant: string;
    database: string;
    collection_id: string;
  }> {
    const clientPath = await this.chromaClient._path();
    return {
      ...clientPath,
      collection_id: this.id,
    };
  }

  private async embed(documents: string[]): Promise<number[][]> {
    if (!this._embeddingFunction) {
      throw new ChromaValueError(
        "Embedding function must be defined for operations requiring embeddings.",
      );
    }

    return await this._embeddingFunction.generate(documents);
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
      recordSet.embeddings = await this.embed(recordSet.documents);
    }

    const preparedRecordSet: PreparedRecordSet = { ...recordSet };

    const base64Supported = await this.chromaClient.supportsBase64Encoding();
    if (base64Supported && recordSet.embeddings) {
      preparedRecordSet.embeddings = embeddingsToBase64Bytes(
        recordSet.embeddings,
      );
    }

    return preparedRecordSet as T extends true ? PreparedRecordSet : PreparedInsertRecordSet;
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
      embeddings = await this.embed(recordSet.documents!);
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
  ) {
    if (ids) validateIDs(ids);
    if (where) validateWhere(where);
    if (whereDocument) validateWhereDocument(whereDocument);
  }

  public async count(): Promise<number> {
    const { data } = await Api.collectionCount({
      client: this.apiClient,
      path: await this.path(),
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

    await Api.collectionAdd({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        documents: preparedRecordSet.documents,
        metadatas: preparedRecordSet.metadatas,
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

    const { data } = await Api.collectionGet({
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

    return new GetResult<TMeta>({
      documents: data.documents ?? [],
      embeddings: data.embeddings ?? [],
      ids: data.ids,
      include: data.include,
      metadatas: (data.metadatas ?? []) as (TMeta | null)[],
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

    const { data } = await Api.collectionQuery({
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

    return new QueryResult({
      distances: data.distances ?? [],
      documents: data.documents ?? [],
      embeddings: data.embeddings ?? [],
      ids: data.ids ?? [],
      include: data.include,
      metadatas: (data.metadatas ?? []) as (TMeta | null)[][],
      uris: data.uris ?? [],
    });
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
    if (name) this.name = name;

    if (metadata) {
      validateMetadata(metadata);
      this.metadata = metadata;
    }

    const { updateConfiguration, updateEmbeddingFunction } = configuration
      ? await processUpdateCollectionConfig({
          collectionName: this.name,
          currentConfiguration: this.configuration,
          newConfiguration: configuration,
          currentEmbeddingFunction: this.embeddingFunction,
        })
      : {};

    if (updateEmbeddingFunction) {
      this.embeddingFunction = updateEmbeddingFunction;
    }

    if (updateConfiguration) {
      this.configuration = {
        hnsw: { ...this.configuration.hnsw, ...updateConfiguration.hnsw },
        spann: { ...this.configuration.spann, ...updateConfiguration.spann },
        embeddingFunction: updateConfiguration.embedding_function,
      };
    }

    await Api.updateCollection({
      client: this.apiClient,
      path: await this.path(),
      body: {
        new_name: name,
        new_metadata: metadata,
        new_configuration: updateConfiguration,
      },
    });
  }

  public async fork({ name }: { name: string }): Promise<Collection> {
    const { data } = await Api.forkCollection({
      client: this.apiClient,
      path: await this.path(),
      body: { new_name: name },
    });

    return new CollectionImpl({
      chromaClient: this.chromaClient,
      apiClient: this.apiClient,
      name: data.name,
      id: data.id,
      embeddingFunction: this._embeddingFunction,
      metadata: data.metadata ?? undefined,
      configuration: data.configuration_json,
    });
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

    await Api.collectionUpdate({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        metadatas: preparedRecordSet.metadatas,
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

    await Api.collectionUpsert({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: preparedRecordSet.ids,
        embeddings: preparedRecordSet.embeddings,
        metadatas: preparedRecordSet.metadatas,
        uris: preparedRecordSet.uris,
        documents: preparedRecordSet.documents,
      },
    });
  }

  public async delete({
    ids,
    where,
    whereDocument,
  }: {
    ids?: string[];
    where?: Where;
    whereDocument?: WhereDocument;
  }): Promise<void> {
    this.validateDelete(ids, where, whereDocument);

    await Api.collectionDelete({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids,
        where,
        where_document: whereDocument,
      },
    });
  }
}
