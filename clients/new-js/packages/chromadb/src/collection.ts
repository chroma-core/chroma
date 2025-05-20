import { ChromaClient } from "./chroma-client";
import {
  EmbeddingFunction,
  getDefaultEFConfig,
  getEmbeddingFunction,
  knownEmbeddingFunctions,
} from "./embedding-function";
import {
  BaseRecordSet,
  CollectionMetadata,
  GetResult,
  Metadata,
  QueryRecordSet,
  QueryResult,
  RecordSet,
  Where,
  WhereDocument,
} from "./types";
import {
  CollectionConfiguration,
  Include,
  UpdateCollectionConfiguration,
} from "./api";
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
} from "./utils";
import { createClient } from "@hey-api/client-fetch";
import { ChromaValueError } from "./errors";

export interface CollectionAPI {
  id: string;
  embeddingFunction?: EmbeddingFunction;
  count(): Promise<number>;
  add(args: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }): Promise<void>;
  get<TMeta extends Metadata = Metadata>(args?: {
    ids?: string[];
    where?: Where;
    limit?: number;
    offset?: number;
    whereDocument?: WhereDocument;
    include?: Include[];
  }): Promise<GetResult<TMeta>>;
  peek(args: { limit?: number }): Promise<GetResult>;
  query<TMeta extends Metadata = Metadata>(args: {
    queryEmbeddings?: number[][];
    queryTexts?: string[];
    queryURIs?: string[];
    ids?: string[];
    nResults?: number;
    where?: Where;
    whereDocument?: WhereDocument;
    include?: Include[];
  }): Promise<QueryResult<TMeta>>;
  modify(args: {
    name?: string;
    metadata?: CollectionMetadata;
    configuration?: UpdateCollectionConfiguration;
  }): Promise<void>;
  fork({ name }: { name: string }): Promise<Collection>;
  update(args: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }): Promise<void>;
  upsert(args: {
    ids: string[];
    embeddings?: number[][];
    metadatas?: Metadata[];
    documents?: string[];
    uris?: string[];
  }): Promise<void>;
  delete(args: {
    ids?: string[];
    where?: Where;
    whereDocument?: WhereDocument;
  }): Promise<void>;
}

export interface Collection extends CollectionAPI {
  name: string;
  embeddingFunction: EmbeddingFunction;
  metadata: CollectionMetadata | undefined;
  configuration: CollectionConfiguration;
}

export interface CollectionArgs {
  chromaClient: ChromaClient;
  apiClient: ReturnType<typeof createClient>;
  name: string;
  id: string;
  embeddingFunction: EmbeddingFunction;
  configuration: CollectionConfiguration;
  metadata?: CollectionMetadata;
}

export class CollectionAPIImpl implements CollectionAPI {
  protected readonly chromaClient: ChromaClient;
  protected readonly apiClient: ReturnType<typeof createClient>;
  public readonly id: string;
  protected _embeddingFunction: EmbeddingFunction | undefined;

  constructor({
    chromaClient,
    apiClient,
    id,
    embeddingFunction,
  }: {
    chromaClient: ChromaClient;
    apiClient: ReturnType<typeof createClient>;
    id: string;
    embeddingFunction?: EmbeddingFunction;
  }) {
    this.chromaClient = chromaClient;
    this.apiClient = apiClient;
    this.id = id;
    this._embeddingFunction = embeddingFunction;
  }

  public get embeddingFunction(): EmbeddingFunction | undefined {
    return this._embeddingFunction;
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

    if (
      this._embeddingFunction &&
      this._embeddingFunction.name === "default" &&
      !knownEmbeddingFunctions.has("default")
    ) {
      await getDefaultEFConfig();
    }

    return await this._embeddingFunction.generate(documents);
  }

  private async prepareRecords({
    recordSet,
    update = false,
  }: {
    recordSet: RecordSet;
    update?: boolean;
  }) {
    validateRecordSetLengthConsistency(recordSet);
    validateIDs(recordSet.ids);
    validateBaseRecordSet({ recordSet, update });

    if (!recordSet.embeddings && recordSet.documents) {
      recordSet.embeddings = await this.embed(recordSet.documents);
    }
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

    await this.prepareRecords({ recordSet });

    await Api.collectionAdd({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: recordSet.ids,
        embeddings: recordSet.embeddings,
        documents: recordSet.documents,
        metadatas: recordSet.metadatas,
        uris: recordSet.uris,
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
    if (metadata) {
      validateMetadata(metadata);
    }

    await Api.updateCollection({
      client: this.apiClient,
      path: await this.path(),
      body: {
        new_name: name,
        new_metadata: metadata,
        new_configuration: configuration,
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
      id: data.name,
      embeddingFunction: this._embeddingFunction
        ? this._embeddingFunction
        : await getEmbeddingFunction(
            data.name,
            data.configuration_json.embedding_function ?? undefined,
          ),
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

    await this.prepareRecords({ recordSet, update: true });

    await Api.collectionUpdate({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: recordSet.ids,
        embeddings: recordSet.embeddings,
        metadatas: recordSet.metadatas,
        uris: recordSet.uris,
        documents: recordSet.documents,
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

    await this.prepareRecords({ recordSet, update: true });

    await Api.collectionUpsert({
      client: this.apiClient,
      path: await this.path(),
      body: {
        ids: recordSet.ids,
        embeddings: recordSet.embeddings,
        metadatas: recordSet.metadatas,
        uris: recordSet.uris,
        documents: recordSet.documents,
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

export class CollectionImpl extends CollectionAPIImpl implements Collection {
  private _name: string;
  override _embeddingFunction: EmbeddingFunction;
  private _metadata: CollectionMetadata | undefined;
  private _configuration: CollectionConfiguration;

  constructor({
    chromaClient,
    apiClient,
    name,
    id,
    embeddingFunction,
    metadata,
    configuration,
  }: CollectionArgs) {
    super({ chromaClient, apiClient, id });
    this._name = name;
    this._embeddingFunction = embeddingFunction;
    this._metadata = metadata;
    this._configuration = configuration;
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

  override get embeddingFunction(): EmbeddingFunction {
    return this._embeddingFunction;
  }

  private set embeddingFunction(embeddingFunction: EmbeddingFunction) {
    this._embeddingFunction = embeddingFunction;
  }

  override async modify({
    name,
    metadata,
    configuration,
  }: {
    name?: string;
    metadata?: CollectionMetadata;
    configuration?: UpdateCollectionConfiguration;
  }): Promise<void> {
    if (name) this.name = name;
    if (configuration) this.configuration = configuration;
    if (metadata) {
      validateMetadata(metadata);
      this.metadata = metadata;
    }

    await super.modify({ name, metadata, configuration });
  }

  override async fork({ name }: { name: string }): Promise<Collection> {
    const { data } = await Api.forkCollection({
      client: this.apiClient,
      path: await this.path(),
      body: { new_name: name },
    });

    return new CollectionImpl({
      chromaClient: this.chromaClient,
      apiClient: this.apiClient,
      name: data.name,
      id: data.name,
      embeddingFunction: this._embeddingFunction,
      metadata: data.metadata ?? undefined,
      configuration: data.configuration_json,
    });
  }
}
