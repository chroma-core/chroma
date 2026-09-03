import { PersistentClient } from "./PersistentClient";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";
import {
  CollectionMetadata,
  AddRecordsParams,
  UpsertRecordsParams,
  BaseGetParams,
  GetResponse,
  UpdateRecordsParams,
  QueryRecordsParams,
  MultiQueryResponse,
  PeekParams,
  MultiGetResponse,
  DeleteParams,
  Embeddings,
  IncludeEnum,
  Metadata,
} from "./types";
import { toArray, toArrayOfArrays } from "./utils";

export class PersistentCollection {
  public name: string;
  public id: string;
  public metadata: CollectionMetadata | undefined;
  private client: PersistentClient;
  public embeddingFunction: IEmbeddingFunction;

  constructor(
    name: string,
    id: string,
    client: PersistentClient,
    embeddingFunction: IEmbeddingFunction,
    metadata?: CollectionMetadata,
  ) {
    this.name = name;
    this.id = id;
    this.metadata = metadata;
    this.client = client;
    this.embeddingFunction = embeddingFunction;
  }

  async add(params: AddRecordsParams): Promise<void> {
    const { ids, embeddings, metadatas, documents } = this.arrayifyParams(params);

    // Generate embeddings if needed
    let embeddingsArray = embeddings;
    if (!embeddingsArray && documents) {
      embeddingsArray = await this.embeddingFunction.generate(documents.filter((d): d is string => d !== undefined));
    }

    if (!embeddingsArray) {
      throw new Error("embeddings and documents cannot both be undefined");
    }

    const metadatasJson = metadatas
      ? JSON.stringify(metadatas)
      : null;

    const documentsJson = documents
      ? JSON.stringify(documents)
      : null;

    this.client._getBindings().add(
      this.id,
      ids,
      embeddingsArray,
      metadatasJson,
      documentsJson,
      this.client.tenant,
      this.client.database,
    );
  }

  async upsert(params: UpsertRecordsParams): Promise<void> {
    const { ids, embeddings, metadatas, documents } = this.arrayifyParams(params);

    // Generate embeddings if needed
    let embeddingsArray = embeddings;
    if (!embeddingsArray && documents) {
      embeddingsArray = await this.embeddingFunction.generate(documents.filter((d): d is string => d !== undefined));
    }

    if (!embeddingsArray) {
      throw new Error("embeddings and documents cannot both be undefined");
    }

    const metadatasJson = metadatas ? JSON.stringify(metadatas) : null;
    const documentsJson = documents ? JSON.stringify(documents) : null;

    (this.client._getBindings() as any).upsert(
      this.id,
      ids,
      embeddingsArray,
      metadatasJson,
      documentsJson,
      this.client.tenant,
      this.client.database,
    );
  }

  async count(): Promise<number> {
    return this.client._getBindings().count(
      this.id,
      this.client.tenant,
      this.client.database,
    );
  }

  async get({
    ids,
    where,
    limit,
    offset,
    include,
    whereDocument,
  }: BaseGetParams = {}): Promise<GetResponse> {
    const idsArray = ids ? toArray(ids) : undefined;

    // Convert include enum to strings
    const includeStrings = include?.map((i) => i as string);

    // Convert where/whereDocument to JSON strings
    const whereJson = where ? JSON.stringify(where) : undefined;
    const whereDocumentJson = whereDocument ? JSON.stringify(whereDocument) : undefined;

    const response = (this.client._getBindings() as any).get(
      this.id,
      idsArray,
      limit,
      offset,
      this.client.tenant,
      this.client.database,
      includeStrings,
      whereJson,
      whereDocumentJson,
    );

    // Parse JSON fields
    const documents = response.documents
      ? (JSON.parse(response.documents) as (string | null)[])
      : null;

    const metadatas = response.metadatas
      ? (JSON.parse(response.metadatas) as (Metadata | null)[])
      : null;

    return {
      ids: response.ids,
      embeddings: response.embeddings as Embeddings | null,
      documents: documents ?? [],
      metadatas: metadatas ?? [],
      included: include ?? [IncludeEnum.Metadatas, IncludeEnum.Documents],
    };
  }

  async update(params: UpdateRecordsParams): Promise<void> {
    const { ids, embeddings, metadatas, documents } = this.arrayifyParams(params);

    // Generate embeddings if needed (only if documents provided)
    let embeddingsArray = embeddings;
    if (!embeddingsArray && documents) {
      embeddingsArray = await this.embeddingFunction.generate(
        documents.filter((d): d is string => d !== undefined)
      );
    }

    const metadatasJson = metadatas ? JSON.stringify(metadatas) : null;
    const documentsJson = documents ? JSON.stringify(documents) : null;

    (this.client._getBindings() as any).update(
      this.id,
      ids,
      embeddingsArray ?? undefined,
      metadatasJson,
      documentsJson,
      this.client.tenant,
      this.client.database,
    );
  }

  async query({
    nResults = 10,
    where,
    whereDocument,
    include,
    queryTexts,
    queryEmbeddings,
    ids,
  }: QueryRecordsParams): Promise<MultiQueryResponse> {
    let embeddings: number[][] = [];

    if (queryEmbeddings) {
      embeddings = toArrayOfArrays(queryEmbeddings);
    } else if (queryTexts) {
      embeddings = await this.embeddingFunction.generate(toArray(queryTexts));
    }

    if (embeddings.length === 0) {
      throw new TypeError(
        "You must provide either queryEmbeddings or queryTexts",
      );
    }

    // Convert include enum to strings
    const includeStrings = include?.map((i) => i as string);

    // Convert ids to array if provided
    const idsArray = ids ? toArray(ids) : undefined;

    // Convert where/whereDocument to JSON strings
    const whereJson = where ? JSON.stringify(where) : undefined;
    const whereDocumentJson = whereDocument ? JSON.stringify(whereDocument) : undefined;

    const response = (this.client._getBindings() as any).query(
      this.id,
      embeddings,
      nResults,
      this.client.tenant,
      this.client.database,
      includeStrings,
      idsArray,
      whereJson,
      whereDocumentJson,
    );

    // Parse JSON fields
    const documents = response.documents
      ? (JSON.parse(response.documents) as (string | null)[][])
      : null;

    const metadatas = response.metadatas
      ? (JSON.parse(response.metadatas) as (Metadata | null)[][])
      : null;

    const embeddingsResult = response.embeddings
      ? (JSON.parse(response.embeddings) as Embeddings[])
      : null;

    return {
      ids: response.ids,
      embeddings: embeddingsResult,
      documents: documents ?? [],
      metadatas: metadatas ?? [],
      distances: response.distances,
      included: include ?? [
        IncludeEnum.Metadatas,
        IncludeEnum.Documents,
        IncludeEnum.Distances,
      ],
    };
  }

  async peek({ limit = 10 }: PeekParams = {}): Promise<MultiGetResponse> {
    const response = await this.get({ limit });
    return response as MultiGetResponse;
  }

  async delete({ ids, where, whereDocument }: DeleteParams = {}): Promise<void> {
    const idsArray = ids ? toArray(ids) : undefined;

    // Convert where/whereDocument to JSON strings
    const whereJson = where ? JSON.stringify(where) : undefined;
    const whereDocumentJson = whereDocument ? JSON.stringify(whereDocument) : undefined;

    (this.client._getBindings() as any).deleteRecords(
      this.id,
      idsArray,
      this.client.tenant,
      this.client.database,
      whereJson,
      whereDocumentJson,
    );
  }

  async modify({
    name,
    metadata,
  }: {
    name?: string;
    metadata?: CollectionMetadata;
  }): Promise<void> {
    const metadataJson = metadata ? JSON.stringify(metadata) : undefined;

    (this.client._getBindings() as any).updateCollection(
      this.id,
      name,
      metadataJson,
    );

    // Update local state to match
    if (name) {
      this.name = name;
    }
    if (metadata) {
      this.metadata = metadata;
    }
  }

  private arrayifyParams(params: AddRecordsParams | UpdateRecordsParams): {
    ids: string[];
    embeddings: number[][] | undefined;
    metadatas: (Metadata | undefined)[] | undefined;
    documents: (string | undefined)[] | undefined;
  } {
    return {
      ids: toArray(params.ids),
      embeddings: params.embeddings
        ? toArrayOfArrays(params.embeddings)
        : undefined,
      metadatas: params.metadatas
        ? toArray<Metadata>(params.metadatas)
        : undefined,
      documents: params.documents ? toArray(params.documents) : undefined,
    };
  }
}
