import { AuthOptions } from "./auth";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";

export enum IncludeEnum {
  Documents = "documents",
  Embeddings = "embeddings",
  Metadatas = "metadatas",
  Distances = "distances",
}

export type Embedding = number[];
export type Embeddings = Embedding[];

export type Metadata = Record<string, string | number | boolean>;
export type Metadatas = Metadata[];

export type Document = string;
export type Documents = Document[];

export type ID = string;
export type IDs = ID[];

export type PositiveInteger = number;

type LiteralValue = string | number | boolean;
type ListLiteralValue = LiteralValue[];
type LiteralNumber = number;
type LogicalOperator = "$and" | "$or";
type InclusionOperator = "$in" | "$nin";
type WhereOperator = "$gt" | "$gte" | "$lt" | "$lte" | "$ne" | "$eq";

type OperatorExpression = {
  [key in WhereOperator | InclusionOperator | LogicalOperator]?:
    | LiteralValue
    | ListLiteralValue;
};

type BaseWhere = {
  [key: string]: LiteralValue | OperatorExpression;
};

type LogicalWhere = {
  [key in LogicalOperator]?: Where[];
};

export type Where = BaseWhere | LogicalWhere;

type WhereDocumentOperator = "$contains" | "$not_contains" | LogicalOperator;

export type WhereDocument = {
  [key in WhereDocumentOperator]?:
    | LiteralValue
    | LiteralNumber
    | WhereDocument[];
};

export type CollectionType = {
  name: string;
  id: string;
  metadata: Metadata | null;
};

export type MultiGetResponse = {
  ids: IDs;
  embeddings: Embeddings | null;
  documents: (Document | null)[];
  metadatas: (Metadata | null)[];
  error: string | null;
  included: IncludeEnum[];
};

export type SingleGetResponse = {
  ids: ID | null;
  embeddings: Embedding | null;
  documents: Document | null;
  metadatas: Metadata | null;
  error: string | null;
  included: IncludeEnum[];
};

export type GetResponse = SingleGetResponse | MultiGetResponse;

export type SingleQueryResponse = {
  ids: IDs;
  embeddings: Embeddings | null;
  documents: (Document | null)[];
  metadatas: (Metadata | null)[];
  distances: number[] | null;
  included: IncludeEnum[];
};

export type MultiQueryResponse = {
  ids: IDs[];
  embeddings: Embeddings[] | null;
  documents: (Document | null)[][];
  metadatas: (Metadata | null)[][];
  distances: number[][] | null;
  included: IncludeEnum[];
};

export type QueryResponse = SingleQueryResponse | MultiQueryResponse;

export type AddResponse = {
  error: string;
};

export interface Collection {
  name: string;
  id: string;
  metadata: CollectionMetadata | undefined;
  embeddingFunction: IEmbeddingFunction;
}

export type CollectionMetadata = Record<string, unknown>;

// RequestInit can be used to set Authorization headers and more
// see all options here: https://www.jsdocs.io/package/@types/node-fetch#RequestInit
export type ConfigOptions = {
  options?: RequestInit;
};

export type GetParams = {
  ids?: ID | IDs;
  where?: Where;
  limit?: PositiveInteger;
  offset?: PositiveInteger;
  include?: IncludeEnum[];
  whereDocument?: WhereDocument;
};

export type SingleGetParams = GetParams & {
  ids: ID;
};

export type MultiGetParams = GetParams & {
  ids?: IDs;
};

export type ListCollectionsParams = {
  limit?: PositiveInteger;
  offset?: PositiveInteger;
};

export type ChromaClientParams = {
  path?: string;
  fetchOptions?: RequestInit;
  auth?: AuthOptions;
  tenant?: string;
  database?: string;
};

export type CreateCollectionParams = {
  name: string;
  metadata?: CollectionMetadata;
  embeddingFunction?: IEmbeddingFunction;
};

export type GetOrCreateCollectionParams = CreateCollectionParams;

export type GetCollectionParams = {
  name: string;
  embeddingFunction: IEmbeddingFunction;
};

export type DeleteCollectionParams = {
  name: string;
};

type SingleDocumentOperationParams = {
  ids: ID;
  embeddings?: Embedding;
  metadatas?: Metadata;
  documents?: Document;
};

type SingleEmbeddingDocumentOperationParams = SingleDocumentOperationParams & {
  embeddings: Embedding;
};

type SingleContentDocumentOperationParams = SingleDocumentOperationParams & {
  documents: Document;
};

type SingleAddDocumentOperationParams =
  | SingleEmbeddingDocumentOperationParams
  | SingleContentDocumentOperationParams;

export type MultiDocumentOperationParams = {
  ids: IDs;
  embeddings?: Embeddings;
  metadatas?: Metadatas;
  documents?: Documents;
};

type MultiEmbeddingDocumentOperationParams = MultiDocumentOperationParams & {
  embeddings: Embeddings;
};

type MultiContentDocumentOperationParams = MultiDocumentOperationParams & {
  documents: Documents;
};

type MultiAddDocumentOperationParams =
  | MultiEmbeddingDocumentOperationParams
  | MultiContentDocumentOperationParams;

export type AddDocumentsParams =
  | SingleAddDocumentOperationParams
  | MultiAddDocumentOperationParams;

export type UpsertDocumentsParams = AddDocumentsParams;
export type UpdateDocumentsParams =
  | MultiDocumentOperationParams
  | SingleDocumentOperationParams;

export type ModifyCollectionParams = {
  name?: string;
  metadata?: CollectionMetadata;
};

/** This type represents the different ways the user can express a query for documents
 *  - string: a simple text query which will be converted to an Embedding
 *  - Embedding: a list of numbers representing the embedding of the query
 */

export type SingleQueryParams = {
  nResults?: PositiveInteger;
  where?: Where;
  query: string | Embedding;
  whereDocument?: WhereDocument; // {"$contains":"search_string"}
  include?: IncludeEnum[]; // ["metadata", "document"]
};

export type MultiQueryParams = {
  nResults?: PositiveInteger;
  where?: Where;
  query: string[] | Embeddings;
  whereDocument?: WhereDocument; // {"$contains":"search_string"}
  include?: IncludeEnum[]; // ["metadata", "document"]
};

export type QueryDocumentsParams = SingleQueryParams | MultiQueryParams;

export type PeekParams = { limit?: PositiveInteger };

export type DeleteParams = {
  ids?: ID | IDs;
  where?: Where;
  whereDocument?: WhereDocument;
};
