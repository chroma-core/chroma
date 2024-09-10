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
  configuration_json: any;
};

export type MultiGetResponse = {
  ids: IDs;
  embeddings: Embeddings | null;
  documents: (Document | null)[];
  metadatas: (Metadata | null)[];
  included: IncludeEnum[];
};

export type GetResponse = MultiGetResponse;

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
  ids: IDs;
};

export interface CollectionParams {
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

export type BaseGetParams = {
  ids?: ID | IDs;
  where?: Where;
  limit?: PositiveInteger;
  offset?: PositiveInteger;
  include?: IncludeEnum[];
  whereDocument?: WhereDocument;
};

export type SingleGetParams = BaseGetParams & {
  ids: ID;
};

export type MultiGetParams = BaseGetParams & {
  ids?: IDs;
};

export type GetParams = SingleGetParams | MultiGetParams;

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

export type BaseRecordOperationParams = {
  ids?: ID | IDs;
  embeddings?: Embedding | Embeddings;
  metadatas?: Metadata | Metadatas;
  documents?: Document | Documents;
};

export type SingleRecordOperationParams =
  SingleRecordOperationParamsWithIDsOptional & {
    ids: ID;
  };

export type SingleRecordOperationParamsWithIDsOptional =
  BaseRecordOperationParams & {
    ids?: ID;
    embeddings?: Embedding;
    metadatas?: Metadata;
    documents?: Document;
  };

type SingleEmbeddingRecordOperationParams = SingleRecordOperationParams & {
  embeddings: Embedding;
};

type SingleContentRecordOperationParams = SingleRecordOperationParams & {
  documents: Document;
};

export type SingleAddRecordOperationParams =
  | SingleEmbeddingRecordOperationParams
  | SingleContentRecordOperationParams;

type SingleEmbeddingRecordOperationParamsWithOptionalIDs =
  SingleRecordOperationParamsWithIDsOptional & {
    embeddings: Embedding;
  };

type SingleContentRecordOperationParamsWithOptionalIDs =
  SingleRecordOperationParamsWithIDsOptional & {
    documents: Document;
  };

export type SingleAddRecordOperationParamsWithOptionalIDs =
  | SingleEmbeddingRecordOperationParamsWithOptionalIDs
  | SingleContentRecordOperationParamsWithOptionalIDs;

export type MultiRecordOperationParams =
  MultiRecordOperationParamsWithIDsOptional & {
    ids: IDs;
  };

export type MultiRecordOperationParamsWithIDsOptional =
  BaseRecordOperationParams & {
    ids?: IDs;
    embeddings?: Embeddings;
    metadatas?: Metadatas;
    documents?: Documents;
  };

type MultiEmbeddingRecordOperationParams = MultiRecordOperationParams & {
  embeddings: Embeddings;
};

type MultiContentRecordOperationParams = MultiRecordOperationParams & {
  documents: Documents;
};

export type MultiAddRecordsOperationParams =
  | MultiEmbeddingRecordOperationParams
  | MultiContentRecordOperationParams;

type MultiEmbeddingRecordOperationParamsWithOptionalIDs =
  MultiRecordOperationParamsWithIDsOptional & {
    embeddings: Embeddings;
  };

type MultiContentRecordOperationParamsWithOptionalIDs =
  MultiRecordOperationParamsWithIDsOptional & {
    documents: Documents;
  };

export type MultiAddRecordsOperationParamsWithOptionalIDs =
  | MultiEmbeddingRecordOperationParamsWithOptionalIDs
  | MultiContentRecordOperationParamsWithOptionalIDs;

export type AddRecordsParams =
  | SingleAddRecordOperationParamsWithOptionalIDs
  | MultiAddRecordsOperationParamsWithOptionalIDs;

export type UpsertRecordsParams =
  | SingleAddRecordOperationParams
  | MultiAddRecordsOperationParams;

export type UpdateRecordsParams =
  | MultiRecordOperationParams
  | SingleRecordOperationParams;

export type ModifyCollectionParams = {
  name?: string;
  metadata?: CollectionMetadata;
};

export type BaseQueryParams = {
  nResults?: PositiveInteger;
  where?: Where;
  queryTexts?: string | string[];
  queryEmbeddings?: Embedding | Embeddings;
  whereDocument?: WhereDocument; // {"$contains":"search_string"}
  include?: IncludeEnum[]; // ["metadata", "document"]
};

export type SingleTextQueryParams = BaseQueryParams & {
  queryTexts: string;
  queryEmbeddings?: never;
};

export type SingleEmbeddingQueryParams = BaseQueryParams & {
  queryTexts?: never;
  queryEmbeddings: Embedding;
};

export type MultiTextQueryParams = BaseQueryParams & {
  queryTexts: string[];
  queryEmbeddings?: never;
};

export type MultiEmbeddingQueryParams = BaseQueryParams & {
  queryTexts?: never;
  queryEmbeddings: Embeddings;
};

export type QueryRecordsParams =
  | SingleTextQueryParams
  | SingleEmbeddingQueryParams
  | MultiTextQueryParams
  | MultiEmbeddingQueryParams;

export type PeekParams = { limit?: PositiveInteger };

export type DeleteParams = {
  ids?: ID | IDs;
  where?: Where;
  whereDocument?: WhereDocument;
};
