import { AuthOptions } from "./auth";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";

export enum IncludeEnum {
  Documents = "documents",
  Embeddings = "embeddings",
  Metadatas = "metadatas",
  Distances = "distances",
}

type Number = number;
export type Embedding = Array<Number>;
export type Embeddings = Array<Embedding>;

export type Metadata = Record<string, string | number | boolean>;
export type Metadatas = Array<Metadata>;

export type Document = string;
export type Documents = Array<Document>;

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

export type GetResponse = {
  ids: IDs;
  embeddings: null | Embeddings;
  documents: (null | Document)[];
  metadatas: (null | Metadata)[];
  error: null | string;
  included: IncludeEnum[]
};

export type QueryResponse = {
  ids: IDs[];
  embeddings: null | Embeddings[];
  documents: (null | Document)[][];
  metadatas: (null | Metadata)[][];
  distances: null | number[][];
  included: IncludeEnum[]
};

export type AddResponse = {
  error: string;
};

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
  embeddingFunction?: IEmbeddingFunction;
};

export type DeleteCollectionParams = {
  name: string;
};

export type AddParams = {
  ids: ID | IDs;
  embeddings?: Embedding | Embeddings;
  metadatas?: Metadata | Metadatas;
  documents?: Document | Documents;
};

export type UpsertParams = AddParams;
export type UpdateParams = AddParams;

export type ModifyCollectionParams = {
  name?: string;
  metadata?: CollectionMetadata;
};

export type QueryParams = {
  queryEmbeddings?: Embedding | Embeddings;
  nResults?: PositiveInteger;
  where?: Where;
  queryTexts?: string | string[];
  whereDocument?: WhereDocument; // {"$contains":"search_string"}
  include?: IncludeEnum[]; // ["metadata", "document"]
};

export type PeekParams = { limit?: PositiveInteger };

export type DeleteParams = {
  ids?: ID | IDs;
  where?: Where;
  whereDocument?: WhereDocument;
};
