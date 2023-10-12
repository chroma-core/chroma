export enum IncludeEnum {
  Documents = 'documents',
  Embeddings = 'embeddings',
  Metadatas = 'metadatas',
  Distances = 'distances'
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
  [key in WhereOperator | InclusionOperator | LogicalOperator ]?: LiteralValue | ListLiteralValue;
};

type BaseWhere = {
  [key: string]: LiteralValue | OperatorExpression;
};

type LogicalWhere = {
  [key in LogicalOperator]?: Where[];
};

export type Where = BaseWhere | LogicalWhere;

type WhereDocumentOperator = "$contains" | LogicalOperator;

export type WhereDocument = {
  [key in WhereDocumentOperator]?: LiteralValue | LiteralNumber | WhereDocument[];
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
};

export type QueryResponse = {
  ids: IDs[];
  embeddings: null | Embeddings[];
  documents: (null | Document)[][];
  metadatas: (null | Metadata)[][];
  distances: null | number[][];
}

export type AddResponse = {
  error: string;
}

export type CollectionMetadata = Record<string, unknown>;

// RequestInit can be used to set Authorization headers and more
// see all options here: https://www.jsdocs.io/package/@types/node-fetch#RequestInit
export type ConfigOptions = {
  options?: RequestInit;
};
