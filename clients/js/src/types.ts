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

type LiteralValue = string | number;
type LogicalOperator = "$and" | "$or";
type WhereOperator = "$gt" | "$gte" | "$lt" | "$lte" | "$ne" | "$eq";

type OperatorExpression = {
  [key in WhereOperator | LogicalOperator]: LiteralValue;
};

export type Where = {
  [key: string | LogicalOperator]: LiteralValue | OperatorExpression | Where[];
};

type WhereDocumentOperator = "$contains" | LogicalOperator;

export type WhereDocument = {
  [key in WhereDocumentOperator]: string | WhereDocument[];
};
