import { GetUserIdentityResponse, HashMap, Include } from "./api";

export type UserIdentity = GetUserIdentityResponse;

export type CollectionMetadata = Record<string, boolean | number | string>;

export type Metadata = Record<string, boolean | number | string>;

export interface BaseRecordSet {
  embeddings?: number[][];
  metadatas?: Metadata[];
  documents?: string[];
  uris?: string[];
}

export const baseRecordSetFields = [
  "ids",
  "embeddings",
  "metadatas",
  "documents",
  "uris",
];

export interface RecordSet extends BaseRecordSet {
  ids: string[];
}

export const recordSetFields = [...baseRecordSetFields, "ids"];

export interface QueryRecordSet extends BaseRecordSet {
  ids?: string[];
  embeddings: number[][];
}

type LiteralValue = string | number | boolean;

type LogicalOperator = "$and" | "$or";

type WhereOperator = "$gt" | "$gte" | "$lt" | "$lte" | "$ne" | "$eq";

type InclusionExclusionOperator = "$in" | "$nin";

type OperatorExpression =
  | { [key in WhereOperator | LogicalOperator]: LiteralValue }
  | { [key in InclusionExclusionOperator]: LiteralValue[] };

export type Where =
  | { [key: string]: LiteralValue | OperatorExpression }
  | { $and: Where[] }
  | { $or: Where[] };

type WhereDocumentOperator =
  | "$contains"
  | "$not_contains"
  | "$matches"
  | "$not_matches"
  | LogicalOperator;

export type WhereDocument = {
  [key in WhereDocumentOperator]: string | WhereDocument[];
};

export enum IncludeEnum {
  distances = "distances",
  documents = "documents",
  embeddings = "embeddings",
  metadatas = "metadatas",
  uris = "uris",
}

export interface GetResult {
  documents?: (string | null)[];
  embeddings?: number[][];
  ids: string[];
  include: Include[];
  metadatas?: (Metadata | null)[];
  uris?: (string | null)[];
}

export interface QueryResult {
  distances?: (number | null)[][];
  documents?: (string | null)[][];
  embeddings?: (number[] | null)[][];
  ids: string[][];
  include: Include[];
  metadatas?: (Metadata | null)[][];
  uris?: (string | null)[][];
}
