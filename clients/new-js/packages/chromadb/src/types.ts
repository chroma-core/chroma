import { GetUserIdentityResponse, Include } from "./api";

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
  | { $gt: LiteralValue }
  | { $gte: LiteralValue }
  | { $lt: LiteralValue }
  | { $lte: LiteralValue }
  | { $ne: LiteralValue }
  | { $eq: LiteralValue }
  | { $and: LiteralValue }
  | { $or: LiteralValue }
  | { $in: LiteralValue[] }
  | { $nin: LiteralValue[] };

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

export type WhereDocument =
  | { $contains: string }
  | { $not_contains: string }
  | { $matches: string }
  | { $not_matches: string }
  | { $and: WhereDocument[] }
  | { $or: WhereDocument[] };

export enum IncludeEnum {
  distances = "distances",
  documents = "documents",
  embeddings = "embeddings",
  metadatas = "metadatas",
  uris = "uris",
}

export class GetResult<TMeta extends Metadata = Metadata> {
  public readonly documents: (string | null)[];
  public readonly embeddings: number[][];
  public readonly ids: string[];
  public readonly include: Include[];
  public readonly metadatas: (TMeta | null)[];
  public readonly uris: (string | null)[];

  constructor({
    documents,
    embeddings,
    ids,
    include,
    metadatas,
    uris,
  }: {
    documents: (string | null)[];
    embeddings: number[][];
    ids: string[];
    include: Include[];
    metadatas: (TMeta | null)[];
    uris: (string | null)[];
  }) {
    this.documents = documents;
    this.embeddings = embeddings;
    this.ids = ids;
    this.include = include;
    this.metadatas = metadatas;
    this.uris = uris;
  }

  public rows() {
    return {
      include: this.include,
      records: this.ids.map((id, index) => {
        return {
          id,
          document: this.include.includes("documents")
            ? this.documents[index]
            : undefined,
          embedding: this.include.includes("embeddings")
            ? this.embeddings[index]
            : undefined,
          metadata: this.include.includes("metadatas")
            ? this.metadatas[index]
            : undefined,
          uri: this.include.includes("uris") ? this.uris[index] : undefined,
        };
      }),
    };
  }
}

export interface QueryRowResult<TMeta extends Metadata = Metadata> {
  include: Include[];
  queries: {
    distance?: number | null;
    document?: string | null;
    embedding?: number[] | null;
    id: string;
    metadata?: TMeta | null;
    uri?: string | null;
  }[][];
}

export class QueryResult<TMeta extends Metadata = Metadata> {
  public readonly distances: (number | null)[][];
  public readonly documents: (string | null)[][];
  public readonly embeddings: (number[] | null)[][];
  public readonly ids: string[][];
  public readonly include: Include[];
  public readonly metadatas: (TMeta | null)[][];
  public readonly uris: (string | null)[][];

  constructor({
    distances,
    documents,
    embeddings,
    ids,
    include,
    metadatas,
    uris,
  }: {
    distances: (number | null)[][];
    documents: (string | null)[][];
    embeddings: (number[] | null)[][];
    ids: string[][];
    include: Include[];
    metadatas: (TMeta | null)[][];
    uris: (string | null)[][];
  }) {
    this.distances = distances;
    this.documents = documents;
    this.embeddings = embeddings;
    this.ids = ids;
    this.include = include;
    this.metadatas = metadatas;
    this.uris = uris;
  }

  public rows(): QueryRowResult<TMeta> {
    const queries: {
      distance?: number | null;
      document?: string | null;
      embedding?: number[] | null;
      id: string;
      metadata?: TMeta | null;
      uri?: string | null;
    }[][] = [];

    for (let q = 0; q < this.ids.length; q++) {
      const records = this.ids[q].map((id, index) => {
        return {
          id,
          document: this.include.includes("documents")
            ? this.documents[q][index]
            : undefined,
          embedding: this.include.includes("embeddings")
            ? this.embeddings[q][index]
            : undefined,
          metadata: this.include.includes("metadatas")
            ? this.metadatas[q][index]
            : undefined,
          uri: this.include.includes("uris") ? this.uris[q][index] : undefined,
          distance: this.include.includes("distances")
            ? this.distances[q][index]
            : undefined,
        };
      });

      queries.push(records);
    }

    return {
      include: this.include,
      queries,
    };
  }
}
