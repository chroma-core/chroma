import type {
  HashMap,
  SearchPayload,
  SearchResponse,
  SparseVector,
} from "../api";

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const deepClone = <T>(value: T): T =>
  JSON.parse(JSON.stringify(value)) as T;

// -----------------------------------------------------------------------------
// Where expressions
// -----------------------------------------------------------------------------

export type WhereJSON = Record<string, unknown>;
export type WhereInput = WhereExpression | WhereJSON | null | undefined;

abstract class WhereExpressionBase {
  public abstract toJSON(): WhereJSON;

  public and(other: WhereInput): WhereExpression {
    const target = WhereExpression.from(other);
    if (!target) {
      return this as unknown as WhereExpression;
    }
    return AndWhere.combine(this as unknown as WhereExpression, target);
  }

  public or(other: WhereInput): WhereExpression {
    const target = WhereExpression.from(other);
    if (!target) {
      return this as unknown as WhereExpression;
    }
    return OrWhere.combine(this as unknown as WhereExpression, target);
  }
}

export abstract class WhereExpression extends WhereExpressionBase {
  public static from(input: WhereInput): WhereExpression | undefined {
    if (input instanceof WhereExpression) {
      return input;
    }
    if (input === null || input === undefined) {
      return undefined;
    }
    if (!isPlainObject(input)) {
      throw new TypeError("Where input must be a WhereExpression or plain object");
    }
    return parseWhereDict(input);
  }
}

class AndWhere extends WhereExpression {
  constructor(private readonly conditions: WhereExpression[]) {
    super();
  }

  public toJSON(): WhereJSON {
    return { $and: this.conditions.map((condition) => condition.toJSON()) };
  }

  public get operands(): WhereExpression[] {
    return this.conditions.slice();
  }

  public static combine(
    left: WhereExpression,
    right: WhereExpression,
  ): WhereExpression {
    const flattened: WhereExpression[] = [];

    const add = (expr: WhereExpression) => {
      if (expr instanceof AndWhere) {
        flattened.push(...expr.operands);
      } else {
        flattened.push(expr);
      }
    };

    add(left);
    add(right);

    if (flattened.length === 1) {
      return flattened[0];
    }

    return new AndWhere(flattened);
  }
}

class OrWhere extends WhereExpression {
  constructor(private readonly conditions: WhereExpression[]) {
    super();
  }

  public toJSON(): WhereJSON {
    return { $or: this.conditions.map((condition) => condition.toJSON()) };
  }

  public get operands(): WhereExpression[] {
    return this.conditions.slice();
  }

  public static combine(
    left: WhereExpression,
    right: WhereExpression,
  ): WhereExpression {
    const flattened: WhereExpression[] = [];

    const add = (expr: WhereExpression) => {
      if (expr instanceof OrWhere) {
        flattened.push(...expr.operands);
      } else {
        flattened.push(expr);
      }
    };

    add(left);
    add(right);

    if (flattened.length === 1) {
      return flattened[0];
    }

    return new OrWhere(flattened);
  }
}

class ComparisonWhere extends WhereExpression {
  constructor(
    private readonly key: string,
    private readonly operator: string,
    private readonly value: unknown,
  ) {
    super();
  }

  public toJSON(): WhereJSON {
    return {
      [this.key]: {
        [this.operator]: this.value,
      },
    };
  }
}

const comparisonOperatorMap = new Map<string, (key: string, value: unknown) => WhereExpression>([
  ["$eq", (key, value) => new ComparisonWhere(key, "$eq", value)],
  ["$ne", (key, value) => new ComparisonWhere(key, "$ne", value)],
  ["$gt", (key, value) => new ComparisonWhere(key, "$gt", value)],
  ["$gte", (key, value) => new ComparisonWhere(key, "$gte", value)],
  ["$lt", (key, value) => new ComparisonWhere(key, "$lt", value)],
  ["$lte", (key, value) => new ComparisonWhere(key, "$lte", value)],
  ["$in", (key, value) => new ComparisonWhere(key, "$in", value)],
  ["$nin", (key, value) => new ComparisonWhere(key, "$nin", value)],
  ["$contains", (key, value) => new ComparisonWhere(key, "$contains", value)],
  ["$not_contains", (key, value) => new ComparisonWhere(key, "$not_contains", value)],
  ["$regex", (key, value) => new ComparisonWhere(key, "$regex", value)],
  ["$not_regex", (key, value) => new ComparisonWhere(key, "$not_regex", value)],
]);

const parseWhereDict = (data: Record<string, unknown>): WhereExpression => {
  if ("$and" in data) {
    if (Object.keys(data).length !== 1) {
      throw new Error("$and cannot be combined with other keys");
    }
    const rawConditions = data["$and"];
    if (!Array.isArray(rawConditions) || rawConditions.length === 0) {
      throw new TypeError("$and must be a non-empty array");
    }
    const conditions = rawConditions.map((item, index) => {
      const expr = WhereExpression.from(item as WhereInput);
      if (!expr) {
        throw new TypeError(`Invalid where clause at index ${index}`);
      }
      return expr;
    });
    if (conditions.length === 1) {
      return conditions[0];
    }
    return conditions.slice(1).reduce(
      (acc, condition) => AndWhere.combine(acc, condition),
      conditions[0],
    );
  }

  if ("$or" in data) {
    if (Object.keys(data).length !== 1) {
      throw new Error("$or cannot be combined with other keys");
    }
    const rawConditions = data["$or"];
    if (!Array.isArray(rawConditions) || rawConditions.length === 0) {
      throw new TypeError("$or must be a non-empty array");
    }
    const conditions = rawConditions.map((item, index) => {
      const expr = WhereExpression.from(item as WhereInput);
      if (!expr) {
        throw new TypeError(`Invalid where clause at index ${index}`);
      }
      return expr;
    });
    if (conditions.length === 1) {
      return conditions[0];
    }
    return conditions.slice(1).reduce(
      (acc, condition) => OrWhere.combine(acc, condition),
      conditions[0],
    );
  }

  const entries = Object.entries(data);
  if (entries.length !== 1) {
    throw new Error("Where dictionary must contain exactly one field");
  }

  const [field, value] = entries[0];
  if (!isPlainObject(value)) {
    return new ComparisonWhere(field, "$eq", value);
  }

  const operatorEntries = Object.entries(value);
  if (operatorEntries.length !== 1) {
    throw new Error(`Operator dictionary for field \"${field}\" must contain exactly one operator`);
  }

  const [operator, operand] = operatorEntries[0];
  const factory = comparisonOperatorMap.get(operator);
  if (!factory) {
    throw new Error(`Unsupported where operator: ${operator}`);
  }

  return factory(field, operand);
};

// -----------------------------------------------------------------------------
// Key helper
// -----------------------------------------------------------------------------

type IterableInput<T> = Iterable<T> | ArrayLike<T>;

const iterableToArray = <T>(values: IterableInput<T>): T[] => {
  if (Array.isArray(values)) {
    return values.slice();
  }
  return Array.from(values as Iterable<T>);
};

const assertNonEmptyArray = (values: unknown[], message: string) => {
  if (values.length === 0) {
    throw new Error(message);
  }
};

export class Key {
  public static readonly ID = new Key("#id");
  public static readonly DOCUMENT = new Key("#document");
  public static readonly EMBEDDING = new Key("#embedding");
  public static readonly METADATA = new Key("#metadata");
  public static readonly SCORE = new Key("#score");

  constructor(public readonly name: string) {}

  public eq(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$eq", value);
  }

  public ne(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$ne", value);
  }

  public gt(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$gt", value);
  }

  public gte(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$gte", value);
  }

  public lt(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$lt", value);
  }

  public lte(value: unknown): WhereExpression {
    return new ComparisonWhere(this.name, "$lte", value);
  }

  public isIn(values: IterableInput<unknown>): WhereExpression {
    const array = iterableToArray(values);
    assertNonEmptyArray(array, "$in requires at least one value");
    return new ComparisonWhere(this.name, "$in", array);
  }

  public notIn(values: IterableInput<unknown>): WhereExpression {
    const array = iterableToArray(values);
    assertNonEmptyArray(array, "$nin requires at least one value");
    return new ComparisonWhere(this.name, "$nin", array);
  }

  public contains(value: string): WhereExpression {
    if (typeof value !== "string") {
      throw new TypeError("$contains requires a string value");
    }
    return new ComparisonWhere(this.name, "$contains", value);
  }

  public notContains(value: string): WhereExpression {
    if (typeof value !== "string") {
      throw new TypeError("$not_contains requires a string value");
    }
    return new ComparisonWhere(this.name, "$not_contains", value);
  }

  public regex(pattern: string): WhereExpression {
    if (typeof pattern !== "string") {
      throw new TypeError("$regex requires a string pattern");
    }
    return new ComparisonWhere(this.name, "$regex", pattern);
  }

  public notRegex(pattern: string): WhereExpression {
    if (typeof pattern !== "string") {
      throw new TypeError("$not_regex requires a string pattern");
    }
    return new ComparisonWhere(this.name, "$not_regex", pattern);
  }
}

export interface KeyFactory {
  (name: string): Key;
  ID: Key;
  DOCUMENT: Key;
  EMBEDDING: Key;
  METADATA: Key;
  SCORE: Key;
}

const createKeyFactory = (): KeyFactory => {
  const factory = ((name: string) => new Key(name)) as KeyFactory;
  factory.ID = Key.ID;
  factory.DOCUMENT = Key.DOCUMENT;
  factory.EMBEDDING = Key.EMBEDDING;
  factory.METADATA = Key.METADATA;
  factory.SCORE = Key.SCORE;
  return factory;
};

export const K: KeyFactory = createKeyFactory();

// -----------------------------------------------------------------------------
// Limit
// -----------------------------------------------------------------------------

export interface LimitOptions {
  offset?: number;
  limit?: number | null | undefined;
}

export type LimitInput = Limit | number | LimitOptions | null | undefined;

export class Limit {
  public readonly offset: number;
  public readonly limit?: number;

  constructor(options: LimitOptions = {}) {
    const { offset = 0, limit } = options;

    if (!Number.isInteger(offset) || offset < 0) {
      throw new TypeError("Limit offset must be a non-negative integer");
    }

    if (limit !== null && limit !== undefined) {
      if (!Number.isInteger(limit) || limit <= 0) {
        throw new TypeError("Limit must be a positive integer when provided");
      }
      this.limit = limit;
    }

    this.offset = offset;
  }

  public static from(input: LimitInput, offsetOverride?: number): Limit {
    if (input instanceof Limit) {
      return new Limit({ offset: input.offset, limit: input.limit });
    }

    if (typeof input === "number") {
      return new Limit({ limit: input, offset: offsetOverride ?? 0 });
    }

    if (input === null || input === undefined) {
      return new Limit();
    }

    if (typeof input === "object") {
      return new Limit(input as LimitOptions);
    }

    throw new TypeError("Invalid limit input");
  }

  public toJSON(): { offset: number; limit?: number } {
    const result: { offset: number; limit?: number } = { offset: this.offset };
    if (this.limit !== undefined) {
      result.limit = this.limit;
    }
    return result;
  }
}

// -----------------------------------------------------------------------------
// Select
// -----------------------------------------------------------------------------

export type SelectKeyInput = string | Key;

export type SelectInput =
  | Select
  | Iterable<SelectKeyInput>
  | { keys?: Iterable<SelectKeyInput> }
  | null
  | undefined;

export class Select {
  private readonly keys: string[];

  constructor(keys: Iterable<SelectKeyInput> = []) {
    const unique = new Set<string>();
    for (const key of keys) {
      const normalized = key instanceof Key ? key.name : key;
      if (typeof normalized !== "string") {
        throw new TypeError("Select keys must be strings or Key instances");
      }
      unique.add(normalized);
    }
    this.keys = Array.from(unique);
  }

  public static from(input: SelectInput): Select {
    if (input instanceof Select) {
      return new Select(input.keys);
    }

    if (input === null || input === undefined) {
      return new Select();
    }

    if (Symbol.iterator in Object(input)) {
      return new Select(input as Iterable<SelectKeyInput>);
    }

    if (typeof input === "object" && "keys" in (input as Record<string, unknown>)) {
      const { keys } = input as { keys?: Iterable<SelectKeyInput> };
      return new Select(keys ?? []);
    }

    throw new TypeError("Unsupported select input");
  }

  public static all(): Select {
    return new Select([Key.DOCUMENT, Key.EMBEDDING, Key.METADATA, Key.SCORE]);
  }

  public get values(): string[] {
    return this.keys.slice();
  }

  public toJSON(): { keys: string[] } {
    return { keys: this.values };
  }
}

// -----------------------------------------------------------------------------
// Rank expressions
// -----------------------------------------------------------------------------

export type RankLiteral = Record<string, unknown>;
export type RankInput = RankExpression | RankLiteral | number | null | undefined;

const requireNumber = (value: unknown, message: string): number => {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    throw new TypeError(message);
  }
  return value;
};

abstract class RankExpressionBase {
  public abstract toJSON(): Record<string, unknown>;

  public add(...others: RankInput[]): RankExpression {
    if (others.length === 0) {
      return this as unknown as RankExpression;
    }
    const expressions = [
      this as unknown as RankExpression,
      ...others.map((item, index) => requireRank(item, `add operand ${index}`)),
    ];
    return SumRankExpression.create(expressions);
  }

  public subtract(other: RankInput): RankExpression {
    return new SubRankExpression(
      this as unknown as RankExpression,
      requireRank(other, "subtract operand"),
    );
  }

  public multiply(...others: RankInput[]): RankExpression {
    if (others.length === 0) {
      return this as unknown as RankExpression;
    }
    const expressions = [
      this as unknown as RankExpression,
      ...others.map((item, index) => requireRank(item, `multiply operand ${index}`)),
    ];
    return MulRankExpression.create(expressions);
  }

  public divide(other: RankInput): RankExpression {
    return new DivRankExpression(
      this as unknown as RankExpression,
      requireRank(other, "divide operand"),
    );
  }

  public negate(): RankExpression {
    return this.multiply(-1);
  }

  public abs(): RankExpression {
    return new AbsRankExpression(this as unknown as RankExpression);
  }

  public exp(): RankExpression {
    return new ExpRankExpression(this as unknown as RankExpression);
  }

  public log(): RankExpression {
    return new LogRankExpression(this as unknown as RankExpression);
  }

  public max(...others: RankInput[]): RankExpression {
    if (others.length === 0) {
      return this as unknown as RankExpression;
    }
    const expressions = [
      this as unknown as RankExpression,
      ...others.map((item, index) => requireRank(item, `max operand ${index}`)),
    ];
    return MaxRankExpression.create(expressions);
  }

  public min(...others: RankInput[]): RankExpression {
    if (others.length === 0) {
      return this as unknown as RankExpression;
    }
    const expressions = [
      this as unknown as RankExpression,
      ...others.map((item, index) => requireRank(item, `min operand ${index}`)),
    ];
    return MinRankExpression.create(expressions);
  }
}

export abstract class RankExpression extends RankExpressionBase {
  public static from(input: RankInput): RankExpression | undefined {
    if (input instanceof RankExpression) {
      return input;
    }
    if (input === null || input === undefined) {
      return undefined;
    }
    if (typeof input === "number") {
      return new ValueRankExpression(input);
    }
    if (isPlainObject(input)) {
      return new RawRankExpression(input);
    }
    throw new TypeError("Rank input must be a RankExpression, number, or plain object");
  }
}

class RawRankExpression extends RankExpression {
  constructor(private readonly raw: RankLiteral) {
    super();
  }

  public toJSON(): RankLiteral {
    return deepClone(this.raw);
  }
}

class ValueRankExpression extends RankExpression {
  constructor(private readonly value: number) {
    super();
  }

  public toJSON(): RankLiteral {
    return { $val: this.value };
  }
}

class SumRankExpression extends RankExpression {
  constructor(private readonly ranks: RankExpression[]) {
    super();
  }

  public static create(ranks: RankExpression[]): RankExpression {
    const flattened: RankExpression[] = [];
    for (const rank of ranks) {
      if (rank instanceof SumRankExpression) {
        flattened.push(...rank.operands);
      } else {
        flattened.push(rank);
      }
    }
    if (flattened.length === 1) {
      return flattened[0];
    }
    return new SumRankExpression(flattened);
  }

  public get operands(): RankExpression[] {
    return this.ranks.slice();
  }

  public toJSON(): RankLiteral {
    return { $sum: this.ranks.map((rank) => rank.toJSON()) };
  }
}

class SubRankExpression extends RankExpression {
  constructor(
    private readonly left: RankExpression,
    private readonly right: RankExpression,
  ) {
    super();
  }

  public toJSON(): RankLiteral {
    return {
      $sub: {
        left: this.left.toJSON(),
        right: this.right.toJSON(),
      },
    };
  }
}

class MulRankExpression extends RankExpression {
  constructor(private readonly ranks: RankExpression[]) {
    super();
  }

  public static create(ranks: RankExpression[]): RankExpression {
    const flattened: RankExpression[] = [];
    for (const rank of ranks) {
      if (rank instanceof MulRankExpression) {
        flattened.push(...rank.operands);
      } else {
        flattened.push(rank);
      }
    }
    if (flattened.length === 1) {
      return flattened[0];
    }
    return new MulRankExpression(flattened);
  }

  public get operands(): RankExpression[] {
    return this.ranks.slice();
  }

  public toJSON(): RankLiteral {
    return { $mul: this.ranks.map((rank) => rank.toJSON()) };
  }
}

class DivRankExpression extends RankExpression {
  constructor(
    private readonly left: RankExpression,
    private readonly right: RankExpression,
  ) {
    super();
  }

  public toJSON(): RankLiteral {
    return {
      $div: {
        left: this.left.toJSON(),
        right: this.right.toJSON(),
      },
    };
  }
}

class AbsRankExpression extends RankExpression {
  constructor(private readonly operand: RankExpression) {
    super();
  }

  public toJSON(): RankLiteral {
    return { $abs: this.operand.toJSON() };
  }
}

class ExpRankExpression extends RankExpression {
  constructor(private readonly operand: RankExpression) {
    super();
  }

  public toJSON(): RankLiteral {
    return { $exp: this.operand.toJSON() };
  }
}

class LogRankExpression extends RankExpression {
  constructor(private readonly operand: RankExpression) {
    super();
  }

  public toJSON(): RankLiteral {
    return { $log: this.operand.toJSON() };
  }
}

class MaxRankExpression extends RankExpression {
  constructor(private readonly ranks: RankExpression[]) {
    super();
  }

  public static create(ranks: RankExpression[]): RankExpression {
    const flattened: RankExpression[] = [];
    for (const rank of ranks) {
      if (rank instanceof MaxRankExpression) {
        flattened.push(...rank.operands);
      } else {
        flattened.push(rank);
      }
    }
    if (flattened.length === 1) {
      return flattened[0];
    }
    return new MaxRankExpression(flattened);
  }

  public get operands(): RankExpression[] {
    return this.ranks.slice();
  }

  public toJSON(): RankLiteral {
    return { $max: this.ranks.map((rank) => rank.toJSON()) };
  }
}

class MinRankExpression extends RankExpression {
  constructor(private readonly ranks: RankExpression[]) {
    super();
  }

  public static create(ranks: RankExpression[]): RankExpression {
    const flattened: RankExpression[] = [];
    for (const rank of ranks) {
      if (rank instanceof MinRankExpression) {
        flattened.push(...rank.operands);
      } else {
        flattened.push(rank);
      }
    }
    if (flattened.length === 1) {
      return flattened[0];
    }
    return new MinRankExpression(flattened);
  }

  public get operands(): RankExpression[] {
    return this.ranks.slice();
  }

  public toJSON(): RankLiteral {
    return { $min: this.ranks.map((rank) => rank.toJSON()) };
  }
}

class KnnRankExpression extends RankExpression {
  constructor(private readonly config: KnnOptionsNormalized) {
    super();
  }

  public toJSON(): RankLiteral {
    const base: Record<string, unknown> = {
      query: this.config.query,
      key: this.config.key,
      limit: this.config.limit,
    };

    if (this.config.defaultValue !== undefined) {
      base.default = this.config.defaultValue;
    }

    if (this.config.returnRank) {
      base.return_rank = true;
    }

    return { $knn: base };
  }
}

interface KnnOptionsNormalized {
  query: number[] | SparseVector;
  key: string;
  limit: number;
  defaultValue?: number;
  returnRank: boolean;
}

export interface KnnOptions {
  query: IterableInput<number> | SparseVector;
  key?: string | Key;
  limit?: number;
  default?: number | null;
  returnRank?: boolean;
}

const normalizeDenseVector = (vector: IterableInput<number>): number[] => {
  if (Array.isArray(vector)) {
    return vector.slice();
  }
  return Array.from(vector as Iterable<number>, (value) => {
    if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
      throw new TypeError("Dense query vector values must be finite numbers");
    }
    return value;
  });
};

const normalizeKnnOptions = (options: KnnOptions): KnnOptionsNormalized => {
  const limit = options.limit ?? 128;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new TypeError("Knn limit must be a positive integer");
  }

  const maybeSparse = options.query as SparseVector;
  const isSparse =
    isPlainObject(maybeSparse) &&
    Array.isArray(maybeSparse.indices) &&
    Array.isArray(maybeSparse.values);

  const query: number[] | SparseVector = isSparse
    ? {
        indices: maybeSparse.indices.slice(),
        values: maybeSparse.values.slice(),
      }
    : normalizeDenseVector(options.query as IterableInput<number>);

  const key = options.key instanceof Key ? options.key.name : options.key ?? "#embedding";
  if (typeof key !== "string") {
    throw new TypeError("Knn key must be a string or Key instance");
  }

  const defaultValue =
    options.default === null || options.default === undefined
      ? undefined
      : requireNumber(options.default, "Knn default must be a number");

  if (defaultValue !== undefined && !Number.isFinite(defaultValue)) {
    throw new TypeError("Knn default must be a finite number");
  }

  return {
    query: Array.isArray(query) ? query : deepClone(query),
    key,
    limit,
    defaultValue,
    returnRank: options.returnRank ?? false,
  };
};

const requireRank = (input: RankInput, context: string): RankExpression => {
  const result = RankExpression.from(input);
  if (!result) {
    throw new TypeError(`${context} must be a rank expression`);
  }
  return result;
};

export const Val = (value: number): RankExpression =>
  new ValueRankExpression(requireNumber(value, "Val requires a numeric value"));

export const Knn = (options: KnnOptions): RankExpression =>
  new KnnRankExpression(normalizeKnnOptions(options));

export interface RrfOptions {
  ranks: RankInput[];
  k?: number;
  weights?: number[];
  normalize?: boolean;
}

export const Rrf = ({ ranks, k = 60, weights, normalize = false }: RrfOptions): RankExpression => {
  if (!Number.isInteger(k) || k <= 0) {
    throw new TypeError("Rrf k must be a positive integer");
  }
  if (!Array.isArray(ranks) || ranks.length === 0) {
    throw new TypeError("Rrf requires at least one rank expression");
  }

  const expressions = ranks.map((rank, index) => requireRank(rank, `ranks[${index}]`));

  let weightValues = weights ? weights.slice() : new Array(expressions.length).fill(1);
  if (weightValues.length !== expressions.length) {
    throw new Error("Number of weights must match number of ranks");
  }
  if (weightValues.some((value) => typeof value !== "number" || value < 0)) {
    throw new TypeError("Weights must be non-negative numbers");
  }

  if (normalize) {
    const total = weightValues.reduce((sum, value) => sum + value, 0);
    if (total <= 0) {
      throw new Error("Weights must sum to a positive value when normalize=true");
    }
    weightValues = weightValues.map((value) => value / total);
  }

  const terms = expressions.map((rank, index) => {
    const weight = weightValues[index];
    const numerator = Val(weight);
    const denominator = rank.add(k);
    return numerator.divide(denominator);
  });

  const fused = terms.reduce((acc, term) => acc.add(term));
  return fused.negate();
};

export const Sum = (...inputs: RankInput[]): RankExpression => {
  if (inputs.length === 0) {
    throw new Error("Sum requires at least one rank expression");
  }
  const expressions = inputs.map((rank, index) => requireRank(rank, `Sum operand ${index}`));
  return SumRankExpression.create(expressions);
};

export const Sub = (left: RankInput, right: RankInput): RankExpression =>
  new SubRankExpression(requireRank(left, "Sub left"), requireRank(right, "Sub right"));

export const Mul = (...inputs: RankInput[]): RankExpression => {
  if (inputs.length === 0) {
    throw new Error("Mul requires at least one rank expression");
  }
  const expressions = inputs.map((rank, index) => requireRank(rank, `Mul operand ${index}`));
  return MulRankExpression.create(expressions);
};

export const Div = (left: RankInput, right: RankInput): RankExpression =>
  new DivRankExpression(requireRank(left, "Div left"), requireRank(right, "Div right"));

export const Abs = (input: RankInput): RankExpression =>
  requireRank(input, "Abs").abs();

export const Exp = (input: RankInput): RankExpression =>
  requireRank(input, "Exp").exp();

export const Log = (input: RankInput): RankExpression =>
  requireRank(input, "Log").log();

export const Max = (...inputs: RankInput[]): RankExpression => {
  if (inputs.length === 0) {
    throw new Error("Max requires at least one rank expression");
  }
  const expressions = inputs.map((rank, index) => requireRank(rank, `Max operand ${index}`));
  return MaxRankExpression.create(expressions);
};

export const Min = (...inputs: RankInput[]): RankExpression => {
  if (inputs.length === 0) {
    throw new Error("Min requires at least one rank expression");
  }
  const expressions = inputs.map((rank, index) => requireRank(rank, `Min operand ${index}`));
  return MinRankExpression.create(expressions);
};

// -----------------------------------------------------------------------------
// Search builder
// -----------------------------------------------------------------------------

export interface SearchInit {
  where?: WhereInput;
  rank?: RankInput;
  limit?: LimitInput;
  select?: SelectInput;
}

interface SearchParts {
  where?: WhereExpression;
  rank?: RankExpression;
  limit: Limit;
  select: Select;
}

export class Search {
  private _where?: WhereExpression;
  private _rank?: RankExpression;
  private _limit: Limit;
  private _select: Select;

  constructor(init: SearchInit = {}) {
    this._where = init.where ? WhereExpression.from(init.where) : undefined;
    this._rank = init.rank ? RankExpression.from(init.rank) : undefined;
    this._limit = Limit.from(init.limit ?? undefined);
    this._select = Select.from(init.select ?? undefined);
  }

  private clone(overrides: Partial<SearchParts>): Search {
    const next = Object.create(Search.prototype) as Search;
    next._where = overrides.where ?? this._where;
    next._rank = overrides.rank ?? this._rank;
    next._limit = overrides.limit ?? this._limit;
    next._select = overrides.select ?? this._select;
    return next;
  }

  public where(where?: WhereInput): Search {
    return this.clone({ where: WhereExpression.from(where) });
  }

  public rank(rank?: RankInput): Search {
    return this.clone({ rank: RankExpression.from(rank ?? undefined) });
  }

  public limit(limit?: LimitInput, offset?: number): Search {
    if (typeof limit === "number") {
      return this.clone({ limit: Limit.from(limit, offset) });
    }
    return this.clone({ limit: Limit.from(limit ?? undefined) });
  }

  public select(keys?: SelectInput): Search;
  public select(...keys: SelectKeyInput[]): Search;
  public select(
    first?: SelectInput | SelectKeyInput,
    ...rest: SelectKeyInput[]
  ): Search {
    if (Array.isArray(first) || first instanceof Set) {
      return this.clone({ select: Select.from(first as Iterable<SelectKeyInput>) });
    }

    if (first instanceof Select) {
      return this.clone({ select: Select.from(first) });
    }

    if (typeof first === "object" && first !== null && "keys" in first) {
      return this.clone({ select: Select.from(first as SelectInput) });
    }

    const allKeys: SelectKeyInput[] = [];
    if (first !== undefined) {
      allKeys.push(first as SelectKeyInput);
    }
    if (rest.length) {
      allKeys.push(...rest);
    }

    return this.clone({ select: Select.from(allKeys) });
  }

  public selectAll(): Search {
    return this.clone({ select: Select.all() });
  }

  public get whereClause(): WhereExpression | undefined {
    return this._where;
  }

  public get rankExpression(): RankExpression | undefined {
    return this._rank;
  }

  public get limitConfig(): Limit {
    return this._limit;
  }

  public get selectConfig(): Select {
    return this._select;
  }

  public toPayload(): SearchPayload {
    const payload: SearchPayload = {
      limit: this._limit.toJSON(),
      select: this._select.toJSON(),
    };

    if (this._where) {
      payload.filter = { where_clause: this._where.toJSON() };
    }

    if (this._rank) {
      payload.rank = this._rank.toJSON();
    }

    return payload;
  }
}

export type SearchLike = Search | SearchInit;

export const toSearch = (input: SearchLike): Search =>
  input instanceof Search ? input : new Search(input);

// -----------------------------------------------------------------------------
// Search result helper
// -----------------------------------------------------------------------------

export interface SearchResultRow {
  id: string;
  document?: string | null;
  embedding?: number[] | null;
  metadata?: HashMap | null;
  score?: number | null;
}

const normalizePayloadArray = <T>(
  payload: Array<T[] | null> | null | undefined,
  count: number,
): Array<T[] | null> => {
  if (!payload) {
    return Array(count).fill(null);
  }
  if (payload.length === count) {
    return payload.map((item) => (item ? item.slice() : null));
  }
  const result: Array<T[] | null> = payload.map((item) => (item ? item.slice() : null));
  while (result.length < count) {
    result.push(null);
  }
  return result;
};

export class SearchResult {
  public readonly ids: string[][];
  public readonly documents: Array<Array<string | null> | null>;
  public readonly embeddings: Array<Array<Array<number> | null> | null>;
  public readonly metadatas: Array<Array<HashMap | null> | null>;
  public readonly scores: Array<Array<number | null> | null>;
  public readonly select: SearchResponse["select"];

  constructor(response: SearchResponse) {
    this.ids = response.ids;
    const payloadCount = this.ids.length;
    this.documents = normalizePayloadArray(response.documents, payloadCount);
    this.embeddings = normalizePayloadArray(response.embeddings, payloadCount);
    this.metadatas = normalizePayloadArray(response.metadatas, payloadCount);
    this.scores = normalizePayloadArray(response.scores, payloadCount);
    this.select = response.select ?? [];
  }

  public rows(): SearchResultRow[][] {
    const results: SearchResultRow[][] = [];

    for (let payloadIndex = 0; payloadIndex < this.ids.length; payloadIndex += 1) {
      const ids = this.ids[payloadIndex];
      const docPayload = this.documents[payloadIndex] ?? [];
      const embedPayload = this.embeddings[payloadIndex] ?? [];
      const metaPayload = this.metadatas[payloadIndex] ?? [];
      const scorePayload = this.scores[payloadIndex] ?? [];

      const rows: SearchResultRow[] = ids.map((id, rowIndex) => {
        const row: SearchResultRow = { id };

        const document = docPayload[rowIndex];
        if (document !== undefined && document !== null) {
          row.document = document;
        }

        const embedding = embedPayload[rowIndex];
        if (embedding !== undefined && embedding !== null) {
          row.embedding = embedding;
        }

        const metadata = metaPayload[rowIndex];
        if (metadata !== undefined && metadata !== null) {
          row.metadata = metadata;
        }

        const score = scorePayload[rowIndex];
        if (score !== undefined && score !== null) {
          row.score = score;
        }

        return row;
      });

      results.push(rows);
    }

    return results;
  }
}
