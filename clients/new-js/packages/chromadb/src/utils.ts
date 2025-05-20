import { AdminClientArgs } from "./admin-client";
import { ChromaClientArgs } from "./chroma-client";
import {
  BaseRecordSet,
  baseRecordSetFields,
  IncludeEnum,
  Metadata,
  RecordSet,
  recordSetFields,
  Where,
  WhereDocument,
} from "./types";
import { Include } from "./api";
import { a, b } from "@hey-api/openapi-ts/dist/types.d-C5lgdIHG";
import { ChromaValueError } from "./errors";

export const DEFAULT_TENANT = "default_tenant";
export const DEFAULT_DATABASE = "default_database";

export const defaultAdminClientArgs: AdminClientArgs = {
  host: "localhost",
  port: 8000,
  ssl: false,
};

export const defaultChromaClientArgs: ChromaClientArgs = {
  ...defaultAdminClientArgs,
  tenant: DEFAULT_TENANT,
  database: DEFAULT_DATABASE,
};

export type HttpMethod =
  | "GET"
  | "POST"
  | "PUT"
  | "DELETE"
  | "HEAD"
  | "CONNECT"
  | "OPTIONS"
  | "PATCH"
  | "TRACE"
  | undefined;

export const normalizeMethod = (method?: string): HttpMethod => {
  if (method) {
    switch (method.toUpperCase()) {
      case "GET":
        return "GET";
      case "POST":
        return "POST";
      case "PUT":
        return "PUT";
      case "DELETE":
        return "DELETE";
      case "HEAD":
        return "HEAD";
      case "CONNECT":
        return "CONNECT";
      case "OPTIONS":
        return "OPTIONS";
      case "PATCH":
        return "PATCH";
      case "TRACE":
        return "TRACE";
      default:
        return undefined;
    }
  }
  return undefined;
};

export const validateRecordSetLengthConsistency = (recordSet: RecordSet) => {
  const lengths: [string, number][] = Object.entries(recordSet)
    .filter(
      ([field, value]) =>
        recordSetFields.includes(field) && value !== undefined,
    )
    .map(([field, value]) => [field, value.length]);

  if (lengths.length === 0) {
    throw new ChromaValueError(
      `At least one of ${recordSetFields.join(", ")} must be provided`,
    );
  }

  const zeroLength = lengths
    .filter(([_, length]) => length === 0)
    .map(([field, _]) => field);
  if (zeroLength.length > 0) {
    throw new ChromaValueError(
      `Non-empty lists are required for ${zeroLength.join(", ")}`,
    );
  }

  if (new Set(lengths.map(([_, length]) => length)).size > 1) {
    throw new ChromaValueError(
      `Unequal lengths for fields ${lengths
        .map(([field, _]) => field)
        .join(", ")}`,
    );
  }
};

const validateEmbeddings = ({
  embeddings,
  fieldName = "embeddings",
}: {
  embeddings: number[][];
  fieldName: string;
}) => {
  if (!Array.isArray(embeddings)) {
    throw new ChromaValueError(
      `Expected '${fieldName}' to be an array, but got ${typeof embeddings}`,
    );
  }

  if (embeddings.length === 0) {
    throw new ChromaValueError(
      "Expected embeddings to be an array with at least one item",
    );
  }

  if (!embeddings.filter((e) => e.every((n: any) => typeof n === "number"))) {
    throw new ChromaValueError(
      "Expected each embedding to be an array of numbers",
    );
  }

  embeddings.forEach((embedding, i) => {
    if (embedding.length === 0) {
      throw new ChromaValueError(
        `Expected each embedding to be a non-empty array of numbers, but got an empty array at index ${i}`,
      );
    }
  });
};

const validateDocuments = ({
  documents,
  nullable = false,
  fieldName = "documents",
}: {
  documents: (string | null | undefined)[];
  fieldName: string;
  nullable?: boolean;
}) => {
  if (!Array.isArray(documents)) {
    throw new ChromaValueError(
      `Expected '${fieldName}' to be an array, but got ${typeof documents}`,
    );
  }

  if (documents.length === 0) {
    throw new ChromaValueError(
      `Expected '${fieldName}' to be a non-empty list`,
    );
  }

  documents.forEach((document) => {
    if (!nullable && typeof document !== "string" && !document) {
      throw new ChromaValueError(
        `Expected each document to be a string, but got ${typeof document}`,
      );
    }
  });
};

export const validateIDs = (ids: string[]) => {
  if (!Array.isArray(ids)) {
    throw new ChromaValueError(
      `Expected 'ids' to be an array, but got ${typeof ids}`,
    );
  }

  if (ids.length === 0) {
    throw new ChromaValueError("Expected 'ids' to be a non-empty list");
  }

  const nonStrings = ids
    .map((id, i) => [id, i] as [any, number])
    .filter(([id, _]) => typeof id !== "string")
    .map(([_, i]) => i);

  if (nonStrings.length > 0) {
    throw new ChromaValueError(
      `Found non-string IDs at ${nonStrings.join(", ")}`,
    );
  }

  const seen = new Set();
  const duplicates = ids.filter((id) => {
    if (seen.has(id)) {
      return id;
    }
    seen.add(id);
  });
  let message = "Expected IDs to be unique, but found duplicates of";
  if (duplicates.length > 0 && duplicates.length <= 5) {
    throw new ChromaValueError(`${message} ${duplicates.join(", ")}`);
  }
  if (duplicates.length > 0) {
    throw new ChromaValueError(
      `${message} ${duplicates.slice(0, 5).join(", ")}, ..., ${duplicates
        .slice(duplicates.length - 5)
        .join(", ")}`,
    );
  }
};

export const validateMetadata = (metadata?: Metadata) => {
  if (!metadata) {
    return;
  }

  if (Object.keys(metadata).length === 0) {
    throw new ChromaValueError("Expected metadata to be non-empty");
  }

  if (
    !Object.values(metadata).every(
      (v: any) =>
        v === null ||
        v === undefined ||
        typeof v === "string" ||
        typeof v === "number" ||
        typeof v === "boolean",
    )
  ) {
    throw new ChromaValueError(
      "Expected metadata to be a string, number, boolean, or nullable",
    );
  }
};

const validateMetadatas = (metadatas: Metadata[]) => {
  if (!Array.isArray(metadatas)) {
    throw new ChromaValueError(
      `Expected metadatas to be an array, but got ${typeof metadatas}`,
    );
  }

  metadatas.forEach((metadata) => validateMetadata(metadata));
};

export const validateBaseRecordSet = ({
  recordSet,
  update = false,
  embeddingsField = "embeddings",
  documentsField = "documents",
}: {
  recordSet: BaseRecordSet;
  update?: boolean;
  embeddingsField?: string;
  documentsField?: string;
}) => {
  if (!recordSet.embeddings && !recordSet.documents && !update) {
    throw new ChromaValueError(
      `At least one of '${embeddingsField}' and '${documentsField}' must be provided`,
    );
  }

  if (recordSet.embeddings) {
    validateEmbeddings({
      embeddings: recordSet.embeddings,
      fieldName: embeddingsField,
    });
  }

  if (recordSet.documents) {
    validateDocuments({
      documents: recordSet.documents,
      fieldName: documentsField,
    });
  }

  if (recordSet.metadatas) {
    validateMetadatas(recordSet.metadatas);
  }
};

export const validateWhere = (where: Where) => {
  if (typeof where !== "object") {
    throw new ChromaValueError("Expected where to be a non-empty object");
  }

  if (Object.keys(where).length != 1) {
    throw new ChromaValueError(
      `Expected 'where' to have exactly one operator, but got ${
        Object.keys(where).length
      }`,
    );
  }

  Object.entries(where).forEach(([key, value]) => {
    if (
      key !== "$and" &&
      key !== "$or" &&
      key !== "$in" &&
      key !== "$nin" &&
      !["string", "number", "boolean", "object"].includes(typeof value)
    ) {
      throw new ChromaValueError(
        `Expected 'where' value to be a string, number, boolean, or an operator expression, but got ${value}`,
      );
    }

    if (key === "$and" || key === "$or") {
      if (Object.keys(value).length <= 1) {
        throw new ChromaValueError(
          `Expected 'where' value for $and or $or to be a list of 'where' expressions, but got ${value}`,
        );
      }

      Object.values(where).forEach((w: Where) => validateWhere(w));
    }

    if (typeof value === "object") {
      if (Object.keys(value).length != 1) {
        throw new ChromaValueError(
          `Expected operator expression to have one operator, but got ${value}`,
        );
      }

      const [operator, operand] = Object.entries(value)[0];

      if (
        ["$gt", "$gte", "$lt", "$lte"].includes(operator) &&
        typeof operand !== "number"
      ) {
        throw new ChromaValueError(
          `Expected operand value to be a number for ${operator}, but got ${typeof operand}`,
        );
      }

      if (["$in", "$nin"].includes(operator) && !Array.isArray(operand)) {
        throw new ChromaValueError(
          `Expected operand value to be an array for ${operator}, but got ${operand}`,
        );
      }

      if (
        !["$gt", "$gte", "$lt", "$lte", "$ne", "$eq", "$in", "$nin"].includes(
          operator,
        )
      ) {
        throw new ChromaValueError(
          `Expected operator to be one of $gt, $gte, $lt, $lte, $ne, $eq, $in, $nin, but got ${operator}`,
        );
      }

      if (
        !["string", "number", "boolean"].includes(typeof operand) &&
        !Array.isArray(operand)
      ) {
        throw new ChromaValueError(
          "Expected operand value to be a string, number, boolean, or a list of those types",
        );
      }

      if (
        Array.isArray(operand) &&
        (operand.length === 0 ||
          !operand.every((item) => typeof item !== typeof operand[0]))
      ) {
        throw new ChromaValueError(
          "Expected 'where' operand value to be a non-empty list and all values to be of the same type",
        );
      }
    }
  });
};

export const validateWhereDocument = (whereDocument: WhereDocument) => {
  if (typeof whereDocument !== "object") {
    throw new ChromaValueError(
      "Expected 'whereDocument' to be a non-empty object",
    );
  }

  if (Object.keys(whereDocument).length != 1) {
    throw new ChromaValueError(
      `Expected 'whereDocument' to have exactly one operator, but got ${whereDocument}`,
    );
  }

  const [operator, operand] = Object.entries(whereDocument)[0];
  if (
    ![
      "$contains",
      "$not_contains",
      "$matches",
      "$not_matches",
      "$and",
      "$or",
    ].includes(operator)
  ) {
    throw new ChromaValueError(
      `Expected 'whereDocument' operator to be one of $contains, $not_contains, $matches, $not_matches, $and, or $or, but got ${operator}`,
    );
  }

  if (operator === "$and" || operator === "$or") {
    if (!Array.isArray(operand)) {
      throw new ChromaValueError(
        `Expected operand for ${operator} to be a list of 'whereDocument' expressions, but got ${operand}`,
      );
    }

    if (operand.length <= 1) {
      throw new ChromaValueError(
        `Expected 'whereDocument' operand for ${operator} to be a list with at least two 'whereDocument' expressions`,
      );
    }

    operand.forEach((item) => validateWhereDocument(item));
  }

  if (
    (operand === "$contains" || operand === "$not_contains") &&
    (typeof (operator as any) !== "string" || operator.length === 0)
  ) {
    throw new ChromaValueError(
      `Expected operand for ${operator} to be a non empty string, but got ${operand}`,
    );
  }
};

export const validateInclude = ({
  include,
  exclude,
}: {
  include: Include[];
  exclude?: Include[];
}) => {
  if (!Array.isArray(include)) {
    throw new ChromaValueError("Expected 'include' to be a non-empty array");
  }

  const validValues = Object.keys(IncludeEnum);
  include.forEach((item) => {
    if (typeof (item as any) !== "string") {
      throw new ChromaValueError("Expected 'include' items to be strings");
    }

    if (!validValues.includes(item)) {
      throw new ChromaValueError(
        `Expected 'include' items to be one of ${validValues.join(
          ", ",
        )}, but got ${item}`,
      );
    }

    if (exclude?.includes(item)) {
      throw new ChromaValueError(`${item} is not allowed for this operation`);
    }
  });
};

export const validateNResults = (nResults: number) => {
  if (typeof (nResults as any) !== "number") {
    throw new ChromaValueError(
      `Expected 'nResults' to be a number, but got ${typeof nResults}`,
    );
  }

  if (nResults <= 0) {
    throw new ChromaValueError("Number of requested results has to positive");
  }
};
