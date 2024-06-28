import { Api } from "./generated";
import Count200Response = Api.Count200Response;
import { AdminClient } from "./AdminClient";
import {
  AddDocumentsParams,
  BaseDocumentOperationParams,
  Embedding,
  MultiAddDocumentOperationParams,
  MultiDocumentOperationParams,
  SingleAddDocumentOperationParams,
} from "./types";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";

// a function to convert a non-Array object to an Array
export function toArray<T>(
  obj: T | T[],
  isSingular: (el: T | T[]) => boolean = (el) => !Array.isArray(el),
): Array<T> {
  if (isSingular(obj)) {
    return [obj as T];
  } else {
    return obj as T[];
  }
}

// a function to convert an array to array of arrays
export function toArrayOfArrays<T>(
  obj: Array<Array<T>> | Array<T>,
): Array<Array<T>> {
  if (Array.isArray(obj[0])) {
    return obj as Array<Array<T>>;
  } else {
    return [obj] as Array<Array<T>>;
  }
}

/**
 * Dynamically imports a specified module, providing a workaround for browser environments.
 * This function is necessary because we dynamically import optional dependencies
 * which can cause issues with bundlers that detect the import and throw an error
 * on build time when the dependency is not installed.
 * Using this workaround, the dynamic import is only evaluated on runtime
 * where we work with try-catch when importing optional dependencies.
 *
 * @param {string} moduleName - Specifies the module to import.
 * @returns {Promise<any>} Returns a Promise that resolves to the imported module.
 */
export async function importOptionalModule(moduleName: string) {
  return Function(`return import("${moduleName}")`)();
}

export async function validateTenantDatabase(
  adminClient: AdminClient,
  tenant: string,
  database: string,
): Promise<void> {
  try {
    await adminClient.getTenant({ name: tenant });
  } catch (error) {
    throw new Error(
      `Error: ${error}, Could not connect to tenant ${tenant}. Are you sure it exists?`,
    );
  }

  try {
    await adminClient.getDatabase({ name: database, tenantName: tenant });
  } catch (error) {
    throw new Error(
      `Error: ${error}, Could not connect to database ${database} for tenant ${tenant}. Are you sure it exists?`,
    );
  }
}

export function isBrowser() {
  return (
    typeof window !== "undefined" && typeof window.document !== "undefined"
  );
}

function isMultiDocumentParams(
  arg: unknown,
): arg is MultiDocumentOperationParams {
  if (!arg || typeof arg !== "object") {
    return false;
  }
  return Array.isArray((arg as MultiDocumentOperationParams).ids);
}

function singleToMultiDocumentParams(
  params: SingleAddDocumentOperationParams,
): MultiAddDocumentOperationParams {
  if ("embedding" in params) {
    return {
      ids: [params.id],
      embeddings: [params.embedding!],
      metadatas: params.metadata ? [params.metadata] : undefined,
      documents: params.document ? [params.document] : undefined,
    };
  }

  return {
    ids: [params.id],
    embeddings: params.embedding ? [params.embedding] : undefined,
    metadatas: params.metadata ? [params.metadata] : undefined,
    documents: [params.document],
  };
}

export async function prepareDocumentRequest(
  reqParams: AddDocumentsParams,
  embeddingFunction: IEmbeddingFunction,
): Promise<MultiDocumentOperationParams> {
  const { ids, embeddings, metadatas, documents } = isMultiDocumentParams(
    reqParams,
  )
    ? reqParams
    : singleToMultiDocumentParams(reqParams);

  if (!embeddings && !documents) {
    throw new Error("embeddings and documents cannot both be undefined");
  }

  const embeddingsArray = embeddings
    ? embeddings
    : documents
    ? await embeddingFunction.generate(documents)
    : undefined;

  if (!embeddingsArray) {
    throw new Error("Wasn't able to generate embeddings for your request.");
  }

  for (let i = 0; i < ids.length; i += 1) {
    if (typeof ids[i] !== "string") {
      throw new Error(
        `Expected ids to be strings, found ${typeof ids[i]} at index ${i}`,
      );
    }
  }

  if (
    (embeddingsArray !== undefined && ids.length !== embeddingsArray.length) ||
    (metadatas !== undefined && ids.length !== metadatas.length) ||
    (documents !== undefined && ids.length !== documents.length)
  ) {
    throw new Error(
      "ids, embeddings, metadatas, and documents must all be the same length",
    );
  }

  const uniqueIds = new Set(ids);
  if (uniqueIds.size !== ids.length) {
    const duplicateIds = ids.filter(
      (item, index) => ids.indexOf(item) !== index,
    );
    throw new Error(
      `ID's must be unique, found duplicates for: ${duplicateIds}`,
    );
  }

  return {
    ids,
    metadatas,
    documents,
    embeddings: embeddingsArray,
  };
}

// we allow users to supply a query as:
//  - a string,
//  - an array of strings,
//  - an embedding (which is an array of numbers),
//  - an array of embeddings.
//
// This function turns that into an array of strings or an array of embeddings
export function toQueryArray(
  query: string | string[] | number[] | number[][],
): [string[] | number[][], boolean] {
  if (typeof query === "string") {
    return [[query], true];
  }
  const element = query[0];
  if (typeof element === "string") {
    return [query as string[], false];
  }
  if (typeof element === "number") {
    return [[query] as number[][], true];
  }
  return [query as number[][], false];
}
