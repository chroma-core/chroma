import { Api } from "./generated";
import Count200Response = Api.Count200Response;
import { AdminClient } from "./AdminClient";
import {
  AddDocumentsParams,
  Embedding,
  MultiDocumentOperationParams,
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

// we need to override constructors to make it work with jest
// https://stackoverflow.com/questions/76007003/jest-tobeinstanceof-expected-constructor-array-received-constructor-array
export function repack(value: unknown): any {
  if (Boolean(value) && typeof value === "object") {
    if (Array.isArray(value)) {
      return new Array(...value);
    } else {
      return { ...value };
    }
  } else {
    return value;
  }
}

export async function handleSuccess(
  response: Response | string | Count200Response,
) {
  switch (true) {
    case response instanceof Response:
      return repack(await (response as Response).json());
    case typeof response === "string":
      return repack(response as string); // currently version is the only thing that return non-JSON
    default:
      return repack(response);
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

export async function prepareDocumentRequest(
  reqParams: AddDocumentsParams,
  embeddingFunction: IEmbeddingFunction,
): Promise<AddDocumentsParams> {
  if (!reqParams?.embeddings && !reqParams?.documents) {
    throw new Error("embeddings and documents cannot both be undefined");
  }

  const embeddingsArray = reqParams.embeddings
    ? toArrayOfArrays<number>(reqParams.embeddings)
    : reqParams.documents
    ? await embeddingFunction.generate(toArray(reqParams.documents))
    : undefined;

  if (!embeddingsArray) {
    throw new Error("Wasn't able to generate embeddings for your request.");
  }

  const idsArray = toArray(reqParams.ids);
  const metadatasArray = reqParams.metadatas
    ? toArray(reqParams.metadatas)
    : undefined;
  const documentsArray = reqParams.documents
    ? toArray(reqParams.documents)
    : undefined;

  for (let i = 0; i < idsArray.length; i += 1) {
    if (typeof idsArray[i] !== "string") {
      throw new Error(
        `Expected ids to be strings, found ${typeof idsArray[i]} at index ${i}`,
      );
    }
  }

  if (
    (embeddingsArray !== undefined &&
      idsArray.length !== embeddingsArray.length) ||
    (metadatasArray !== undefined &&
      idsArray.length !== metadatasArray.length) ||
    (documentsArray !== undefined && idsArray.length !== documentsArray.length)
  ) {
    throw new Error(
      "ids, embeddings, metadatas, and documents must all be the same length",
    );
  }

  const uniqueIds = new Set(idsArray);
  if (uniqueIds.size !== idsArray.length) {
    const duplicateIds = idsArray.filter(
      (item, index) => idsArray.indexOf(item) !== index,
    );
    throw new Error(
      `ID's must be unique, found duplicates for: ${duplicateIds}`,
    );
  }

  return {
    ...reqParams,
    ids: idsArray,
    metadatas: metadatasArray,
    documents: documentsArray,
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
