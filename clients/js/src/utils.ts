import { Api } from "./generated";
import Count200Response = Api.Count200Response;
import { AdminClient } from "./AdminClient";

// a function to convert a non-Array object to an Array
export function toArray<T>(obj: T | Array<T>): Array<T> {
  if (Array.isArray(obj)) {
    return obj;
  } else {
    return [obj];
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
