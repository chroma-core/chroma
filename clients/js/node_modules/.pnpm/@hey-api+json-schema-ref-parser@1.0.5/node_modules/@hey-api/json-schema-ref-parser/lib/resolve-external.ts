import $Ref from "./ref.js";
import Pointer from "./pointer.js";
import { newFile, parseFile } from "./parse.js";
import * as url from "./util/url.js";
import type $Refs from "./refs.js";
import type { $RefParserOptions } from "./options.js";
import type { JSONSchema } from "./types/index.js";
import { getResolvedInput } from "./index.js";
import type { $RefParser } from "./index.js";
import { isHandledError } from "./util/errors.js";
import { fileResolver } from "./resolvers/file.js";
import { urlResolver } from "./resolvers/url.js";

/**
 * Crawls the JSON schema, finds all external JSON references, and resolves their values.
 * This method does not mutate the JSON schema. The resolved values are added to {@link $RefParser#$refs}.
 *
 * NOTE: We only care about EXTERNAL references here. INTERNAL references are only relevant when dereferencing.
 *
 * @returns
 * The promise resolves once all JSON references in the schema have been resolved,
 * including nested references that are contained in externally-referenced files.
 */
export function resolveExternal(
  parser: $RefParser,
  options: $RefParserOptions,
) {
  try {
    // console.log('Resolving $ref pointers in %s', parser.$refs._root$Ref.path);
    const promises = crawl(parser.schema, parser.$refs._root$Ref.path + "#", parser.$refs, options);
    return Promise.all(promises);
  } catch (e) {
    return Promise.reject(e);
  }
}

/**
 * Recursively crawls the given value, and resolves any external JSON references.
 *
 * @param obj - The value to crawl. If it's not an object or array, it will be ignored.
 * @param path - The full path of `obj`, possibly with a JSON Pointer in the hash
 * @param {boolean} external - Whether `obj` was found in an external document.
 * @param $refs
 * @param options
 * @param seen - Internal.
 *
 * @returns
 * Returns an array of promises. There will be one promise for each JSON reference in `obj`.
 * If `obj` does not contain any JSON references, then the array will be empty.
 * If any of the JSON references point to files that contain additional JSON references,
 * then the corresponding promise will internally reference an array of promises.
 */
function crawl<S extends object = JSONSchema>(
  obj: string | Buffer | S | undefined | null,
  path: string,
  $refs: $Refs<S>,
  options: $RefParserOptions,
  seen?: Set<any>,
  external?: boolean,
) {
  seen ||= new Set();
  let promises: any = [];

  if (obj && typeof obj === "object" && !ArrayBuffer.isView(obj) && !seen.has(obj)) {
    seen.add(obj); // Track previously seen objects to avoid infinite recursion
    if ($Ref.isExternal$Ref(obj)) {
      promises.push(resolve$Ref<S>(obj, path, $refs, options));
    }

    const keys = Object.keys(obj) as string[];
    for (const key of keys) {
      const keyPath = Pointer.join(path, key);
      const value = obj[key as keyof typeof obj] as string | JSONSchema | Buffer | undefined;
      promises = promises.concat(crawl(value, keyPath, $refs, options, seen, external));
    }
  }

  return promises;
}

/**
 * Resolves the given JSON Reference, and then crawls the resulting value.
 *
 * @param $ref - The JSON Reference to resolve
 * @param path - The full path of `$ref`, possibly with a JSON Pointer in the hash
 * @param $refs
 * @param options
 *
 * @returns
 * The promise resolves once all JSON references in the object have been resolved,
 * including nested references that are contained in externally-referenced files.
 */
async function resolve$Ref<S extends object = JSONSchema>(
  $ref: S,
  path: string,
  $refs: $Refs<S>,
  options: $RefParserOptions,
) {
  const resolvedPath = url.resolve(path, ($ref as JSONSchema).$ref!);
  const withoutHash = url.stripHash(resolvedPath);

  // $ref.$ref = url.relative($refs._root$Ref.path, resolvedPath);

  // Do we already have this $ref?
  const ref = $refs._$refs[withoutHash];
  if (ref) {
    // We've already parsed this $ref, so use the existing value
    return Promise.resolve(ref.value);
  }

  // Parse the $referenced file/url
  const file = newFile(resolvedPath)

  // Add a new $Ref for this file, even though we don't have the value yet.
  // This ensures that we don't simultaneously read & parse the same file multiple times
  const $refAdded = $refs._add(file.url);

  try {
    const resolvedInput = getResolvedInput({ pathOrUrlOrSchema: resolvedPath })

    $refAdded.pathType = resolvedInput.type;

    let promises: any = [];

    if (resolvedInput.type !== 'json') {
      const resolver = resolvedInput.type === 'file' ? fileResolver : urlResolver;
      await resolver.handler({ file });
      const parseResult = await parseFile(file, options);
      $refAdded.value = parseResult.result;
      promises = crawl(parseResult.result, `${withoutHash}#`, $refs, options, new Set(), true);
    }

    return Promise.all(promises);
  } catch (err) {
    if (isHandledError(err)) {
      $refAdded.value = err;
    }

    throw err;
  }
}
