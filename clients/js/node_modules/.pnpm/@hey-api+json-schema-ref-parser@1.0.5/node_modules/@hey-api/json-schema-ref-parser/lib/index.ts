import $Refs from "./refs.js";
import { newFile, parseFile } from "./parse.js";
import { resolveExternal } from "./resolve-external.js";
import { bundle as _bundle } from "./bundle.js";
import _dereference from "./dereference.js";
import * as url from "./util/url.js";
import { isHandledError, JSONParserErrorGroup } from "./util/errors.js";
import { ono } from "@jsdevtools/ono";
import { getJsonSchemaRefParserDefaultOptions } from "./options.js";
import type { JSONSchema } from "./types/index.js";
import { urlResolver } from "./resolvers/url.js";
import { fileResolver } from "./resolvers/file.js";

interface ResolvedInput {
  path: string;
  schema: string | JSONSchema | Buffer | Awaited<JSONSchema> | undefined;
  type: 'file' | 'json' | 'url';
}

export const getResolvedInput = ({
  pathOrUrlOrSchema,
}: {
  pathOrUrlOrSchema: JSONSchema | string | unknown;
}): ResolvedInput => {
  if (!pathOrUrlOrSchema) {
    throw ono(`Expected a file path, URL, or object. Got ${pathOrUrlOrSchema}`);
  }

  const resolvedInput: ResolvedInput = {
    path: typeof pathOrUrlOrSchema === 'string' ? pathOrUrlOrSchema : '',
    schema: undefined,
    type: 'url',
  }

  // If the path is a filesystem path, then convert it to a URL.
  // NOTE: According to the JSON Reference spec, these should already be URLs,
  // but, in practice, many people use local filesystem paths instead.
  // So we're being generous here and doing the conversion automatically.
  // This is not intended to be a 100% bulletproof solution.
  // If it doesn't work for your use-case, then use a URL instead.
  if (resolvedInput.path && url.isFileSystemPath(resolvedInput.path)) {
    resolvedInput.path = url.fromFileSystemPath(resolvedInput.path);
    resolvedInput.type = 'file';
  } else if (!resolvedInput.path && pathOrUrlOrSchema && typeof pathOrUrlOrSchema === 'object') {
    if ("$id" in pathOrUrlOrSchema && pathOrUrlOrSchema.$id) {
      // when schema id has defined an URL should use that hostname to request the references,
      // instead of using the current page URL
      const { hostname, protocol } = new URL(pathOrUrlOrSchema.$id as string);
      resolvedInput.path = `${protocol}//${hostname}:${protocol === "https:" ? 443 : 80}`;
      resolvedInput.type = 'url';
    } else {
      resolvedInput.schema = pathOrUrlOrSchema;
      resolvedInput.type = 'json';
    }
  }

  if (resolvedInput.type !== 'json') {
    // resolve the absolute path of the schema
    resolvedInput.path = url.resolve(url.cwd(), resolvedInput.path);
  }

  return resolvedInput;
}

/**
 * This class parses a JSON schema, builds a map of its JSON references and their resolved values,
 * and provides methods for traversing, manipulating, and dereferencing those references.
 */
export class $RefParser {
  /**
   * The resolved JSON references
   *
   * @type {$Refs}
   * @readonly
   */
  $refs = new $Refs<JSONSchema>();
  public options = getJsonSchemaRefParserDefaultOptions()
  /**
   * The parsed (and possibly dereferenced) JSON schema object
   *
   * @type {object}
   * @readonly
   */
  public schema: JSONSchema | null = null;

  /**
   * Bundles all referenced files/URLs into a single schema that only has internal `$ref` pointers. This lets you split-up your schema however you want while you're building it, but easily combine all those files together when it's time to package or distribute the schema to other people. The resulting schema size will be small, since it will still contain internal JSON references rather than being fully-dereferenced.
   *
   * This also eliminates the risk of circular references, so the schema can be safely serialized using `JSON.stringify()`.
   *
   * See https://apitools.dev/json-schema-ref-parser/docs/ref-parser.html#bundleschema-options-callback
   *
   * @param pathOrUrlOrSchema A JSON Schema object, or the file path or URL of a JSON Schema file.
   */
  public async bundle({
    arrayBuffer,
    fetch,
    pathOrUrlOrSchema,
    resolvedInput,
  }: {
    arrayBuffer?: ArrayBuffer;
    fetch?: RequestInit;
    pathOrUrlOrSchema: JSONSchema | string | unknown;
    resolvedInput?: ResolvedInput;
  }): Promise<JSONSchema> {
    await this.parse({
      arrayBuffer,
      fetch,
      pathOrUrlOrSchema,
      resolvedInput,
    });
    await resolveExternal(this, this.options);
    const errors = JSONParserErrorGroup.getParserErrors(this);
    if (errors.length > 0) {
      throw new JSONParserErrorGroup(this);
    }
    _bundle(this, this.options);
    const errors2 = JSONParserErrorGroup.getParserErrors(this);
    if (errors2.length > 0) {
      throw new JSONParserErrorGroup(this);
    }
    return this.schema!;
  }

  /**
   * Dereferences all `$ref` pointers in the JSON Schema, replacing each reference with its resolved value. This results in a schema object that does not contain any `$ref` pointers. Instead, it's a normal JavaScript object tree that can easily be crawled and used just like any other JavaScript object. This is great for programmatic usage, especially when using tools that don't understand JSON references.
   *
   * The dereference method maintains object reference equality, meaning that all `$ref` pointers that point to the same object will be replaced with references to the same object. Again, this is great for programmatic usage, but it does introduce the risk of circular references, so be careful if you intend to serialize the schema using `JSON.stringify()`. Consider using the bundle method instead, which does not create circular references.
   *
   * See https://apitools.dev/json-schema-ref-parser/docs/ref-parser.html#dereferenceschema-options-callback
   *
   * @param pathOrUrlOrSchema A JSON Schema object, or the file path or URL of a JSON Schema file.
   */
  public async dereference({
    fetch,
    pathOrUrlOrSchema,
  }: {
    fetch?: RequestInit;
    pathOrUrlOrSchema: JSONSchema | string | unknown;
  }): Promise<JSONSchema> {
    await this.parse({
      fetch,
      pathOrUrlOrSchema,
    });
    await resolveExternal(this, this.options);
    const errors = JSONParserErrorGroup.getParserErrors(this);
    if (errors.length > 0) {
      throw new JSONParserErrorGroup(this);
    }
    _dereference(this, this.options);
    const errors2 = JSONParserErrorGroup.getParserErrors(this);
    if (errors2.length > 0) {
      throw new JSONParserErrorGroup(this);
    }
    return this.schema!;
  }

  /**
   * Parses the given JSON schema.
   * This method does not resolve any JSON references.
   * It just reads a single file in JSON or YAML format, and parse it as a JavaScript object.
   *
   * @param pathOrUrlOrSchema A JSON Schema object, or the file path or URL of a JSON Schema file.
   * @returns - The returned promise resolves with the parsed JSON schema object.
   */
  public async parse({
    arrayBuffer,
    fetch,
    pathOrUrlOrSchema,
    resolvedInput: _resolvedInput,
  }: {
    arrayBuffer?: ArrayBuffer;
    fetch?: RequestInit;
    pathOrUrlOrSchema: JSONSchema | string | unknown;
    resolvedInput?: ResolvedInput;
  }): Promise<{ schema: JSONSchema }> {
    const resolvedInput = _resolvedInput || getResolvedInput({ pathOrUrlOrSchema });
    const { path, type } = resolvedInput;
    let { schema } = resolvedInput;

    // reset everything
    this.schema = null;
    this.$refs = new $Refs();

    if (schema) {
      // immediately add a new $Ref with the schema object as value
      const $ref = this.$refs._add(path);
      $ref.pathType = url.isFileSystemPath(path) ? 'file' : 'http';
      $ref.value = schema;
    } else if (type !== 'json') {
      const file = newFile(path)

      // Add a new $Ref for this file, even though we don't have the value yet.
      // This ensures that we don't simultaneously read & parse the same file multiple times
      const $refAdded = this.$refs._add(file.url);
      $refAdded.pathType = type;
      try {
        const resolver = type === 'file' ? fileResolver : urlResolver;
        await resolver.handler({
          arrayBuffer,
          fetch,
          file,
        });
        const parseResult = await parseFile(file, this.options);
        $refAdded.value = parseResult.result;
        schema = parseResult.result;
      } catch (err) {
        if (isHandledError(err)) {
          $refAdded.value = err;
        }
    
        throw err;
      }
    }

    if (schema === null || typeof schema !== 'object' || Buffer.isBuffer(schema)) {
      throw ono.syntax(`"${this.$refs._root$Ref.path || schema}" is not a valid JSON Schema`);
    }

    this.schema = schema;

    return {
      schema,
    };
  }
}

export { sendRequest } from './resolvers/url.js'
export type { JSONSchema } from "./types/index.js";
