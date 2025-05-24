import type {
  JSONSchema4,
  JSONSchema4Object,
  JSONSchema6,
  JSONSchema6Object,
  JSONSchema7,
  JSONSchema7Object,
} from "json-schema";

export type JSONSchema = JSONSchema4 | JSONSchema6 | JSONSchema7;
export type JSONSchemaObject = JSONSchema4Object | JSONSchema6Object | JSONSchema7Object;

export interface Plugin {
  /**
   * Can this parser be used to process this file?
   */
  canHandle: (file: FileInfo) => boolean;
  /**
   * This is where the real work of a parser happens. The `parse` method accepts the same file info object as the `canHandle` function, but rather than returning a boolean value, the `parse` method should return a JavaScript representation of the file contents.  For our CSV parser, that is a two-dimensional array of lines and values.  For your parser, it might be an object, a string, a custom class, or anything else.
   *
   * Unlike the `canHandle` function, the `parse` method can also be asynchronous. This might be important if your parser needs to retrieve data from a database or if it relies on an external HTTP service to return the parsed value.  You can return your asynchronous value via a [Promise](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise) or a Node.js-style error-first callback.  Here are examples of both approaches:
   */
  handler: (file: FileInfo) => string | Buffer | JSONSchema | Promise<{ data: Buffer }> | Promise<string | Buffer | JSONSchema>;
  name: 'binary' | 'file' | 'http' | 'json' | 'text' | 'yaml';
}

/**
 * JSON Schema `$Ref` Parser supports plug-ins, such as resolvers and parsers. These plug-ins can have methods such as `canHandle()`, `read()`, `canHandle()`, and `parse()`. All of these methods accept the same object as their parameter: an object containing information about the file being read or parsed.
 *
 * The file info object currently only consists of a few properties, but it may grow in the future if plug-ins end up needing more information.
 *
 * See https://apitools.dev/json-schema-ref-parser/docs/plugins/file-info-object.html
 */
export interface FileInfo {
  /**
   * The raw file contents, in whatever form they were returned by the resolver that read the file.
   */
  data: string | Buffer;
  /**
   * The lowercase file extension, such as ".json", ".yaml", ".txt", etc.
   */
  extension: string;
  /**
   * The hash (URL fragment) of the file URL, including the # symbol. If the URL doesn't have a hash, then this will be an empty string.
   */
  hash: string;
  /**
   * The full URL of the file. This could be any type of URL, including "http://", "https://", "file://", "ftp://", "mongodb://", or even a local filesystem path (when running in Node.js).
   */
  url: string;
}
