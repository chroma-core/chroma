import Pointer from "./pointer.js";
import type { JSONParserError, MissingPointerError, ParserError, ResolverError } from "./util/errors.js";
import { normalizeError } from "./util/errors.js";
import type $Refs from "./refs.js";
import type { ParserOptions } from "./options.js";
import type { JSONSchema } from "./types";

export type $RefError = JSONParserError | ResolverError | ParserError | MissingPointerError;

/**
 * This class represents a single JSON reference and its resolved value.
 *
 * @class
 */
class $Ref<S extends object = JSONSchema> {
  /**
   * The file path or URL of the referenced file.
   * This path is relative to the path of the main JSON schema file.
   *
   * This path does NOT contain document fragments (JSON pointers). It always references an ENTIRE file.
   * Use methods such as {@link $Ref#get}, {@link $Ref#resolve}, and {@link $Ref#exists} to get
   * specific JSON pointers within the file.
   *
   * @type {string}
   */
  path: undefined | string;

  /**
   * The resolved value of the JSON reference.
   * Can be any JSON type, not just objects. Unknown file types are represented as Buffers (byte arrays).
   *
   * @type {?*}
   */
  value: any;

  /**
   * The {@link $Refs} object that contains this {@link $Ref} object.
   *
   * @type {$Refs}
   */
  $refs: $Refs<S>;

  /**
   * Indicates the type of {@link $Ref#path} (e.g. "file", "http", etc.)
   */
  pathType: string | unknown;

  /**
   * List of all errors. Undefined if no errors.
   */
  errors: Array<$RefError> = [];

  constructor($refs: $Refs<S>) {
    this.$refs = $refs;
  }

  /**
   * Pushes an error to errors array.
   *
   * @param err - The error to be pushed
   * @returns
   */
  addError(err: $RefError) {
    if (this.errors === undefined) {
      this.errors = [];
    }

    const existingErrors = this.errors.map(({ footprint }: any) => footprint);

    // the path has been almost certainly set at this point,
    // but just in case something went wrong, normalizeError injects path if necessary
    // moreover, certain errors might point at the same spot, so filter them out to reduce noise
    if ("errors" in err && Array.isArray(err.errors)) {
      this.errors.push(
        ...err.errors.map(normalizeError).filter(({ footprint }: any) => !existingErrors.includes(footprint)),
      );
    } else if (!("footprint" in err) || !existingErrors.includes(err.footprint)) {
      this.errors.push(normalizeError(err));
    }
  }

  /**
   * Determines whether the given JSON reference exists within this {@link $Ref#value}.
   *
   * @param path - The full path being resolved, optionally with a JSON pointer in the hash
   * @param options
   * @returns
   */
  exists(path: string, options?: ParserOptions) {
    try {
      this.resolve(path, options);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Resolves the given JSON reference within this {@link $Ref#value} and returns the resolved value.
   *
   * @param path - The full path being resolved, optionally with a JSON pointer in the hash
   * @param options
   * @returns - Returns the resolved value
   */
  get(path: string, options?: ParserOptions) {
    return this.resolve(path, options)?.value;
  }

  /**
   * Resolves the given JSON reference within this {@link $Ref#value}.
   *
   * @param path - The full path being resolved, optionally with a JSON pointer in the hash
   * @param options
   * @param friendlyPath - The original user-specified path (used for error messages)
   * @param pathFromRoot - The path of `obj` from the schema root
   * @returns
   */
  resolve(path: string, options?: ParserOptions, friendlyPath?: string, pathFromRoot?: string) {
    const pointer = new Pointer<S>(this, path, friendlyPath);
    return pointer.resolve(this.value, options, pathFromRoot);
  }

  /**
   * Sets the value of a nested property within this {@link $Ref#value}.
   * If the property, or any of its parents don't exist, they will be created.
   *
   * @param path - The full path of the property to set, optionally with a JSON pointer in the hash
   * @param value - The value to assign
   */
  set(path: string, value: any) {
    const pointer = new Pointer(this, path);
    this.value = pointer.set(this.value, value);
  }

  /**
   * Determines whether the given value is a JSON reference.
   *
   * @param value - The value to inspect
   * @returns
   */
  static is$Ref(value: unknown): value is { $ref: string; length?: number } {
    return (
      Boolean(value) &&
      typeof value === "object" &&
      value !== null &&
      "$ref" in value &&
      typeof value.$ref === "string" &&
      value.$ref.length > 0
    );
  }

  /**
   * Determines whether the given value is an external JSON reference.
   *
   * @param value - The value to inspect
   * @returns
   */
  static isExternal$Ref(value: unknown): boolean {
    return $Ref.is$Ref(value) && value.$ref![0] !== "#";
  }

  /**
   * Determines whether the given value is a JSON reference, and whether it is allowed by the options.
   *
   * @param value - The value to inspect
   * @param options
   * @returns
   */
  static isAllowed$Ref(value: unknown) {
    if (this.is$Ref(value)) {
      if (value.$ref.substring(0, 2) === "#/" || value.$ref === "#") {
        // It's a JSON Pointer reference, which is always allowed
        return true;
      } else if (value.$ref[0] !== "#") {
        // It's an external reference, which is allowed by the options
        return true;
      }
    }
    return undefined;
  }

  /**
   * Determines whether the given value is a JSON reference that "extends" its resolved value.
   * That is, it has extra properties (in addition to "$ref"), so rather than simply pointing to
   * an existing value, this $ref actually creates a NEW value that is a shallow copy of the resolved
   * value, plus the extra properties.
   *
   * @example: {
     person: {
       properties: {
         firstName: { type: string }
         lastName: { type: string }
       }
     }
     employee: {
       properties: {
         $ref: #/person/properties
         salary: { type: number }
       }
     }
   }
   *  In this example, "employee" is an extended $ref, since it extends "person" with an additional
   *  property (salary).  The result is a NEW value that looks like this:
   *
   *  {
   *    properties: {
   *      firstName: { type: string }
   *      lastName: { type: string }
   *      salary: { type: number }
   *    }
   *  }
   *
   * @param value - The value to inspect
   * @returns
   */
  static isExtended$Ref(value: unknown) {
    return $Ref.is$Ref(value) && Object.keys(value).length > 1;
  }

  /**
   * Returns the resolved value of a JSON Reference.
   * If necessary, the resolved value is merged with the JSON Reference to create a new object
   *
   * @example: {
  person: {
    properties: {
      firstName: { type: string }
      lastName: { type: string }
    }
  }
  employee: {
    properties: {
      $ref: #/person/properties
      salary: { type: number }
    }
  }
  } When "person" and "employee" are merged, you end up with the following object:
   *
   *  {
   *    properties: {
   *      firstName: { type: string }
   *      lastName: { type: string }
   *      salary: { type: number }
   *    }
   *  }
   *
   * @param $ref - The JSON reference object (the one with the "$ref" property)
   * @param resolvedValue - The resolved value, which can be any type
   * @returns - Returns the dereferenced value
   */
  static dereference<S extends object = JSONSchema>(
    $ref: $Ref<S>,
    resolvedValue: S,
  ): S {
    if (resolvedValue && typeof resolvedValue === "object" && $Ref.isExtended$Ref($ref)) {
      const merged = {};
      for (const key of Object.keys($ref)) {
        if (key !== "$ref") {
          // @ts-expect-error TS(7053): Element implicitly has an 'any' type because expre... Remove this comment to see the full error message
          merged[key] = $ref[key];
        }
      }

      for (const key of Object.keys(resolvedValue)) {
        if (!(key in merged)) {
          // @ts-expect-error TS(7053): Element implicitly has an 'any' type because expre... Remove this comment to see the full error message
          merged[key] = resolvedValue[key];
        }
      }

      return merged as S;
    } else {
      // Completely replace the original reference with the resolved value
      return resolvedValue;
    }
  }
}

export default $Ref;
