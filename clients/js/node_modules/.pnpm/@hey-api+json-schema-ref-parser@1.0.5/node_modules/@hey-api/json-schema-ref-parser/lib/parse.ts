import { ono } from "@jsdevtools/ono";
import { getExtension } from "./util/url.js";
import * as plugins from "./util/plugins.js";
import type { PluginResult } from "./util/plugins.js";
import { ParserError } from "./util/errors.js";
import type { $RefParserOptions } from "./options.js";
import type { FileInfo } from "./types/index.js";

/**
 * Prepares the file object so we can populate it with data and other values
 * when it's read and parsed. This "file object" will be passed to all
 * resolvers and parsers.
 */
export function newFile(path: string): FileInfo {
  let url = path;
  // Remove the URL fragment, if any
  const hashIndex = url.indexOf("#");
  let hash = "";
  if (hashIndex > -1) {
    hash = url.substring(hashIndex);
    url = url.substring(0, hashIndex);
  }
  return {
    extension: getExtension(url),
    hash,
    url,
  } as FileInfo;
}

/**
 * Parses the given file's contents, using the configured parser plugins.
 */
export const parseFile = async (file: FileInfo, options: $RefParserOptions): Promise<PluginResult> => {
  try {
    // If none of the parsers are a match for this file, try all of them. This
    // handles situations where the file is a supported type, just with an
    // unknown extension.
    const parsers = [options.parse.json, options.parse.yaml, options.parse.text, options.parse.binary];
    const filtered = parsers.filter((plugin) => plugin.canHandle(file));
    return await plugins.run(filtered.length ? filtered : parsers, file);
  } catch (error: any) {
    if (error && error.message && error.message.startsWith("Error parsing")) {
      throw error;
    }
    
    if (!error || !("error" in error)) {
      throw ono.syntax(`Unable to parse ${file.url}`);
    }
    
    if (error.error instanceof ParserError) {
      throw error.error;
    }

    throw new ParserError(error.error.message, file.url);
  }
}
