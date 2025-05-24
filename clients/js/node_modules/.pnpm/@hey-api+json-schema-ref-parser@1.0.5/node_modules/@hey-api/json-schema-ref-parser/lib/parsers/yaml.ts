import { ParserError } from "../util/errors.js";
import yaml from "js-yaml";
import { JSON_SCHEMA } from "js-yaml";
import type { FileInfo, JSONSchema } from "../types/index.js";
import type { Plugin } from "../types/index.js";

export const yamlParser: Plugin = {
  // JSON is valid YAML
  canHandle: (file: FileInfo) => [".yaml", ".yml", ".json"].includes(file.extension),
  handler: async (file: FileInfo): Promise<JSONSchema> => {
    const data = Buffer.isBuffer(file.data) ? file.data.toString() : file.data;

    if (typeof data !== "string") {
      // data is already a JavaScript value (object, array, number, null, NaN, etc.)
      return data;
    }

    try {
      const yamlSchema = yaml.load(data, { schema: JSON_SCHEMA }) as JSONSchema
      return yamlSchema;
    } catch (error: any) {
      throw new ParserError(error?.message || "Parser Error", file.url);
    }
  },
  name: 'yaml',
};
