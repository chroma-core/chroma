import type { FileInfo } from "../types/index.js";
import { ParserError } from "../util/errors.js";
import type { Plugin } from "../types/index.js";

const TEXT_REGEXP = /\.(txt|htm|html|md|xml|js|min|map|css|scss|less|svg)$/i;

export const textParser: Plugin = {
  canHandle: (file: FileInfo) => (typeof file.data === "string" || Buffer.isBuffer(file.data)) && TEXT_REGEXP.test(file.url),
  handler(file: FileInfo): string {
    if (typeof file.data === "string") {
      return file.data;
    }

    if (!Buffer.isBuffer(file.data)) {
      throw new ParserError("data is not text", file.url);
    }

    return file.data.toString('utf-8');
  },
  name: 'text',
};
