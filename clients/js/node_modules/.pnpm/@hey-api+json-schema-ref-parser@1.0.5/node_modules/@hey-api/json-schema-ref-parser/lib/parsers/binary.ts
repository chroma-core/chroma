import type { FileInfo } from "../types/index.js";
import type { Plugin } from "../types/index.js";

const BINARY_REGEXP = /\.(jpeg|jpg|gif|png|bmp|ico)$/i;

export const binaryParser: Plugin = {
  canHandle: (file: FileInfo) => Buffer.isBuffer(file.data) && BINARY_REGEXP.test(file.url),
  handler: (file: FileInfo): Buffer => Buffer.isBuffer(file.data)
    ? file.data
    // This will reject if data is anything other than a string or typed array
    : Buffer.from(file.data),
  name: 'binary',
};
