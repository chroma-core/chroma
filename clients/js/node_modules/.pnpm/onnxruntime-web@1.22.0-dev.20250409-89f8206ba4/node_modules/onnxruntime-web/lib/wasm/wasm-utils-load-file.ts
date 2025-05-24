// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { isNode } from './wasm-utils-env';

/**
 * Load a file into a Uint8Array.
 *
 * @param file - the file to load. Can be a URL/path, a Blob, an ArrayBuffer, or a Uint8Array.
 * @returns a Uint8Array containing the file data.
 */
export const loadFile = async (file: string | Blob | ArrayBufferLike | Uint8Array): Promise<Uint8Array> => {
  if (typeof file === 'string') {
    if (isNode) {
      // load file into ArrayBuffer in Node.js
      try {
        const { readFile } = require('node:fs/promises');
        return new Uint8Array(await readFile(file));
      } catch (e) {
        if (e.code === 'ERR_FS_FILE_TOO_LARGE') {
          // file is too large, use fs.createReadStream instead
          const { createReadStream } = require('node:fs');
          const stream = createReadStream(file);
          const chunks: Uint8Array[] = [];
          for await (const chunk of stream) {
            chunks.push(chunk);
          }
          return new Uint8Array(Buffer.concat(chunks));
        }
        throw e;
      }
    } else {
      // load file into ArrayBuffer in browsers
      const response = await fetch(file);
      if (!response.ok) {
        throw new Error(`failed to load external data file: ${file}`);
      }
      const contentLengthHeader = response.headers.get('Content-Length');
      const fileSize = contentLengthHeader ? parseInt(contentLengthHeader, 10) : 0;
      if (fileSize < 1073741824 /* 1GB */) {
        // when Content-Length header is not set, we cannot determine the file size. We assume it is small enough to
        // load into memory.
        return new Uint8Array(await response.arrayBuffer());
      } else {
        // file is too large, use stream instead
        if (!response.body) {
          throw new Error(`failed to load external data file: ${file}, no response body.`);
        }
        const reader = response.body.getReader();

        let buffer;
        try {
          // try to create ArrayBuffer directly
          buffer = new ArrayBuffer(fileSize);
        } catch (e) {
          if (e instanceof RangeError) {
            // use WebAssembly Memory to allocate larger ArrayBuffer
            const pages = Math.ceil(fileSize / 65536);
            buffer = new WebAssembly.Memory({ initial: pages, maximum: pages }).buffer;
          } else {
            throw e;
          }
        }

        let offset = 0;
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }
          const chunkSize = value.byteLength;
          const chunk = new Uint8Array(buffer, offset, chunkSize);
          chunk.set(value);
          offset += chunkSize;
        }
        return new Uint8Array(buffer, 0, fileSize);
      }
    }
  } else if (file instanceof Blob) {
    return new Uint8Array(await file.arrayBuffer());
  } else if (file instanceof Uint8Array) {
    return file;
  } else {
    return new Uint8Array(file);
  }
};
