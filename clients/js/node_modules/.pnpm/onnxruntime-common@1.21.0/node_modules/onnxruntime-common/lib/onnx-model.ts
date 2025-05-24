// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * A string that represents a file's URL or path.
 *
 * Path is vailable only in onnxruntime-node or onnxruntime-web running in Node.js.
 */
export type FileUrlOrPath = string;

/**
 * A Blob object that represents a file.
 */
export type FileBlob = Blob;

/**
 * A Uint8Array, ArrayBuffer or SharedArrayBuffer object that represents a file content.
 *
 * When it is an ArrayBuffer or SharedArrayBuffer, the whole buffer is assumed to be the file content.
 */
export type FileData = Uint8Array | ArrayBufferLike;

/**
 * Represents a file that can be loaded by the ONNX Runtime JavaScript API.
 */
export type FileType = FileUrlOrPath | FileBlob | FileData;

/**
 * Represents an external data file.
 */
export interface ExternalDataFileDescription {
  /**
   * Specify the external data file.
   */
  data: FileType;
  /**
   * Specify the file path.
   */
  path: string;
}

/**
 * Represents an external data file.
 *
 * When using a string, it should be a file URL or path that in the same directory as the model file.
 */
export type ExternalDataFileType = ExternalDataFileDescription | FileUrlOrPath;

/**
 * Options for model loading.
 */
export interface OnnxModelOptions {
  /**
   * Specifying a list of files that represents the external data.
   */
  externalData?: readonly ExternalDataFileType[];
}
