// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import type { Env, InferenceSession, Tensor } from 'onnxruntime-common';

/**
 * Among all the tensor locations, only 'cpu' is serializable.
 */
export type SerializableTensorMetadata = [
  dataType: Tensor.Type,
  dims: readonly number[],
  data: Tensor.DataType,
  location: 'cpu',
];

export type GpuBufferMetadata = {
  gpuBuffer: Tensor.GpuBufferType;
  download?: () => Promise<Tensor.DataTypeMap[Tensor.GpuBufferDataTypes]>;
  dispose?: () => void;
};

export type MLTensorMetadata = {
  mlTensor: Tensor.MLTensorType;
  download?: () => Promise<Tensor.DataTypeMap[Tensor.MLTensorDataTypes]>;
  dispose?: () => void;
};

/**
 * Tensors on location 'cpu-pinned', 'gpu-buffer', and 'ml-tensor' are not serializable.
 */
export type UnserializableTensorMetadata =
  | [dataType: Tensor.Type, dims: readonly number[], data: GpuBufferMetadata, location: 'gpu-buffer']
  | [dataType: Tensor.Type, dims: readonly number[], data: MLTensorMetadata, location: 'ml-tensor']
  | [dataType: Tensor.Type, dims: readonly number[], data: Tensor.DataType, location: 'cpu-pinned'];

/**
 * Tensor metadata is a tuple of [dataType, dims, data, location], where
 * - dataType: tensor data type
 * - dims: tensor dimensions
 * - data: tensor data, which can be one of the following depending on the location:
 *   - cpu: Uint8Array
 *   - cpu-pinned: Uint8Array
 *   - gpu-buffer: GpuBufferMetadata
 *   - ml-tensor: MLTensorMetadata
 * - location: tensor data location
 */
export type TensorMetadata = SerializableTensorMetadata | UnserializableTensorMetadata;

export type SerializableSessionMetadata = [
  sessionHandle: number,
  inputNames: string[],
  outputNames: string[],
  inputMetadata: InferenceSession.ValueMetadata[],
  outputMetadata: InferenceSession.ValueMetadata[],
];

export type SerializableInternalBuffer = [bufferOffset: number, bufferLength: number];

interface MessageError {
  err?: string;
}

interface MessageInitWasm extends MessageError {
  type: 'init-wasm';
  in?: Env;
  out?: never;
}

interface MessageInitEp extends MessageError {
  type: 'init-ep';
  in?: { env: Env; epName: string };
  out?: never;
}

interface MessageCopyFromExternalBuffer extends MessageError {
  type: 'copy-from';
  in?: { buffer: Uint8Array };
  out?: SerializableInternalBuffer;
}

interface MessageCreateSession extends MessageError {
  type: 'create';
  in?: { model: SerializableInternalBuffer | Uint8Array; options?: InferenceSession.SessionOptions };
  out?: SerializableSessionMetadata;
}

interface MessageReleaseSession extends MessageError {
  type: 'release';
  in?: number;
  out?: never;
}

interface MessageRun extends MessageError {
  type: 'run';
  in?: {
    sessionId: number;
    inputIndices: number[];
    inputs: SerializableTensorMetadata[];
    outputIndices: number[];
    options: InferenceSession.RunOptions;
  };
  out?: SerializableTensorMetadata[];
}

interface MesssageEndProfiling extends MessageError {
  type: 'end-profiling';
  in?: number;
  out?: never;
}

export type OrtWasmMessage =
  | MessageInitWasm
  | MessageInitEp
  | MessageCopyFromExternalBuffer
  | MessageCreateSession
  | MessageReleaseSession
  | MessageRun
  | MesssageEndProfiling;
