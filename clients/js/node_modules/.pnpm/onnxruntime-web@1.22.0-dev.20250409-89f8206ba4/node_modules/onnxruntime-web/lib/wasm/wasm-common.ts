// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from 'onnxruntime-common';

// a dummy type declaration for Float16Array in case any polyfill is available.
declare global {
  // eslint-disable-next-line @typescript-eslint/naming-convention, @typescript-eslint/no-explicit-any
  const Float16Array: any;
}

// This file includes common definitions. They do NOT have dependency on the WebAssembly instance.

/**
 * Copied from ONNX definition. Use this to drop dependency 'onnx_proto' to decrease compiled .js file size.
 */
export const enum DataType {
  undefined = 0,
  float = 1,
  uint8 = 2,
  int8 = 3,
  uint16 = 4,
  int16 = 5,
  int32 = 6,
  int64 = 7,
  string = 8,
  bool = 9,
  float16 = 10,
  double = 11,
  uint32 = 12,
  uint64 = 13,
  complex64 = 14,
  complex128 = 15,
  bfloat16 = 16,

  // 4-bit data-types
  uint4 = 21,
  int4 = 22,
}

/**
 * Map string tensor data to enum value
 */
export const tensorDataTypeStringToEnum = (type: string): DataType => {
  switch (type) {
    case 'int8':
      return DataType.int8;
    case 'uint8':
      return DataType.uint8;
    case 'bool':
      return DataType.bool;
    case 'int16':
      return DataType.int16;
    case 'uint16':
      return DataType.uint16;
    case 'int32':
      return DataType.int32;
    case 'uint32':
      return DataType.uint32;
    case 'float16':
      return DataType.float16;
    case 'float32':
      return DataType.float;
    case 'float64':
      return DataType.double;
    case 'string':
      return DataType.string;
    case 'int64':
      return DataType.int64;
    case 'uint64':
      return DataType.uint64;
    case 'int4':
      return DataType.int4;
    case 'uint4':
      return DataType.uint4;

    default:
      throw new Error(`unsupported data type: ${type}`);
  }
};

/**
 * Map enum value to string tensor data
 */
export const tensorDataTypeEnumToString = (typeProto: DataType): Tensor.Type => {
  switch (typeProto) {
    case DataType.int8:
      return 'int8';
    case DataType.uint8:
      return 'uint8';
    case DataType.bool:
      return 'bool';
    case DataType.int16:
      return 'int16';
    case DataType.uint16:
      return 'uint16';
    case DataType.int32:
      return 'int32';
    case DataType.uint32:
      return 'uint32';
    case DataType.float16:
      return 'float16';
    case DataType.float:
      return 'float32';
    case DataType.double:
      return 'float64';
    case DataType.string:
      return 'string';
    case DataType.int64:
      return 'int64';
    case DataType.uint64:
      return 'uint64';
    case DataType.int4:
      return 'int4';
    case DataType.uint4:
      return 'uint4';

    default:
      throw new Error(`unsupported data type: ${typeProto}`);
  }
};

/**
 * get tensor size in bytes by the given data type and dimensions
 * @returns size in integer or undefined if the data type is not supported
 */
export const calculateTensorSizeInBytes = (
  dateType: number,
  dimsOrSize: readonly number[] | number,
): number | undefined => {
  const elementSize = [
    -1, // undefined = 0
    4, // float = 1
    1, // uint8 = 2
    1, // int8 = 3
    2, // uint16 = 4
    2, // int16 = 5
    4, // int32 = 6
    8, // int64 = 7
    -1, // string = 8
    1, // bool = 9
    2, // float16 = 10
    8, // double = 11
    4, // uint32 = 12
    8, // uint64 = 13
    -1, // complex64 = 14
    -1, // complex128 = 15
    -1, // bfloat16 = 16
    -1, // FLOAT8E4M3FN = 17
    -1, // FLOAT8E4M3FNUZ = 18
    -1, // FLOAT8E5M2 = 19
    -1, // FLOAT8E5M2FNUZ = 20
    0.5, // uint4 = 21
    0.5, // int4 = 22
  ][dateType];

  const size = typeof dimsOrSize === 'number' ? dimsOrSize : dimsOrSize.reduce((a, b) => a * b, 1);
  return elementSize > 0 ? Math.ceil(size * elementSize) : undefined;
};

/**
 * get typed array constructor by the given tensor type
 */
export const tensorTypeToTypedArrayConstructor = (
  type: Tensor.Type,
):
  | Float32ArrayConstructor
  | Uint8ArrayConstructor
  | Int8ArrayConstructor
  | Uint16ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | BigInt64ArrayConstructor
  | Uint8ArrayConstructor
  | Float64ArrayConstructor
  | Uint32ArrayConstructor
  | BigUint64ArrayConstructor => {
  switch (type) {
    case 'float16':
      // allow Float16Array polyfill.
      return typeof Float16Array !== 'undefined' && Float16Array.from ? Float16Array : Uint16Array;
    case 'float32':
      return Float32Array;
    case 'uint8':
      return Uint8Array;
    case 'int8':
      return Int8Array;
    case 'uint16':
      return Uint16Array;
    case 'int16':
      return Int16Array;
    case 'int32':
      return Int32Array;
    case 'bool':
      return Uint8Array;
    case 'float64':
      return Float64Array;
    case 'uint32':
      return Uint32Array;
    case 'int64':
      return BigInt64Array;
    case 'uint64':
      return BigUint64Array;
    default:
      throw new Error(`unsupported type: ${type}`);
  }
};

/**
 * Map string log level to integer value
 */
export const logLevelStringToEnum = (logLevel?: 'verbose' | 'info' | 'warning' | 'error' | 'fatal'): number => {
  switch (logLevel) {
    case 'verbose':
      return 0;
    case 'info':
      return 1;
    case 'warning':
      return 2;
    case 'error':
      return 3;
    case 'fatal':
      return 4;
    default:
      throw new Error(`unsupported logging level: ${logLevel}`);
  }
};

/**
 * Check whether the given tensor type is supported by GPU buffer
 */
export const isGpuBufferSupportedType = (type: Tensor.Type): type is Tensor.GpuBufferDataTypes =>
  type === 'float32' ||
  type === 'float16' ||
  type === 'int32' ||
  type === 'int64' ||
  type === 'uint32' ||
  type === 'uint8' ||
  type === 'bool' ||
  type === 'uint4' ||
  type === 'int4';

/**
 * Check whether the given tensor type is supported by WebNN MLTensor
 */
export const isMLTensorSupportedType = (type: Tensor.Type): type is Tensor.MLTensorDataTypes =>
  type === 'float32' ||
  type === 'float16' ||
  type === 'int32' ||
  type === 'int64' ||
  type === 'uint32' ||
  type === 'uint64' ||
  type === 'int8' ||
  type === 'uint8' ||
  type === 'bool' ||
  type === 'uint4' ||
  type === 'int4';

/**
 * Map string data location to integer value
 */
export const dataLocationStringToEnum = (location: Tensor.DataLocation): number => {
  switch (location) {
    case 'none':
      return 0;
    case 'cpu':
      return 1;
    case 'cpu-pinned':
      return 2;
    case 'texture':
      return 3;
    case 'gpu-buffer':
      return 4;
    case 'ml-tensor':
      return 5;
    default:
      throw new Error(`unsupported data location: ${location}`);
  }
};

/**
 * Map integer data location to string value
 */
export const dataLocationEnumToString = (location: number): Tensor.DataLocation | undefined =>
  (['none', 'cpu', 'cpu-pinned', 'texture', 'gpu-buffer', 'ml-tensor'] as const)[location];
