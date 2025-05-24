// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from 'onnxruntime-common';

import { tensorTypeToTypedArrayConstructor } from '../wasm-common';

export const createView = (
  dataBuffer: ArrayBuffer,
  type: Tensor.Type,
):
  | Int32Array
  | Uint32Array
  | BigInt64Array
  | BigUint64Array
  | Uint8Array
  | Float32Array
  | Float64Array
  | Int8Array
  | Int16Array
  | Uint16Array => new (tensorTypeToTypedArrayConstructor(type))(dataBuffer);

/**
 * a TensorView does not own the data.
 */
export interface TensorView {
  readonly data: number;
  readonly dataType: number;
  readonly dims: readonly number[];

  /**
   * get a Float16Array data view of the tensor data. tensor data must be on CPU.
   */
  getUint16Array(): Uint16Array;

  /**
   * get a Float32Array data view of the tensor data. tensor data must be on CPU.
   */
  getFloat32Array(): Float32Array;

  /**
   * get a BigInt64Array data view of the tensor data. tensor data must be on CPU.
   */
  getBigInt64Array(): BigInt64Array;

  /**
   * get a Int32Array data view of the tensor data. tensor data must be on CPU.
   */
  getInt32Array(): Int32Array;

  /**
   * get a Uint16Array data view of the tensor data. tensor data must be on CPU.
   */
  getUint16Array(): Uint16Array;

  /**
   * create a new tensor view with the same data but different dimensions.
   */
  reshape(newDims: readonly number[]): TensorView;
}
