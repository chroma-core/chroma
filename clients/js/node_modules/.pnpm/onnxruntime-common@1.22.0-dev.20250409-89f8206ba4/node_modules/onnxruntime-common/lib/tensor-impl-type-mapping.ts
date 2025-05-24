// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from './tensor.js';

export type SupportedTypedArrayConstructors =
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
  | BigUint64ArrayConstructor;
export type SupportedTypedArray = InstanceType<SupportedTypedArrayConstructors>;

// a runtime map that maps type string to TypedArray constructor. Should match Tensor.DataTypeMap.
export const NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP = new Map<string, SupportedTypedArrayConstructors>([
  ['float32', Float32Array],
  ['uint8', Uint8Array],
  ['int8', Int8Array],
  ['uint16', Uint16Array],
  ['int16', Int16Array],
  ['int32', Int32Array],
  ['bool', Uint8Array],
  ['float64', Float64Array],
  ['uint32', Uint32Array],
  ['int4', Uint8Array],
  ['uint4', Uint8Array],
]);

// a runtime map that maps type string to TypedArray constructor. Should match Tensor.DataTypeMap.
export const NUMERIC_TENSOR_TYPEDARRAY_TO_TYPE_MAP = new Map<SupportedTypedArrayConstructors, Tensor.Type>([
  [Float32Array, 'float32'],
  [Uint8Array, 'uint8'],
  [Int8Array, 'int8'],
  [Uint16Array, 'uint16'],
  [Int16Array, 'int16'],
  [Int32Array, 'int32'],
  [Float64Array, 'float64'],
  [Uint32Array, 'uint32'],
]);

// the following code allows delaying execution of BigInt/Float16Array checking. This allows lazy initialization for
// NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP and NUMERIC_TENSOR_TYPEDARRAY_TO_TYPE_MAP, which allows BigInt/Float16Array
// polyfill if available.
let isTypedArrayChecked = false;
export const checkTypedArray = () => {
  if (!isTypedArrayChecked) {
    isTypedArrayChecked = true;
    const isBigInt64ArrayAvailable = typeof BigInt64Array !== 'undefined' && BigInt64Array.from;
    const isBigUint64ArrayAvailable = typeof BigUint64Array !== 'undefined' && BigUint64Array.from;

    // eslint-disable-next-line @typescript-eslint/naming-convention, @typescript-eslint/no-explicit-any
    const Float16Array = (globalThis as any).Float16Array;
    const isFloat16ArrayAvailable = typeof Float16Array !== 'undefined' && Float16Array.from;

    if (isBigInt64ArrayAvailable) {
      NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP.set('int64', BigInt64Array);
      NUMERIC_TENSOR_TYPEDARRAY_TO_TYPE_MAP.set(BigInt64Array, 'int64');
    }
    if (isBigUint64ArrayAvailable) {
      NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP.set('uint64', BigUint64Array);
      NUMERIC_TENSOR_TYPEDARRAY_TO_TYPE_MAP.set(BigUint64Array, 'uint64');
    }
    if (isFloat16ArrayAvailable) {
      NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP.set('float16', Float16Array);
      NUMERIC_TENSOR_TYPEDARRAY_TO_TYPE_MAP.set(Float16Array, 'float16');
    } else {
      // if Float16Array is not available, use 'Uint16Array' to store the data.
      NUMERIC_TENSOR_TYPE_TO_TYPEDARRAY_MAP.set('float16', Uint16Array);
    }
  }
};
