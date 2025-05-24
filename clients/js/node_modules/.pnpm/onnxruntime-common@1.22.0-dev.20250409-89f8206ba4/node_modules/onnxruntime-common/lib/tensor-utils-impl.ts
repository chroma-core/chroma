// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import {
  CpuPinnedConstructorParameters,
  GpuBufferConstructorParameters,
  MLTensorConstructorParameters,
  TextureConstructorParameters,
} from './tensor-factory.js';
import { Tensor } from './tensor-impl.js';

/**
 * calculate size from dims.
 *
 * @param dims the dims array. May be an illegal input.
 */
export const calculateSize = (dims: readonly unknown[]): number => {
  let size = 1;
  for (let i = 0; i < dims.length; i++) {
    const dim = dims[i];
    if (typeof dim !== 'number' || !Number.isSafeInteger(dim)) {
      throw new TypeError(`dims[${i}] must be an integer, got: ${dim}`);
    }
    if (dim < 0) {
      throw new RangeError(`dims[${i}] must be a non-negative integer, got: ${dim}`);
    }
    size *= dim;
  }
  return size;
};

/**
 * implementation of Tensor.reshape()
 */
export const tensorReshape = (tensor: Tensor, dims: readonly number[]): Tensor => {
  switch (tensor.location) {
    case 'cpu':
      return new Tensor(tensor.type, tensor.data, dims);
    case 'cpu-pinned':
      return new Tensor({
        location: 'cpu-pinned',
        data: tensor.data as CpuPinnedConstructorParameters['data'],
        type: tensor.type as CpuPinnedConstructorParameters['type'],
        dims,
      });
    case 'texture':
      return new Tensor({
        location: 'texture',
        texture: tensor.texture,
        type: tensor.type as TextureConstructorParameters['type'],
        dims,
      });
    case 'gpu-buffer':
      return new Tensor({
        location: 'gpu-buffer',
        gpuBuffer: tensor.gpuBuffer,
        type: tensor.type as GpuBufferConstructorParameters['type'],
        dims,
      });
    case 'ml-tensor':
      return new Tensor({
        location: 'ml-tensor',
        mlTensor: tensor.mlTensor,
        type: tensor.type as MLTensorConstructorParameters['type'],
        dims,
      });
    default:
      throw new Error(`tensorReshape: tensor location ${tensor.location} is not supported`);
  }
};
