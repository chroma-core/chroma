// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { BroadcastUtil, ShapeUtil } from '../../util';
import { ComputeContext } from '../types';

import { createNaiveMatmulProgramInfo } from './matmul-shaders';
import { createMatmulProgramInfo } from './3rd-party/matmul_packed_webgpu';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('MatMul requires 2 inputs.');
  }

  if (inputs[0].dims[inputs[0].dims.length - 1] !== inputs[1].dims[inputs[1].dims.length - 2]) {
    throw new Error('shared dimension does not match.');
  }
};

export const matMul = (context: ComputeContext): void => {
  validateInputs(context.inputs);
  const outputShape = BroadcastUtil.calcShape(context.inputs[0].dims, context.inputs[1].dims, true);
  if (!outputShape) {
    throw new Error("Can't use matmul on the given tensors");
  }
  const N = outputShape[outputShape.length - 1];
  const K = context.inputs[0].dims[context.inputs[0].dims.length - 1];
  if (N < 8 && K < 8) {
    context.compute(createNaiveMatmulProgramInfo(context.inputs, { activation: '' }, outputShape));
  } else {
    const M = outputShape[outputShape.length - 2];
    const batchA = ShapeUtil.size(context.inputs[0].dims.slice(0, -2));
    const batchB = ShapeUtil.size(context.inputs[1].dims.slice(0, -2));
    if (batchA !== 1 && M === 1 && batchB === 1) {
      // Optimization for batched vec-mat-mul
      const reshapedA = context.inputs[0].reshape([1, batchA, K]);
      const reshapedB = context.inputs[1].reshape([1, K, N]);
      const matmulOutputShape = [1, batchA, N];
      const matmulInputs = [reshapedA, reshapedB];
      context.compute(createMatmulProgramInfo(matmulInputs, { activation: '' }, outputShape, matmulOutputShape), {
        inputs: matmulInputs,
      });
    } else {
      context.compute(createMatmulProgramInfo(context.inputs, { activation: '' }, outputShape));
    }
  }
};
