// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { inputVariable, outputVariable, ShaderHelper, tensorTypeToWsglStorageType } from './common';
import { erfImpl } from './unary-op';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (inputs[0].dims.length !== 3) {
    throw new Error('input should have 3 dimensions');
  }

  if (![2560, 5120, 10240].includes(inputs[0].dims[2])) {
    throw new Error('hidden state should be 2560, 5120 or 10240');
  }

  if (inputs[1].dims.length !== 1) {
    throw new Error('bias is expected to have 1 dimensions');
  }

  if (inputs[0].dims[2] !== inputs[1].dims[0]) {
    throw new Error('last dimension of input and bias are not the same');
  }
};

const createBiasSplitGeluProgramInfo = (inputs: readonly TensorView[]): ProgramInfo => {
  const outputShape = inputs[0].dims.slice();
  outputShape[2] = outputShape[2] / 2;

  const input = inputVariable('input', inputs[0].dataType, inputs[0].dims, 4);
  const bias = inputVariable('bias', inputs[0].dataType, [inputs[0].dims[2]], 4);
  const output = outputVariable('output', inputs[0].dataType, outputShape, 4);

  const outputSize = ShapeUtil.size(outputShape) / 4;
  const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);

  const getShaderSource = (shaderHelper: ShaderHelper) => `
  const M_SQRT2 = sqrt(2.0);
  const halfChannels = ${inputs[0].dims[2] / 4 / 2}u;

  ${shaderHelper.declareVariables(input, bias, output)}

  ${erfImpl(dataType)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes(outputSize)}
    let biasIdx = global_idx % halfChannels;
    let batchIndex = global_idx / halfChannels;
    let inputOffset = biasIdx + batchIndex * halfChannels * 2;
    let valueLeft = input[inputOffset] + bias[biasIdx];
    let valueRight = input[inputOffset + halfChannels] + bias[biasIdx + halfChannels];
    let geluRight = valueRight * 0.5 * (erf_vf32(valueRight / M_SQRT2) + 1);

    ${output.setByOffset('global_idx', 'valueLeft * geluRight')}
  }`;

  return {
    name: 'BiasSplitGelu',
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
    }),
    getShaderSource,
  };
};

export const biasSplitGelu = (context: ComputeContext): void => {
  validateInputs(context.inputs);
  context.compute(createBiasSplitGeluProgramInfo(context.inputs));
};
