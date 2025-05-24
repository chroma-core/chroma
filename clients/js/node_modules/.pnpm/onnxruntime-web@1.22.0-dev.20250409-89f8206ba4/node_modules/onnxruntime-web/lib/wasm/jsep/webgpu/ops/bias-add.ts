// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { inputVariable, outputVariable, ShaderHelper } from './common';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (inputs[0].dims.length !== 3) {
    throw new Error('input should have 3 dimensions');
  }

  if (![320, 640, 1280].includes(inputs[0].dims[2])) {
    throw new Error('number of channels should be 320, 640 or 1280');
  }

  if (inputs[1].dims.length !== 1) {
    throw new Error('bias is expected to have 1 dimensions');
  }

  if (inputs[0].dims[2] !== inputs[1].dims[0]) {
    throw new Error('last dimension of input and bias are not the same');
  }
};

const createBiasAddProgramInfo = (inputs: readonly TensorView[]): ProgramInfo => {
  const outputShape = inputs[0].dims;

  const channels = inputs[0].dims[2];
  // since channel number can be only 320/640/1280, it's always divisable by 4
  const outputSize = ShapeUtil.size(outputShape) / 4;

  const dataType = inputs[0].dataType;
  const input = inputVariable('input', dataType, outputShape, 4);
  const bias = inputVariable('bias', dataType, [channels], 4);
  const residual = inputVariable('residual', dataType, outputShape, 4);
  const output = outputVariable('output', dataType, outputShape, 4);

  const getShaderSource = (shaderHelper: ShaderHelper) => `
  const channels = ${channels}u / 4;
  ${shaderHelper.declareVariables(input, bias, residual, output)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes(outputSize)}
    let value = ${input.getByOffset('global_idx')}
      + ${bias.getByOffset('global_idx % channels')} + ${residual.getByOffset('global_idx')};
    ${output.setByOffset('global_idx', 'value')}
  }`;

  return {
    name: 'BiasAdd',
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
    }),
    getShaderSource,
  };
};

export const biasAdd = (context: ComputeContext): void => {
  validateInputs(context.inputs);
  context.compute(createBiasAddProgramInfo(context.inputs));
};
