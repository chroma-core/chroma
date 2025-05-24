// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import {
  inputVariable,
  outputVariable,
  ShaderHelper,
  tensorTypeToWsglValueType,
  UniformsArrayType,
  WORKGROUP_SIZE,
} from './common';
import * as unary from './unary-op';

// GELU is defined as Y=0.5*X*(1+tanh(0.797885*X+0.035677*X*X*X)), where X may pre-add a bias.

const createFastGeluProgramInfo = (inputTensors: readonly TensorView[]): ProgramInfo => {
  const dataType = inputTensors[0].dataType;
  const outputSize = ShapeUtil.size(inputTensors[0].dims);
  const biasLength = ShapeUtil.size(inputTensors[1].dims);
  // can only use vec4 when bias length is multiple of 4
  const useVec4 = biasLength % 4 === 0;
  const getShaderSource = (shaderHelper: ShaderHelper): string => {
    const x = inputVariable('x', dataType, [1], 4);
    const bias = inputVariable('bias', dataType, [1], 4);
    const y = outputVariable('y', dataType, [1], 4);

    const uniforms: UniformsArrayType = [
      { name: 'output_vec_size', type: 'u32' },
      { name: 'bias_size', type: 'u32' },
    ];

    const singleElementBias = (i: 0 | 1 | 2 | 3) => `
      let bias${i}_offset: u32 = (global_idx * 4 + ${i}) % uniforms.bias_size;
      let bias${i} = ${bias.getByOffset(`bias${i}_offset / 4`)}[bias${i}_offset % 4];`;
    const biasGetExpression = useVec4
      ? `
      let bias = ${bias.getByOffset('global_idx % (uniforms.bias_size / 4)')};`
      : `${singleElementBias(0)}${singleElementBias(1)}${singleElementBias(2)}${singleElementBias(3)}
      let bias = ${x.type.value}(bias0, bias1, bias2, bias3);`;

    return `${shaderHelper.registerUniforms(uniforms).declareVariables(x, bias, y)}

    ${unary.fastGeluImpl(tensorTypeToWsglValueType(dataType))}

    ${shaderHelper.mainStart(WORKGROUP_SIZE)}
      ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_vec_size')}

      let x = ${x.getByOffset('global_idx')};
      ${biasGetExpression}
      let x_in = x + bias;
      ${y.setByOffset('global_idx', unary.fastGeluExpression('x_in'))}
    }`;
  };

  return {
    name: 'FastGeluWithBias',
    shaderCache: { hint: `${useVec4}`, inputDependencies: ['type', 'type'] },
    getShaderSource,
    getRunData: (inputs) => ({
      outputs: [{ dims: inputs[0].dims, dataType: inputs[0].dataType }],
      programUniforms: [
        { type: DataType.uint32, data: Math.ceil(outputSize / 4) },
        { type: DataType.uint32, data: biasLength },
      ],
      dispatchGroup: { x: Math.ceil(outputSize / WORKGROUP_SIZE / 4) },
    }),
  };
};

export const fastGelu = (context: ComputeContext): void => {
  if (context.inputs.length < 2 || ShapeUtil.size(context.inputs[1].dims) === 0) {
    unary.fastGelu(context);
  } else {
    context.compute(createFastGeluProgramInfo(context.inputs));
  }
};
