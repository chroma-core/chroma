// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import {
  castToF32,
  fillVector,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  sumVector,
  tensorTypeToWsglStorageType,
  UniformsArrayType,
} from './common';

interface LayerNormAttributes {
  simplified: boolean;
  axis: number;
  epsilon: number;
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length < 2) {
    throw new Error('layerNorm requires at least 2 inputs.');
  }
};

const createLayerNormProgramInfo = (
  inputs: readonly TensorView[],
  attributes: LayerNormAttributes,
  outputCount: number,
): ProgramInfo => {
  const simplified = attributes.simplified;

  const xShape = inputs[0].dims;
  const scale = inputs[1];
  const bias = !simplified && inputs[2];

  const outputShape = xShape;
  const axis = ShapeUtil.normalizeAxis(attributes.axis, xShape.length);
  const normCount = ShapeUtil.sizeToDimension(xShape, axis);
  const normSize = ShapeUtil.sizeFromDimension(xShape, axis);

  const scaleSize = ShapeUtil.size(scale.dims);
  const biasSize = bias ? ShapeUtil.size(bias.dims) : 0;
  if (scaleSize !== normSize || (bias && biasSize !== normSize)) {
    throw new Error(`Size of X.shape()[axis:] == ${normSize}.
       Size of scale and bias (if provided) must match this.
       Got scale size of ${scaleSize} and bias size of ${biasSize}`);
  }

  const meanInvStdDevDim: number[] = [];
  for (let i = 0; i < xShape.length; ++i) {
    if (i < axis) {
      meanInvStdDevDim.push(xShape[i]);
    } else {
      meanInvStdDevDim.push(1);
    }
  }
  const components = getMaxComponents(normSize);
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['type', 'type'];
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: normCount },
    { type: DataType.float, data: normSize },
    { type: DataType.uint32, data: Math.floor(normSize / components) },
    { type: DataType.float, data: attributes.epsilon },
  ];
  if (bias) {
    inputDependencies.push('type');
  }
  const hasMeanDataOutput = outputCount > 1;
  const hasInvStdOutput = outputCount > 2;

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);
    const variables = [
      inputVariable('x', inputs[0].dataType, inputs[0].dims, components),
      inputVariable('scale', scale.dataType, scale.dims, components),
    ];
    if (bias) {
      variables.push(inputVariable('bias', bias.dataType, bias.dims, components));
    }
    variables.push(outputVariable('output', inputs[0].dataType, outputShape, components));
    if (hasMeanDataOutput) {
      variables.push(outputVariable('mean_data_output', DataType.float, meanInvStdDevDim));
    }
    if (hasInvStdOutput) {
      variables.push(outputVariable('inv_std_output', DataType.float, meanInvStdDevDim));
    }

    const uniforms: UniformsArrayType = [
      { name: 'norm_count', type: 'u32' },
      { name: 'norm_size', type: 'f32' },
      { name: 'norm_size_vectorized', type: 'u32' },
      { name: 'epsilon', type: 'f32' },
    ];
    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...variables)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.norm_count')}
    let offset = global_idx * uniforms.norm_size_vectorized;
    var mean_vector = ${fillVector('f32', components)};
    var mean_square_vector = ${fillVector('f32', components)};

    for (var h: u32 = 0u; h < uniforms.norm_size_vectorized; h++) {
      let value = ${castToF32(dataType, components, 'x[h + offset]')};
      mean_vector += value;
      mean_square_vector += value * value;
    }
    let mean = ${sumVector('mean_vector', components)} / uniforms.norm_size;
    let inv_std_dev = inverseSqrt(${sumVector('mean_square_vector', components)} / uniforms.norm_size ${
      simplified ? '' : '- mean * mean'
    } + uniforms.epsilon);

    for (var j: u32 = 0; j < uniforms.norm_size_vectorized; j++) {
      let f32input = ${castToF32(dataType, components, 'x[j + offset]')};
      let f32scale = ${castToF32(dataType, components, 'scale[j]')};
      output[j + offset] = ${variables[0].type.value}((f32input ${simplified ? '' : '- mean'}) * inv_std_dev * f32scale
        ${bias ? `+ ${castToF32(dataType, components, 'bias[j]')}` : ''}
      );
    }

    ${hasMeanDataOutput ? 'mean_data_output[global_idx] = mean' : ''};
    ${hasInvStdOutput ? 'inv_std_output[global_idx] = inv_std_dev' : ''};
  }`;
  };
  const outputs = [{ dims: outputShape, dataType: inputs[0].dataType }];
  if (hasMeanDataOutput) {
    outputs.push({ dims: meanInvStdDevDim, dataType: DataType.float });
  }
  if (hasInvStdOutput) {
    outputs.push({ dims: meanInvStdDevDim, dataType: DataType.float });
  }

  return {
    name: 'LayerNormalization',
    shaderCache: { hint: `${components};${outputCount};${simplified}`, inputDependencies },
    getRunData: () => ({
      outputs,
      dispatchGroup: { x: Math.ceil(normCount / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const layerNorm = (context: ComputeContext, attributes: LayerNormAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createLayerNormProgramInfo(context.inputs, attributes, context.outputCount));
};
