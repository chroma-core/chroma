// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import {
  castToF32,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  sumVector,
  tensorTypeToWsglStorageType,
  UniformsArrayType,
} from './common';

export interface SkipLayerNormAttributes {
  simplified: boolean;
  epsilon: number;
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length < 3) {
    throw new Error('layerNorm requires at least 3 inputs.');
  }

  const input: TensorView = inputs[0];
  const skip: TensorView = inputs[1];
  const gamma: TensorView = inputs[2];

  if (input.dataType !== skip.dataType || input.dataType !== gamma.dataType) {
    throw new Error('All inputs must have the same data type');
  }

  if (input.dims.length !== 3 && input.dims.length !== 2) {
    throw new Error('Input must be 2D or 3D');
  }

  if (skip.dims.length !== 3 && skip.dims.length !== 2) {
    throw new Error('Skip must be 2D or 3D');
  }

  const hiddenSize = input.dims[input.dims.length - 1];
  const sequenceLength = input.dims[input.dims.length - 2];
  if (skip.dims[skip.dims.length - 1] !== hiddenSize) {
    throw new Error('Skip must have the same hidden size as input');
  }
  if (skip.dims[skip.dims.length - 2] !== sequenceLength) {
    throw new Error('Skip must have the same sequence length as input');
  }

  if (gamma.dims.length !== 1) {
    throw new Error('Gamma must be 1D');
  }
  if (gamma.dims[gamma.dims.length - 1] !== hiddenSize) {
    throw new Error('Gamma must have the same hidden size as input');
  }
  if (inputs.length > 3) {
    const beta: TensorView = inputs[3];
    if (beta.dims.length !== 1) {
      throw new Error('Beta must be 1D');
    }
    if (beta.dims[beta.dims.length - 1] !== hiddenSize) {
      throw new Error('Beta must have the same hidden size as input');
    }
  }
  if (inputs.length > 4) {
    const bias: TensorView = inputs[4];
    if (bias.dims.length !== 1) {
      throw new Error('Bias must be 1D');
    }
    if (bias.dims[bias.dims.length - 1] !== hiddenSize) {
      throw new Error('Bias must have the same hidden size as input');
    }
  }
};

const createSkipLayerNormProgramInfo = (
  inputs: readonly TensorView[],
  attributes: SkipLayerNormAttributes,
  outputCount: number,
  isTraining: boolean,
): ProgramInfo => {
  const simplified = attributes.simplified;

  const inputShape = inputs[0].dims;
  const inputSize = ShapeUtil.size(inputShape);
  const outputShape = inputShape;
  const outputSize = inputSize;
  const hiddenSize = inputShape.slice(-1)[0];
  const meanInvStdDevDim = isTraining ? inputShape.slice(0, -1).concat(1) : [];
  const hasBetaInput = !simplified && inputs.length > 3;
  const hasBiasInput = inputs.length > 4;
  const hasMeanOutput = isTraining && outputCount > 1;
  const hasInvStdDevOutput = isTraining && outputCount > 2;
  const hasInputSkipBiasSumOutput = outputCount > 3;
  const workgroupSize = 64;

  const components = getMaxComponents(hiddenSize);

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: components },
    { type: DataType.uint32, data: hiddenSize },
    { type: DataType.float, data: attributes.epsilon },
  ];
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const uniformsArray: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'components', type: 'u32' },
      { name: 'hidden_size', type: 'u32' },
      { name: 'epsilon', type: 'f32' },
    ];
    const variables = [
      inputVariable('x', inputs[0].dataType, inputs[0].dims, components),
      inputVariable('skip', inputs[1].dataType, inputs[1].dims, components),
      inputVariable('gamma', inputs[2].dataType, inputs[2].dims, components),
    ];
    if (hasBetaInput) {
      variables.push(inputVariable('beta', inputs[3].dataType, inputs[3].dims, components));
    }
    if (hasBiasInput) {
      variables.push(inputVariable('bias', inputs[4].dataType, inputs[4].dims, components));
    }
    variables.push(outputVariable('output', inputs[0].dataType, outputShape, components));
    if (hasMeanOutput) {
      variables.push(outputVariable('mean_output', DataType.float, meanInvStdDevDim));
    }
    if (hasInvStdDevOutput) {
      variables.push(outputVariable('inv_std_output', DataType.float, meanInvStdDevDim));
    }
    if (hasInputSkipBiasSumOutput) {
      variables.push(outputVariable('input_skip_bias_sum', inputs[0].dataType, outputShape, components));
    }
    const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);
    const vecDataType = tensorTypeToWsglStorageType(DataType.float, components);
    return `

      ${shaderHelper.registerUniforms(uniformsArray).declareVariables(...variables)}
      var<workgroup> sum_shared : array<${vecDataType}, ${workgroupSize}>;
      var<workgroup> sum_squared_shared : array<${vecDataType}, ${workgroupSize}>;

      ${shaderHelper.mainStart([workgroupSize, 1, 1])}
        let ix = local_id.x;
        let iy = global_id.x / ${workgroupSize};

        let hidden_size_vectorized: u32 = uniforms.hidden_size / uniforms.components;
        var stride = hidden_size_vectorized / ${workgroupSize};
        let offset = ix * stride + iy * hidden_size_vectorized;
        let offset1d = stride * ix;
        if (ix == ${workgroupSize - 1}) {
          stride = hidden_size_vectorized - stride * ix;
        }
        for (var i: u32 = 0; i < stride; i++) {
          let skip_value = skip[offset + i];
          let bias_value = ${hasBiasInput ? 'bias[offset1d + i]' : dataType + '(0.0)'};
          let input_value = x[offset + i];
          let value = input_value + skip_value + bias_value;
          ${hasInputSkipBiasSumOutput ? 'input_skip_bias_sum[offset + i] = value;' : ''}
          output[offset + i] = value;
          let f32_value = ${castToF32(dataType, components, 'value')};
          sum_shared[ix] += f32_value;
          sum_squared_shared[ix] += f32_value * f32_value;
        }
        workgroupBarrier();

        var reduce_size : u32 = ${workgroupSize};
        for (var curr_size = reduce_size >> 1;  curr_size > 0; curr_size = reduce_size >> 1) {
          reduce_size = curr_size + (reduce_size & 1);
          if (ix < curr_size) {
            sum_shared[ix] += sum_shared[ix + reduce_size];
            sum_squared_shared[ix] += sum_squared_shared[ix + reduce_size];
          }
          workgroupBarrier();
        }

        let sum = sum_shared[0];
        let square_sum = sum_squared_shared[0];
        let mean = ${sumVector('sum', components)} / f32(uniforms.hidden_size);
        let inv_std_dev = inverseSqrt(${sumVector('square_sum', components)} / f32(uniforms.hidden_size) ${
          simplified ? '' : '- mean * mean'
        } + uniforms.epsilon);
        ${hasMeanOutput ? 'mean_output[global_idx] = mean;' : ''}
        ${hasInvStdDevOutput ? 'inv_std_output[global_idx] = inv_std_dev;' : ''}

        for (var i: u32 = 0; i < stride; i++) {
          output[offset + i] = (output[offset + i] ${simplified ? '' : `- ${dataType}(mean)`}) *
            ${dataType}(inv_std_dev) * gamma[offset1d + i]
            ${hasBetaInput ? '+ beta[offset1d + i]' : ''};
        }
      }`;
  };
  const outputs = [{ dims: outputShape, dataType: inputs[0].dataType }];
  if (outputCount > 1) {
    outputs.push({ dims: meanInvStdDevDim, dataType: DataType.float });
  }
  if (outputCount > 2) {
    outputs.push({ dims: meanInvStdDevDim, dataType: DataType.float });
  }
  if (outputCount > 3) {
    outputs.push({ dims: inputShape, dataType: inputs[0].dataType });
  }
  return {
    name: 'SkipLayerNormalization',
    shaderCache: {
      hint: `${components};${hasMeanOutput};${hasInvStdDevOutput};${hasInputSkipBiasSumOutput}`,
      inputDependencies: inputs.map((_input, _index) => 'type'),
    },
    getShaderSource,
    getRunData: () => ({
      outputs,
      dispatchGroup: {
        x: Math.ceil(outputSize / hiddenSize),
      },
      programUniforms,
    }),
  };
};

export const skipLayerNorm = (context: ComputeContext, attributes: SkipLayerNormAttributes): void => {
  // TODO: initialize isTraining from ComputeContext
  const isTraining = false;
  validateInputs(context.inputs);
  // Mean and InvStdDev are only used in training mode and are not required for inference.
  // They are added here for completeness only.
  const outputs = [0];
  if (context.outputCount > 1) {
    outputs.push(isTraining ? 1 : -3);
  }
  if (context.outputCount > 2) {
    outputs.push(isTraining ? 2 : -3);
  }
  if (context.outputCount > 3) {
    outputs.push(3);
  }
  context.compute(createSkipLayerNormProgramInfo(context.inputs, attributes, context.outputCount, isTraining), {
    outputs,
  });
};
