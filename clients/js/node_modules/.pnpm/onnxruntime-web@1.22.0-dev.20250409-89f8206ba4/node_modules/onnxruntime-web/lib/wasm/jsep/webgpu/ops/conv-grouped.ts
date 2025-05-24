// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  tensorTypeToWsglStorageType,
  UniformsArrayType,
} from './common';
import { ConvAttributes } from './conv';
import { appendActivationUniforms, appendActivationUniformsData, getActivationSnippet } from './fuse-utils';

/**
 * naive grouped conv implementation, supports 1d/2d conv
 * @param squeezeOutputShapeFunction - an optional function to squeeze the output shape, only used in conv1d
 */
export const createGroupedConvProgramInfo = (
  inputs: readonly TensorView[],
  attributes: ConvAttributes,
  outputShape: readonly number[],
  squeezeOutputShapeFunction?: (shape: readonly number[]) => number[],
): ProgramInfo => {
  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += b[output_channel];' : '';
  const xShape = inputs[0].dims;
  const wShape = inputs[1].dims;

  const isChannelLast = attributes.format === 'NHWC';
  const outputChannels = isChannelLast ? outputShape[3] : outputShape[1];
  const outputChannelsPerGroup = outputChannels / attributes.group;
  const components = isChannelLast && outputChannelsPerGroup >= 4 ? getMaxComponents(outputChannels) : 1;
  const outputSize = ShapeUtil.size(outputShape) / components;

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: attributes.dilations },
    { type: DataType.uint32, data: [attributes.strides[0], attributes.strides[1]] },
    { type: DataType.uint32, data: [attributes.pads[0], attributes.pads[1]] },
    { type: DataType.uint32, data: outputChannelsPerGroup },
  ];
  appendActivationUniformsData(attributes, programUniforms);
  programUniforms.push(
    ...createTensorShapeVariables(xShape, [wShape[0], wShape[1], wShape[2], wShape[3] / components]),
  );
  const inputDependencies: ProgramInputTensorInfoDependency[] = hasBias ? ['rank', 'rank', 'rank'] : ['rank', 'rank'];
  programUniforms.push(
    ...createTensorShapeVariables([outputShape[0], outputShape[1], outputShape[2], outputShape[3] / components]),
  );

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const output = outputVariable('output', inputs[0].dataType, outputShape.length, components);
    const baseType = tensorTypeToWsglStorageType(output.type.tensor);
    const applyActivation = getActivationSnippet(attributes, output.type.value, baseType);
    const x = inputVariable('x', inputs[0].dataType, xShape.length);
    const w = inputVariable('w', inputs[1].dataType, wShape.length, components);
    const inputVars = [x, w];
    if (hasBias) {
      inputVars.push(inputVariable('b', inputs[2].dataType, inputs[2].dims, components));
    }

    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'dilations', type: 'u32', length: attributes.dilations.length },
      { name: 'strides', type: 'u32', length: 2 },
      { name: 'pads', type: 'u32', length: 2 },
      { name: 'output_channels_per_group', type: 'u32' },
    ];
    appendActivationUniforms(attributes, uniforms);

    const calculateResult = isChannelLast
      ? `
      for (var wHeight: u32 = 0u; wHeight < uniforms.w_shape[0]; wHeight++) {
        let xHeight = xRCCorner.x + wHeight * uniforms.dilations[0];

        if (xHeight < 0u || xHeight >= uniforms.x_shape[1]) {
          continue;
        }

        for (var wWidth: u32 = 0u; wWidth < uniforms.w_shape[1]; wWidth++) {
          let xWidth = xRCCorner.y + wWidth * uniforms.dilations[1];
          if (xWidth < 0u || xWidth >= uniforms.x_shape[2]) {
            continue;
          }

          for (var wInChannel: u32 = 0u; wInChannel < uniforms.w_shape[2]; wInChannel++) {
            let input_channel = in_channel_offset + wInChannel;
            let xVal = ${x.get('batch', 'xHeight', 'xWidth', 'input_channel')};
            let wVal = ${w.get('wHeight', 'wWidth', 'wInChannel', 'output_channel')};
            value += xVal * wVal;
          }
        }
      }
      `
      : `
      for (var wInChannel: u32 = 0u; wInChannel < uniforms.w_shape[1]; wInChannel++) {
        let input_channel = in_channel_offset + wInChannel;
        for (var wHeight: u32 = 0u; wHeight < uniforms.w_shape[2]; wHeight++) {
          let xHeight = xRCCorner.x + wHeight * uniforms.dilations[0];

          if (xHeight < 0u || xHeight >= uniforms.x_shape[2]) {
            continue;
          }

          for (var wWidth: u32 = 0u; wWidth < uniforms.w_shape[3]; wWidth++) {
            let xWidth = xRCCorner.y + wWidth * uniforms.dilations[1];
            if (xWidth < 0u || xWidth >= uniforms.x_shape[3]) {
              continue;
            }

            let xVal = ${x.get('batch', 'input_channel', 'xHeight', 'xWidth')};
            let wVal = ${w.get('output_channel', 'wInChannel', 'wHeight', 'wWidth')};
            value += xVal * wVal;
          }
        }
      }
      `;
    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...inputVars, output)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}

    let outputIndices = ${output.offsetToIndices('global_idx')};
    let batch: u32 = outputIndices[0];
    let output_channel: u32 = outputIndices[${isChannelLast ? 3 : 1}];
    let xRCCorner: vec2<u32> = vec2<u32>(outputIndices[${isChannelLast ? 1 : 2}], outputIndices[${
      isChannelLast ? 2 : 3
    }]) * uniforms.strides - uniforms.pads;
    let group_id: u32 = output_channel * ${components} / uniforms.output_channels_per_group;
    var in_channel_offset = group_id * uniforms.w_shape[${isChannelLast ? 2 : 1}];

    var value: ${output.type.value} = ${output.type.value}(0);
    ${calculateResult}
    ${processBias}
    ${applyActivation}
    ${output.setByOffset('global_idx', 'value')}
  }`;
  };
  return {
    name: 'GroupedConv',
    shaderCache: { hint: `${attributes.cacheKey}_${components}`, inputDependencies },
    getRunData: () => ({
      outputs: [
        {
          dims: squeezeOutputShapeFunction ? squeezeOutputShapeFunction(outputShape) : outputShape,
          dataType: inputs[0].dataType,
        },
      ],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const createGroupedConvVectorizeProgramInfo = (
  inputs: readonly TensorView[],
  attributes: ConvAttributes,
  outputShape: readonly number[],
  squeezeOutputShapeFunction?: (shape: readonly number[]) => number[],
): ProgramInfo => {
  const hasBias = inputs.length > 2;
  const components = getMaxComponents(outputShape[3]);
  const outputNumber = getMaxComponents(outputShape[2]);
  const outputSize = ShapeUtil.size(outputShape) / components / outputNumber;
  const xShape = [inputs[0].dims[0], inputs[0].dims[1], inputs[0].dims[2], inputs[0].dims[3] / components];
  const wShape = [inputs[1].dims[0], inputs[1].dims[1], inputs[1].dims[2], inputs[1].dims[3] / components];
  const outputShapeInShader = [outputShape[0], outputShape[1], outputShape[2], outputShape[3] / components];

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.int32, data: [attributes.strides[0], attributes.strides[1]] },
    { type: DataType.int32, data: [attributes.pads[0], attributes.pads[1]] },
  ];
  appendActivationUniformsData(attributes, programUniforms);
  programUniforms.push(...createTensorShapeVariables(xShape, wShape, outputShapeInShader));
  const xNumber = (outputNumber - 1) * attributes.strides[1] + wShape[1];
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const output = outputVariable('output', inputs[0].dataType, outputShapeInShader.length, components);
    const baseType = tensorTypeToWsglStorageType(output.type.tensor);
    const applyActivation = getActivationSnippet(attributes, output.type.value, baseType);
    const x = inputVariable('x', inputs[0].dataType, xShape.length, components);
    const w = inputVariable('w', inputs[1].dataType, wShape.length, components);
    const inputVars = [x, w];
    if (hasBias) {
      inputVars.push(inputVariable('b', inputs[2].dataType, inputs[2].dims, components));
    }
    const processBias = hasBias ? 'value += b[output_channel];' : '';
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'strides', type: 'i32', length: 2 },
      { name: 'pads', type: 'i32', length: 2 },
    ];
    appendActivationUniforms(attributes, uniforms);
    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...inputVars, output)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
    let width0 = uniforms.output_shape[3];
    let output_channel = global_idx % width0;
    var index1 = global_idx / width0;
    let width1 = uniforms.output_shape[2] / ${outputNumber}u;
    let col = (index1 % width1) * ${outputNumber}u;
    index1 = index1 / width1;
    let row = index1 % uniforms.output_shape[1];
    let batch = index1 / uniforms.output_shape[1];

    let x_corner = vec2<i32>(i32(row), i32(col)) * uniforms.strides - uniforms.pads;

    var x_vals: array<${x.type.value}, ${xNumber}>;
    var values: array<${output.type.value}, ${outputNumber}>;
    let input_channel = output_channel;
    // Use constant instead of uniform can give better performance for w's height/width.
    for (var w_height: u32 = 0u; w_height < ${wShape[0]}; w_height++) {
      let x_height = x_corner.x + i32(w_height);
      if (x_height >= 0 && u32(x_height) < uniforms.x_shape[1]) {
        for (var i = 0; i < ${xNumber}; i++) {
          let x_width = x_corner.y + i;
          if (x_width >= 0 && u32(x_width) < uniforms.x_shape[2]) {
            x_vals[i] = ${x.get('batch', 'u32(x_height)', 'u32(x_width)', 'input_channel')};
          } else {
            x_vals[i] = ${x.type.value}(0);
          }
        }
        for (var w_width: u32 = 0u; w_width < ${wShape[1]}; w_width++) {
          let w_val = ${w.get('w_height', 'w_width', '0', 'output_channel')};
          for (var i = 0u; i < ${outputNumber}u; i++) {
            values[i] = fma(x_vals[i * u32(uniforms.strides[1]) + w_width], w_val, values[i]);
          }
        }
      }
    }

    for (var i = 0u; i < ${outputNumber}u; i++) {
      var value = values[i];
      ${processBias}
      ${applyActivation}
      ${output.set('batch', 'row', 'col + i', 'output_channel', 'value')};
    }
  }`;
  };

  return {
    name: 'GroupedConv-Vectorize',
    shaderCache: {
      hint: `${attributes.cacheKey};${components};${outputNumber};${xNumber};${wShape[0]};${wShape[1]}`,
      inputDependencies: hasBias ? ['rank', 'rank', 'type'] : ['rank', 'rank'],
    },
    getRunData: () => ({
      outputs: [
        {
          dims: squeezeOutputShapeFunction ? squeezeOutputShapeFunction(outputShape) : outputShape,
          dataType: inputs[0].dataType,
        },
      ],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};
