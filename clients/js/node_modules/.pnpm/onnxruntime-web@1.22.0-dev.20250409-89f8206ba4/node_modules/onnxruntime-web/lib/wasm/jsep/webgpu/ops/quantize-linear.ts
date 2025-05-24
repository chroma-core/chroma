// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  UniformsArrayType,
} from './common';

export interface DequantizeLinerAttributes extends AttributeWithCacheKey {
  axis: number;
  blockSize: number;
}

const validateInputs = (inputs: readonly TensorView[], attributes: DequantizeLinerAttributes): void => {
  if (inputs.length < 2 || inputs.length > 3) {
    throw new Error('DequantizeLinear requires 2 or 3 inputs.');
  }
  if (inputs.length === 3 && inputs[1].dims === inputs[2].dims) {
    throw new Error('x-scale and x-zero-point must have the same shape.');
  }
  if (inputs.length === 3 && inputs[0].dataType !== inputs[2].dataType) {
    throw new Error('x and x-zero-point must have the same data type.');
  }
  if (inputs[0].dataType === DataType.int32 && inputs.length > 2) {
    throw new Error('In the case of dequantizing int32 there is no zero point.');
  }
  if (inputs[1].dims.length !== 0 && inputs[1].dims.length !== 1 && inputs[1].dims.length !== inputs[0].dims.length) {
    throw new Error('scale input must be a scalar, a 1D tensor, or have the same rank as the input tensor.');
  }
  // validate scale and zero-point input shapes
  if (inputs.length > 2) {
    // zero-point input type should be the same as input data type.
    if (inputs[0].dataType !== inputs[2].dataType) {
      throw new Error('x and x-zero-point must have the same data type.');
    }
    // Scale and zero-point inputs must have the same shape
    if (inputs[1].dims.length !== inputs[2].dims.length) {
      throw new Error('scale and zero-point inputs must have the same rank.');
    }
    if (!inputs[1].dims.map((d, i) => d === inputs[2].dims[i]).reduce((a, b) => a && b, true)) {
      throw new Error('scale and zero-point inputs must have the same shape.');
    }
  }
  // Validate blockSize
  if (attributes.blockSize > 0) {
    // Block qunatization
    if (inputs[1].dims.length === 0 || (inputs[1].dims.length === 1 && inputs[1].dims[0] === 1)) {
      throw new Error('blockSize must be set only for block quantization.');
    }
    if (
      !inputs[1].dims.map((d, i) => i === attributes.axis || d === inputs[0].dims[i]).reduce((a, b) => a && b, true)
    ) {
      throw new Error('For block qunatization, scale input shape to match the input shape except for the axis');
    }
    // Scale input rank should be same as the input rank
    if (inputs[1].dims.length !== inputs[0].dims.length) {
      throw new Error('For block qunatization the scale input rank must be the same as the x rank.');
    }
    const dI = inputs[0].dims[attributes.axis];
    const si = inputs[1].dims[attributes.axis];
    if (attributes.blockSize < Math.ceil(dI / si) || attributes.blockSize > Math.ceil(dI / (si - 1) - 1)) {
      throw new Error('blockSize must be with in the range [ceil(dI / Si), ceil(dI / (Si - 1) - 1)].');
    }
  }
};

const createDequantizeLinearProgramInfo = (
  inputs: readonly TensorView[],
  attributes: DequantizeLinerAttributes,
): ProgramInfo => {
  const axis = ShapeUtil.normalizeAxis(attributes.axis, inputs[0].dims.length);
  const inputType = inputs[0].dataType;
  const isSigned = inputType === DataType.int8;
  const outputShape = inputs[0].dims; // output shape is same as the input shape
  const dataType = inputs[1].dataType; // output type is same as the the scale input type
  const outputSize = ShapeUtil.size(outputShape);
  const isPacked = inputType === DataType.int8 || inputType === DataType.uint8;
  const inputShape = isPacked ? [Math.ceil(ShapeUtil.size(inputs[0].dims) / 4)] : inputs[0].dims;
  const scaleShape = inputs[1].dims;
  const zeroPointInput = inputs.length > 2 ? inputs[2] : undefined;
  const zeroPointShape = zeroPointInput
    ? isPacked
      ? [Math.ceil(ShapeUtil.size(zeroPointInput.dims) / 4)]
      : zeroPointInput.dims
    : undefined;
  // Scales input is a scaler for per-tensor/per-layer quantization, 1-D tensor for per-axis quantization
  // or tensor with same rank as input for blocked quantization.
  const perLayerQuantization = scaleShape.length === 0 || (scaleShape.length === 1 && scaleShape[0] === 1);
  const perAxisQuantization = perLayerQuantization === false && scaleShape.length === 1;
  // Left unnecessary commented-out assignment for documentation
  // const blockQuantization = perLayerQuantization === false && perAxisQuantization === false;
  const maxComponents = getMaxComponents(outputSize);
  const useComponents = perLayerQuantization && (!isPacked || maxComponents === 4);
  const components = useComponents ? maxComponents : 1;
  const inputComponent = useComponents && !isPacked ? maxComponents : 1;
  const input = inputVariable('input', isPacked ? DataType.uint32 : inputType, inputShape.length, inputComponent);
  const scale = inputVariable('scale', dataType, scaleShape.length);
  const zeroPoint = zeroPointInput
    ? inputVariable('zero_point', isPacked ? DataType.uint32 : inputType, zeroPointShape!.length)
    : undefined;
  const output = outputVariable('output', dataType, outputShape.length, components);
  const inputVariables = [input, scale];
  if (zeroPoint) {
    inputVariables.push(zeroPoint);
  }
  const inputShapes = [inputShape, scaleShape];
  if (zeroPointInput) {
    inputShapes.push(zeroPointShape!);
  }
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize / components },
    { type: DataType.uint32, data: axis },
    { type: DataType.uint32, data: attributes.blockSize },
    ...createTensorShapeVariables(...inputShapes, outputShape),
  ];
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'axis', type: 'u32' },
      { name: 'block_size', type: 'u32' },
    ];
    return `
      ${shaderHelper.registerUniforms(uniforms).declareVariables(...inputVariables, output)}
      ${shaderHelper.mainStart()}
          ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
          let output_indices = ${output.offsetToIndices('global_idx')};

          // Set input x
          ${(() => {
            if (isPacked) {
              return `
            let input = ${input.getByOffset('global_idx / 4')};
            let x_vec = ${isSigned ? 'unpack4xI8(input)' : 'unpack4xU8(input)'};
            let x_value = ${components === 1 ? 'x_vec[global_idx % 4]' : 'x_vec'};`;
            } else {
              return `let x_value = ${input.getByOffset('global_idx')};`;
            }
          })()};

          // Set scale input
          ${(() => {
            if (perLayerQuantization) {
              // scale input is a scalar ()
              return `let scale_value= ${scale.getByOffset('0')}`;
            } else if (perAxisQuantization) {
              // scale input is a 1D tensor
              return `
            let scale_index = ${output.indicesGet('output_indices', 'uniforms.axis')};
            let scale_value= ${scale.getByOffset('scale_index')};`;
            } else {
              // Block quantization. Scale input rank is same as input/output rank.
              return `
            var scale_indices: ${scale.type.indices} = output_indices;
            let index = ${scale.indicesGet('scale_indices', 'uniforms.axis')} / uniforms.block_size;
            ${scale.indicesSet('scale_indices', 'uniforms.axis', 'index')};
            let scale_value= ${scale.getByIndices('scale_indices')};`;
            }
          })()};

          // Set zero-point input
          ${(() => {
            if (zeroPoint) {
              if (perLayerQuantization) {
                // zero-point input is a scalar
                if (isPacked) {
                  return `
                let zero_point_input = ${zeroPoint.getByOffset('0')};
                let zero_point_vec =  ${isSigned ? 'unpack4xI8(zero_point_input)' : 'unpack4xU8(zero_point_input)'};
                let zero_point_value= zero_point_vec[0]`;
                } else {
                  return `let zero_point_value = ${zeroPoint.getByOffset('0')}`;
                }
              } else if (perAxisQuantization) {
                // zero-point input is a 1D tensor
                if (isPacked) {
                  return `
                let zero_point_index = ${output.indicesGet('output_indices', 'uniforms.axis')};
                let zero_point_input = ${zeroPoint.getByOffset('zero_point_index / 4')};
                let zero_point_vec =  ${isSigned ? 'unpack4xI8(zero_point_input)' : 'unpack4xU8(zero_point_input)'};
                let zero_point_value = zero_point_vec[zero_point_index % 4]`;
                } else {
                  return `
                let zero_point_index = ${output.indicesGet('output_indices', 'uniforms.axis')};
                let zero_point_value = ${zeroPoint.getByOffset('zero_point_index')};`;
                }
              } else {
                // BlockedQuantization. The zero-point input shape is same as the input shape except along axis.
                if (isPacked) {
                  return `
                let zero_point_offset = ${scale.indicesToOffset('scale_indices')};
                let zero_point_input = ${zeroPoint.getByOffset('zero_point_offset / 4')};
                let zero_point_vec = ${isSigned ? 'unpack4xI8(zero_point_input)' : 'unpack4xU8(zero_point_input)'};
                let zero_point_value = zero_point_vec[zero_point_offset % 4];`;
                } else {
                  return `let zero_point_value = ${zeroPoint.getByIndices('scale_indices')};`;
                }
              }
            } else {
              return `let zero_point_value = ${isPacked ? (isSigned ? 'i32' : 'u32') : input.type.value}(0);`;
            }
          })()};
      // Compute and write output
      ${output.setByOffset('global_idx', `${output.type.value}(x_value - zero_point_value) * scale_value`)};
      }`;
  };
  return {
    name: 'DequantizeLinear',
    shaderCache: {
      hint: attributes.cacheKey,
      inputDependencies: zeroPoint ? ['rank', 'rank', 'rank'] : ['rank', 'rank'],
    },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / components / 64), y: 1, z: 1 },
      programUniforms,
    }),
  };
};

export const dequantizeLinear = (context: ComputeContext, attributes: DequantizeLinerAttributes): void => {
  validateInputs(context.inputs, attributes);
  context.compute(createDequantizeLinearProgramInfo(context.inputs, attributes));
};

export const parseDequantizeLinearAttributes = (attributes: Record<string, unknown>): DequantizeLinerAttributes =>
  createAttributeWithCacheKey({ axis: attributes.axis as number, blockSize: attributes.blockSize as number });
