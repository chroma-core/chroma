// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getElementAt,
  IndicesHelper,
  inputVariable,
  outputVariable,
  ShaderHelper,
  UniformDataElementType,
  UniformsArrayType,
} from './common';

interface PadAttributes {
  // 0-constant, 1-reflect, 2-edge, 3-wrap
  readonly mode: number;
  readonly value: number;
  readonly pads: number[];
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length < 1) {
    throw new Error('Too few inputs');
  }
  if (inputs[0].dataType !== DataType.float && inputs[0].dataType !== DataType.float16) {
    throw new Error('Input type must be float or float16.');
  }

  if (inputs.length >= 2) {
    let validPads = inputs[0].dims.length * 2 === inputs[1].dims[0];
    if (inputs.length === 4) {
      validPads = inputs[3].dims[0] * 2 === inputs[1].dims[0];
    }
    if (!validPads) {
      throw new Error('The pads should be a 1D tensor of shape [2 * input_rank] or [2 * num_axes].');
    }
  }
};

const getPadConstant = (output: IndicesHelper, inputRank: number, padsLength: number): string => {
  let block = '';
  for (let i = inputRank - 1; i >= 0; --i) {
    block += `
            k = i32(${output.indicesGet('indices', i)}) - ${getElementAt('uniforms.pads', i, padsLength)};
            if (k < 0) {
              break;
            }
            if (k >= i32(${getElementAt('uniforms.x_shape', i, inputRank)})) {
              break;
            }
            offset += k * i32(${getElementAt('uniforms.x_strides', i, inputRank)});
        `;
  }

  return `
          value = ${output.type.value}(uniforms.constant_value);
          for (var i = 0; i < 1; i++) {
            var offset = 0;
            var k = 0;
            ${block}
            value = x[offset];
          }
      `;
};

const getPadReflect = (output: IndicesHelper, inputRank: number, padsLength: number): string => {
  let block = '';
  for (let i = inputRank - 1; i >= 0; --i) {
    block += `
                k = i32(${output.indicesGet('indices', i)}) - ${getElementAt('uniforms.pads', i, padsLength)};
                if (k < 0) {
                  k = -k;
                }
                {
                  let _2n_1 = 2 * (i32(${getElementAt('uniforms.x_shape', i, inputRank)}) - 1);
                  k = k % _2n_1;
                  if(k >= i32(${getElementAt('uniforms.x_shape', i, inputRank)})) {
                    k = _2n_1 - k;
                  }
                }
                offset += k * i32(${getElementAt('uniforms.x_strides', i, inputRank)});
            `;
  }

  return `
              var offset = 0;
              var k = 0;
              ${block}
              value = x[offset];
          `;
};

const getPadEdge = (output: IndicesHelper, inputRank: number, padsLength: number): string => {
  let block = '';
  for (let i = inputRank - 1; i >= 0; --i) {
    block += `
                k = i32(${output.indicesGet('indices', i)}) - ${getElementAt('uniforms.pads', i, padsLength)};
                if (k < 0) {
                  k = 0;
                }
                if (k >= i32(${getElementAt('uniforms.x_shape', i, inputRank)})) {
                  k = i32(${getElementAt('uniforms.x_shape', i, inputRank)}) - 1;
                }
                offset += k * i32(${getElementAt('uniforms.x_strides', i, inputRank)});
            `;
  }

  return `
              var offset = 0;
              var k = 0;
              ${block}
              value = x[offset];
          `;
};

const getPadWrap = (output: IndicesHelper, inputRank: number, padsLength: number): string => {
  let block = '';
  for (let i = inputRank - 1; i >= 0; --i) {
    block += `
                k = i32(${output.indicesGet('indices', i)}) - ${getElementAt('uniforms.pads', i, padsLength)};
                if (k < 0)  {
                  k += i32(${getElementAt('uniforms.x_shape', i, inputRank)}]);
                }
                if (k >= i32(${getElementAt('uniforms.x_shape', i, inputRank)})) {
                  k -= i32(${getElementAt('uniforms.x_shape', i, inputRank)});
                }
                offset += k * i32(${getElementAt('uniforms.x_strides', i, inputRank)});
            `;
  }

  return `
              var offset = 0;
              var k = 0;
              ${block}
              value = x[offset];
          `;
};

const getPadSnippet = (output: IndicesHelper, inputRank: number, attributes: PadAttributes): string => {
  switch (attributes.mode) {
    case 0:
      return getPadConstant(output, inputRank, attributes.pads.length);
    case 1:
      return getPadReflect(output, inputRank, attributes.pads.length);
    case 2:
      return getPadEdge(output, inputRank, attributes.pads.length);
    case 3:
      return getPadWrap(output, inputRank, attributes.pads.length);
    default:
      throw new Error('Invalid mode');
  }
};

const createPadProgramInfo = (inputs: readonly TensorView[], attributes: PadAttributes): ProgramInfo => {
  const outputShape = ShapeUtil.padShape(inputs[0].dims.slice(), attributes.pads);
  const inputDims = inputs[0].dims;
  const outputSize = ShapeUtil.size(outputShape);
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.int32, data: attributes.pads },
  ];

  const isValueFromInput = inputs.length >= 3 && inputs[2].data;
  if (attributes.mode === 0) {
    programUniforms.push({ type: isValueFromInput ? inputs[2].dataType : DataType.float, data: attributes.value });
  }

  programUniforms.push(...createTensorShapeVariables(inputs[0].dims, outputShape));
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['rank'];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const output = outputVariable('output', inputs[0].dataType, outputShape.length);
    const input = inputVariable('x', inputs[0].dataType, inputDims.length);
    const dataType = input.type.value;
    const padSnippet = getPadSnippet(output, inputDims.length, attributes);
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'pads', type: 'i32', length: attributes.pads.length },
    ];
    if (attributes.mode === 0) {
      uniforms.push({ name: 'constant_value', type: (isValueFromInput ? dataType : 'f32') as UniformDataElementType });
    }

    return `
            ${shaderHelper.registerUniforms(uniforms).declareVariables(input, output)}
            ${shaderHelper.mainStart()}
            ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}

            let indices = ${output.offsetToIndices('global_idx')};

            var value = ${dataType}(0);
            ${padSnippet}
            output[global_idx] = value;
        }`;
  };

  return {
    name: 'Pad',
    shaderCache: { hint: `${attributes.mode}${isValueFromInput}`, inputDependencies },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(ShapeUtil.size(outputShape) / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

const createPadAttributesFromInputs = (inputs: readonly TensorView[], attributes: PadAttributes): PadAttributes => {
  if (inputs.length > 1) {
    const bigInt64Pads = inputs[1].getBigInt64Array();
    const value =
      inputs.length >= 3 && inputs[2].data
        ? inputs[2].dataType === DataType.float16
          ? inputs[2].getUint16Array()[0]
          : inputs[2].getFloat32Array()[0]
        : 0.0;

    const inputRank = inputs[0].dims.length;
    const updatePads = new Int32Array(2 * inputRank).fill(0);
    if (inputs.length >= 4) {
      const axes = inputs[3].getBigInt64Array();
      for (let i = 0; i < axes.length; i++) {
        updatePads[Number(axes[i])] = Number(bigInt64Pads[i]);
        updatePads[Number(axes[i]) + inputRank] = Number(bigInt64Pads[i + axes.length]);
      }
    } else {
      bigInt64Pads.forEach((v, i) => (updatePads[Number(i)] = Number(v)));
    }

    const pads: number[] = [];
    updatePads.forEach((v) => pads.push(v));

    return { mode: attributes.mode, value, pads };
  } else {
    return attributes;
  }
};

export const pad = (context: ComputeContext, attributes: PadAttributes): void => {
  validateInputs(context.inputs);
  const updatedAttributes = createPadAttributesFromInputs(context.inputs, attributes);
  context.compute(createPadProgramInfo(context.inputs, updatedAttributes), { inputs: [0] });
};
