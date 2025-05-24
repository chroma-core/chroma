// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from 'onnxruntime-common';

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { PoolConvUtil, ShapeUtil } from '../../util';
import { AttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getElementAt,
  IndicesHelper,
  inputVariable,
  outputVariable,
  ShaderHelper,
  UniformsArrayType,
} from './common';

// TODO: support:
// - ceil_mode                 "test_maxpool_2d_ceil"
// - storage_order             "test_maxpool_with_argmax_2d_precomputed_strides"
// - [MaxPool] dilations       "test_maxpool_2d_dilations"
// - [MaxPool] output[1]       "test_maxpool_with_argmax_2d_precomputed_pads"

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (env.webgpu.validateInputContent && (!inputs || inputs.length !== 1)) {
    throw new Error('Pool ops requires 1 input.');
  }
};

const getAdjustedPoolAttributesAndOutputShape = <AttributeType extends AveragePoolAttributes | MaxPoolAttributes>(
  input: TensorView,
  attributes: AttributeType,
  isGlobalOperator: boolean,
): [AttributeType, number[]] => {
  const isChannelsLast = attributes.format === 'NHWC';
  const inputShapeAsChannelFirst = input.dims.slice();
  if (isChannelsLast) {
    inputShapeAsChannelFirst.splice(1, 0, inputShapeAsChannelFirst.pop()!); // Move channel to the second position.
  }
  const hasDilations = Object.hasOwnProperty.call(attributes, 'dilations');
  const kernelShape = attributes.kernelShape.slice();
  const strides = attributes.strides.slice();
  const dilations: number[] = hasDilations ? (attributes as MaxPoolAttributes).dilations.slice() : [];
  const pads = attributes.pads.slice();
  PoolConvUtil.adjustPoolAttributes(isGlobalOperator, inputShapeAsChannelFirst, kernelShape, strides, dilations, pads);

  const outputShapeAsChannelFirst = PoolConvUtil.computePoolOutputShape(
    isGlobalOperator,
    inputShapeAsChannelFirst,
    strides,
    dilations,
    kernelShape,
    pads,
    attributes.autoPad,
  );

  const newAttributes = Object.assign({}, attributes);
  if (hasDilations) {
    Object.assign(newAttributes, { kernelShape, strides, pads, dilations, cacheKey: attributes.cacheKey });
  } else {
    Object.assign(newAttributes, { kernelShape, strides, pads, cacheKey: attributes.cacheKey });
  }
  const outputShapeAsChannelLast = outputShapeAsChannelFirst.slice();
  outputShapeAsChannelLast.push(outputShapeAsChannelLast.splice(1, 1)[0]);
  return [newAttributes, isChannelsLast ? outputShapeAsChannelLast : outputShapeAsChannelFirst];
};

const getUniformAndPadInfo = <AttributeType extends AveragePoolAttributes | MaxPoolAttributes>(
  outputShape: readonly number[],
  attributes: AttributeType,
): [ProgramUniform[], UniformsArrayType, boolean, boolean, boolean] => {
  const isChannelsLast = attributes.format === 'NHWC';
  const outputSize = ShapeUtil.size(outputShape);
  const kernelSize = ShapeUtil.size(attributes.kernelShape);
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: kernelSize },
  ];
  const uniforms: UniformsArrayType = [
    { name: 'outputSize', type: 'u32' },
    { name: 'kernelSize', type: 'u32' },
  ];
  if (attributes.kernelShape.length <= 2) {
    const kw = attributes.kernelShape[attributes.kernelShape.length - 1];
    const sw = attributes.strides[attributes.strides.length - 1];
    const pwStart = attributes.pads[attributes.pads.length / 2 - 1];
    const pwEnd = attributes.pads[attributes.pads.length - 1];
    const pwStartEndNotZero = !!(pwStart + pwEnd);
    programUniforms.push(
      { type: DataType.uint32, data: kw },
      { type: DataType.uint32, data: sw },
      { type: DataType.uint32, data: pwStart },
      { type: DataType.uint32, data: pwEnd },
    );
    uniforms.push(
      { name: 'kw', type: 'u32' },
      { name: 'sw', type: 'u32' },
      { name: 'pwStart', type: 'u32' },
      { name: 'pwEnd', type: 'u32' },
    );

    let phStartEndNotZero = false;
    if (attributes.kernelShape.length === 2) {
      const kh = attributes.kernelShape[attributes.kernelShape.length - 2];
      const sh = attributes.strides[attributes.strides.length - 2];
      const phStart = attributes.pads[attributes.pads.length / 2 - 2];
      const phEnd = attributes.pads[attributes.pads.length - 2];
      phStartEndNotZero = !!(phStart + phEnd);
      programUniforms.push(
        { type: DataType.uint32, data: kh },
        { type: DataType.uint32, data: sh },
        { type: DataType.uint32, data: phStart },
        { type: DataType.uint32, data: phEnd },
      );

      uniforms.push(
        { name: 'kh', type: 'u32' },
        { name: 'sh', type: 'u32' },
        { name: 'phStart', type: 'u32' },
        { name: 'phEnd', type: 'u32' },
      );
    }
    return [programUniforms, uniforms, true, pwStartEndNotZero, phStartEndNotZero];
  } else {
    if (isChannelsLast) {
      throw new Error('Pooling with kernelShape.length > 2 is not supported for NHWC format.');
    }
    const kernelStrides = ShapeUtil.computeStrides(attributes.kernelShape);
    programUniforms.push(
      { type: DataType.uint32, data: kernelStrides },
      { type: DataType.uint32, data: attributes.pads },
      { type: DataType.uint32, data: attributes.strides },
    );
    uniforms.push(
      { name: 'kernelStrides', type: 'u32', length: kernelStrides.length },
      { name: 'pads', type: 'u32', length: attributes.pads.length },
      { name: 'strides', type: 'u32', length: attributes.strides.length },
    );

    const hasPads = attributes.pads.reduce((sum, cur) => sum + cur);
    return [programUniforms, uniforms, !!hasPads, false, false];
  }
};

const generatePoolingCode = <AttributeType extends AveragePoolAttributes | MaxPoolAttributes>(
  shaderHelper: ShaderHelper,
  x: IndicesHelper,
  rank: number,
  outputShapeRank: number,
  attributes: AttributeType,
  op1: string,
  op2: string,
  start: number,
  uniforms: UniformsArrayType,
  hasPads: boolean,
  pwStartEndNotZero: boolean,
  phStartEndNotZero: boolean,
): string => {
  const isChannelsLast = attributes.format === 'NHWC';
  const dataType = x.type.value;
  const output = outputVariable('output', x.type.tensor, outputShapeRank);

  if (attributes.kernelShape.length <= 2) {
    let codeW = '';
    let codeH = '';
    let codeHEnd = '';
    const dimIdxW = rank - (isChannelsLast ? 2 : 1);
    if (pwStartEndNotZero) {
      codeW = `
                for (var i: u32 = 0u; i < uniforms.kw; i++) {
                  xIndices[${dimIdxW}] = indices[${dimIdxW}] * uniforms.sw - uniforms.pwStart + i;
                  if (xIndices[${dimIdxW}] < 0 || xIndices[${dimIdxW}]
                      >= uniforms.x_shape[${dimIdxW}]) {
                    pad++;
                    continue;
                  }
                  let x_val = x[${x.indicesToOffset('xIndices')}];
                  ${op1}
                }`;
    } else {
      codeW = `
                for (var i: u32 = 0u; i < uniforms.kw; i++) {
                  xIndices[${dimIdxW}] = indices[${dimIdxW}] * uniforms.sw - uniforms.pwStart + i;
                  let x_val = x[${x.indicesToOffset('xIndices')}];
                  ${op1}
                }`;
    }

    if (attributes.kernelShape.length === 2) {
      const dimIdxH = rank - (isChannelsLast ? 3 : 2);
      if (phStartEndNotZero) {
        codeH = `
                for (var j: u32 = 0u; j < uniforms.kh; j++) {
                  xIndices[${dimIdxH}] = indices[${dimIdxH}] * uniforms.sh - uniforms.phStart + j;
                  if (xIndices[${dimIdxH}] < 0 || xIndices[${dimIdxH}] >= uniforms.x_shape[${dimIdxH}]) {
                    pad += i32(uniforms.kw);
                    continue;
                  }
              `;
      } else {
        codeH = `
                for (var j: u32 = 0u; j < uniforms.kh; j++) {
                  xIndices[${dimIdxH}] = indices[${dimIdxH}] * uniforms.sh - uniforms.phStart + j;
                `;
      }
      codeHEnd = `
              }
            `;
    }

    const poolingCode = `
            ${shaderHelper.registerUniforms(uniforms).declareVariables(x, output)}

            ${shaderHelper.mainStart()}
              ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}

              let indices = ${output.offsetToIndices('global_idx')};
              var xIndices = ${output.offsetToIndices('global_idx')};

              var value = ${dataType}(${start});
              var pad = 0;
              ${codeH}
              ${codeW}
              ${codeHEnd}
              ${op2}

              output[global_idx] = value;
            }`;
    return poolingCode;
  } else {
    if (isChannelsLast) {
      throw new Error('Pooling with kernelShape.length > 2 is not supported for NHWC format.');
    }
    const stridesRank = attributes.kernelShape.length;
    const padsRank = attributes.pads.length;
    let padCode = '';
    if (hasPads) {
      padCode = `
                if (xIndices[j] >= uniforms.x_shape[j]) {
                  pad++;
                  isPad = true;
                  break;
                }
              }
              if (!isPad) {
                let x_val = x[${x.indicesToOffset('xIndices')}];
                ${op1}
              }`;
    } else {
      padCode = `
              }
              let x_val = x[${x.indicesToOffset('xIndices')}];
              ${op1}
            `;
    }
    const poolingCode = `
            ${shaderHelper.registerUniforms(uniforms).declareVariables(x, output)}

            ${shaderHelper.mainStart()}
              ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
              let indices = ${output.offsetToIndices('global_idx')};
              var xIndices = ${output.offsetToIndices('global_idx')};

              var offsets: array<u32, ${stridesRank}>;

              var value = ${dataType}(${start});
              var pad = 0;
              var isPad = false;

              for (var i: u32 = 0u; i < uniforms.kernelSize; i++) {
                var offset = i;
                for (var j = 0u; j < ${stridesRank - 1}u; j++) {
                  offsets[j] = offset / ${getElementAt('uniforms.kernelStrides', 'j', stridesRank)};
                  offset -= offsets[j] * ${getElementAt('uniforms.kernelStrides', 'j', stridesRank)};
                }
                offsets[${stridesRank - 1}] = offset;

                isPad = false;
                for (var j = ${rank - stridesRank}u; j < ${rank}u; j++) {
                  xIndices[j] = indices[j] * ${getElementAt(
                    'uniforms.strides',
                    `j - ${rank - stridesRank}u`,
                    stridesRank,
                  )}
                    + offsets[j - ${rank - stridesRank}u] - ${getElementAt('uniforms.pads', 'j - 2u', padsRank)};
                  ${padCode}
              }
              ${op2}

              output[global_idx] = value;
            }`;
    return poolingCode;
  }
};

export interface FormatAttributes {
  readonly format: 'NHWC' | 'NCHW';
}

export interface PoolCommonAttributes extends FormatAttributes {
  readonly autoPad: string;
  readonly ceilMode: number;
  readonly kernelShape: readonly number[];
  readonly strides: readonly number[];
  readonly pads: readonly number[];
}

const createShaderKeyFromAttributes = (attributes: PoolCommonAttributes): string =>
  `${attributes.format};${attributes.ceilMode};${attributes.autoPad};${attributes.kernelShape.length}`;

const createAveragePoolShaderKeyFromAttributes = (attributes: AveragePoolAttributes): string =>
  `${createShaderKeyFromAttributes(attributes)};${attributes.countIncludePad}`;

const createMaxPoolShaderKeyFromAttributes = (attributes: MaxPoolAttributes): string =>
  `${createShaderKeyFromAttributes(attributes)};${attributes.storageOrder};${attributes.dilations}`;

const parsePoolCommonAttributes = (attributes: Record<string, unknown>): PoolCommonAttributes => ({
  format: attributes.format as FormatAttributes['format'],
  autoPad: ['NOTSET', 'VALID', 'SAME_UPPER', 'SAME_LOWER'][attributes.auto_pad as number],
  ceilMode: attributes.ceil_mode as number,
  kernelShape: attributes.kernel_shape as [number, number],
  strides: attributes.strides as [number, number],
  pads: attributes.pads as [number, number, number, number],
});

export interface AveragePoolAttributes extends PoolCommonAttributes, AttributeWithCacheKey {
  readonly countIncludePad: boolean;
}

const createAveragePoolProgramInfo = (
  name: string,
  input: TensorView,
  isGlobalOperator: boolean,
  attributes: AveragePoolAttributes,
): ProgramInfo => {
  const [adjustedAttributes, outputShape] = getAdjustedPoolAttributesAndOutputShape(
    input,
    attributes,
    isGlobalOperator,
  );
  const x = inputVariable('x', input.dataType, input.dims.length);
  const dataType = x.type.value;

  const op1 = 'value += x_val;';
  let op2 = '';
  if (adjustedAttributes.countIncludePad) {
    op2 += `value /= ${dataType}(uniforms.kernelSize);`;
  } else {
    op2 += `value /= ${dataType}(i32(uniforms.kernelSize) - pad);`;
  }
  const [programUniforms, uniforms, hasPads, pwStartEndNotZero, phStartEndNotZero] = getUniformAndPadInfo(
    outputShape,
    adjustedAttributes,
  );
  programUniforms.push(...createTensorShapeVariables(input.dims, outputShape));
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['rank'];
  return {
    name,
    shaderCache: {
      hint: `${attributes.cacheKey};${hasPads};${pwStartEndNotZero};${phStartEndNotZero}`,
      inputDependencies,
    },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: input.dataType }],
      dispatchGroup: { x: Math.ceil(ShapeUtil.size(outputShape) / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource: (shaderHelper) =>
      generatePoolingCode(
        shaderHelper,
        x,
        input.dims.length,
        outputShape.length,
        adjustedAttributes,
        op1,
        op2,
        0.0,
        uniforms,
        hasPads,
        pwStartEndNotZero,
        phStartEndNotZero,
      ),
  };
};

export const parseAveragePoolAttributes = (attributes: Record<string, unknown>): AveragePoolAttributes => {
  const countIncludePad = (attributes.count_include_pad as number) === 0 ? false : true;

  const attr = parsePoolCommonAttributes(attributes);
  // TODO: support attribute 'ceil_mode'
  if (attr.ceilMode !== 0) {
    throw new Error('using ceil() in shape computation is not yet supported for AveragePool');
  }
  const averagePoolAttributes = { countIncludePad, ...attr, cacheKey: '' };
  return { ...averagePoolAttributes, cacheKey: createAveragePoolShaderKeyFromAttributes(averagePoolAttributes) };
};

export const averagePool = (context: ComputeContext, attributes: AveragePoolAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createAveragePoolProgramInfo('AveragePool', context.inputs[0], false, attributes));
};

const globalPoolAttributes = {
  autoPad: '',
  ceilMode: 0,
  countIncludePad: false,
  kernelShape: [],
  strides: [],
  pads: [],
  storageOrder: 0,
  dilations: [],
};

export const parseGlobalAveragePoolAttributes = (attributes: Record<string, unknown>): AveragePoolAttributes => {
  const format = attributes.format as FormatAttributes['format'];
  return { format, ...globalPoolAttributes, cacheKey: format };
};

export const globalAveragePool = (context: ComputeContext, attributes: AveragePoolAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createAveragePoolProgramInfo('GlobalAveragePool', context.inputs[0], true, attributes));
};

export interface MaxPoolAttributes extends PoolCommonAttributes, AttributeWithCacheKey {
  readonly storageOrder: number;
  readonly dilations: number[];
}

const createMaxPoolProgramInfo = (
  name: string,
  input: TensorView,
  isGlobalOperator: boolean,
  attributes: MaxPoolAttributes,
): ProgramInfo => {
  const [adjustedAttributes, outputShape] = getAdjustedPoolAttributesAndOutputShape(
    input,
    attributes,
    isGlobalOperator,
  );
  const op1 = `
      value = max(x_val, value);
    `;
  const op2 = '';
  const x = inputVariable('x', input.dataType, input.dims.length);
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['rank'];
  const [programUniforms, uniforms, hasPads, pwStartEndNotZero, phStartEndNotZero] = getUniformAndPadInfo(
    outputShape,
    adjustedAttributes,
  );
  programUniforms.push(...createTensorShapeVariables(input.dims, outputShape));
  return {
    name,
    shaderCache: {
      hint: `${attributes.cacheKey};${hasPads};${pwStartEndNotZero};${phStartEndNotZero}`,
      inputDependencies,
    },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: input.dataType }],
      dispatchGroup: { x: Math.ceil(ShapeUtil.size(outputShape) / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource: (shaderHelper) =>
      generatePoolingCode(
        shaderHelper,
        x,
        input.dims.length,
        outputShape.length,
        adjustedAttributes,
        op1,
        op2,
        input.dataType === DataType.float16 ? -65504 : -1e5,
        uniforms,
        hasPads,
        pwStartEndNotZero,
        phStartEndNotZero,
      ),
  };
};

export const maxPool = (context: ComputeContext, attributes: MaxPoolAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createMaxPoolProgramInfo('MaxPool', context.inputs[0], false, attributes));
};

export const parseMaxPoolAttributes = (attributes: Record<string, unknown>): MaxPoolAttributes => {
  const storageOrder = attributes.storage_order as number;
  const dilations = attributes.dilations as [number, number];

  const attr = parsePoolCommonAttributes(attributes);
  // TODO: support attribute 'ceil_mode' and 'storage_order'
  if (storageOrder !== 0) {
    throw new Error('column major storage order is not yet supported for MaxPool');
  }
  if (attr.ceilMode !== 0) {
    throw new Error('using ceil() in shape computation is not yet supported for MaxPool');
  }
  const maxPoolAttributes = { storageOrder, dilations, ...attr, cacheKey: '' };
  return { ...maxPoolAttributes, cacheKey: createMaxPoolShaderKeyFromAttributes(maxPoolAttributes) };
};

export const parseGlobalMaxPoolAttributes = (attributes: Record<string, unknown>): MaxPoolAttributes => {
  const format = attributes.format as FormatAttributes['format'];
  return { format, ...globalPoolAttributes, cacheKey: format };
};

export const globalMaxPool = (context: ComputeContext, attributes: MaxPoolAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createMaxPoolProgramInfo('GlobalMaxPool', context.inputs[0], true, attributes));
};
