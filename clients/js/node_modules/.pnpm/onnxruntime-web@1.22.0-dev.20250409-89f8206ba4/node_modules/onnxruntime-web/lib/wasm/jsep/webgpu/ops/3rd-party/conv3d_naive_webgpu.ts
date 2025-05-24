/**
 * @license
 * Copyright 2019 Google LLC. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =============================================================================
 */

// sampled from [@tensorflow/tfjs] tfjs-backend-webgpu/src/conv3d_naive_webgpu.ts
//
// modified to fit the needs of the project

import { DataType } from '../../../../wasm-common';
import { LOG_DEBUG } from '../../../log';
import { TensorView } from '../../../tensor-view';
import { ShapeUtil } from '../../../util';
import { ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../../types';
import {
  createTensorShapeVariables,
  getElementAt,
  inputVariable,
  outputVariable,
  ShaderHelper,
  tensorTypeToWsglStorageType,
  UniformsArrayType,
} from '../common';
import { ConvAttributes } from '../conv';
import { appendActivationUniforms, appendActivationUniformsData, getActivationSnippet } from '../fuse-utils';

import { typeSnippet } from './activation_util';

const arrayProduct = (arr: number[]) => {
  let product = 1;
  for (let i = 0; i < arr.length; i++) {
    product *= arr[i];
  }
  return product;
};

const parse3TupleParam = (param: number | [number, number, number]): [number, number, number] =>
  typeof param === 'number' ? [param, param, param] : param;

const getEffectiveFilterSize = (filterSize: number, dilation: number): number => {
  if (dilation <= 1) {
    return filterSize;
  }

  return filterSize + (filterSize - 1) * (dilation - 1);
};

const computeDefaultPad = (
  inputShape: [number, number] | [number, number, number, number],
  fieldSize: number,
  stride: number,
  dilation = 1,
): number => {
  const effectiveFieldSize = getEffectiveFilterSize(fieldSize, dilation);
  return Math.floor((inputShape[0] * (stride - 1) - stride + effectiveFieldSize) / 2);
};

const computeOutputShape4D = (
  inShape: [number, number, number, number],
  filterShape: [number, number, number],
  outChannels: number,
  strides: [number, number, number],
  zeroPad?: number,
): [number, number, number, number] => {
  if (zeroPad == null) {
    // eslint-disable-next-line no-param-reassign
    zeroPad = computeDefaultPad(inShape, filterShape[0], strides[0]);
  }
  const outShape: [number, number, number, number] = [0, 0, 0, outChannels];
  for (let index = 0; index < 3; index++) {
    if (inShape[index] + 2 * zeroPad >= filterShape[index]) {
      outShape[index] = Math.trunc((inShape[index] - filterShape[index] + 2 * zeroPad) / strides[index] + 1);
    }
  }
  return outShape;
};

const get3DPadAndOutInfo = (
  pad: number | string | number[],
  inDepth: number,
  inHeight: number,
  inWidth: number,
  strideDepth: number,
  strideHeight: number,
  strideWidth: number,
  filterDepth: number,
  filterHeight: number,
  filterWidth: number,
): { padInfo: PadInfo3D; outDepth: number; outHeight: number; outWidth: number } => {
  let padInfo: PadInfo3D;
  let outDepth: number;
  let outHeight: number;
  let outWidth: number;

  if (pad === 'VALID') {
    // eslint-disable-next-line no-param-reassign
    pad = 0;
  }

  if (typeof pad === 'number') {
    padInfo = { top: pad, bottom: pad, left: pad, right: pad, front: pad, back: pad };
    const outShape = computeOutputShape4D(
      [inDepth, inHeight, inWidth, 1],
      [filterDepth, filterHeight, filterWidth],
      1,
      [strideDepth, strideHeight, strideWidth],
      pad,
    );
    outDepth = outShape[0];
    outHeight = outShape[1];
    outWidth = outShape[2];
  } else if (Array.isArray(pad)) {
    if (!pad.every((val, _, arr) => val === arr[0])) {
      throw Error(`Unsupported padding parameter: ${pad}`);
    }
    padInfo = { top: pad[0], bottom: pad[1], left: pad[2], right: pad[3], front: pad[4], back: pad[5] };
    const outShape = computeOutputShape4D(
      [inDepth, inHeight, inWidth, 1],
      [filterDepth, filterHeight, filterWidth],
      1,
      [strideDepth, strideHeight, strideWidth],
      pad[0],
    );
    outDepth = outShape[0];
    outHeight = outShape[1];
    outWidth = outShape[2];
  } else if (pad === 'SAME_UPPER') {
    // TODO: support 'SAME_LOWER'.
    outDepth = Math.ceil(inDepth / strideDepth);
    outHeight = Math.ceil(inHeight / strideHeight);
    outWidth = Math.ceil(inWidth / strideWidth);
    const padAlongDepth = (outDepth - 1) * strideDepth + filterDepth - inDepth;
    const padAlongHeight = (outHeight - 1) * strideHeight + filterHeight - inHeight;
    const padAlongWidth = (outWidth - 1) * strideWidth + filterWidth - inWidth;
    const front = Math.floor(padAlongDepth / 2);
    const back = padAlongDepth - front;
    const top = Math.floor(padAlongHeight / 2);
    const bottom = padAlongHeight - top;
    const left = Math.floor(padAlongWidth / 2);
    const right = padAlongWidth - left;

    padInfo = { top, bottom, left, right, front, back };
  } else {
    throw Error(`Unknown padding parameter: ${pad}`);
  }
  return { padInfo, outDepth, outHeight, outWidth };
};

type PadInfo3D = {
  top: number;
  left: number;
  right: number;
  bottom: number;
  front: number;
  back: number;
};

export type Conv3DInfo = {
  batchSize: number;
  inDepth: number;
  inHeight: number;
  inWidth: number;
  inChannels: number;
  outDepth: number;
  outHeight: number;
  outWidth: number;
  outChannels: number;
  dataFormat: 'channelsFirst' | 'channelsLast';
  strideDepth: number;
  strideHeight: number;
  strideWidth: number;
  dilationDepth: number;
  dilationHeight: number;
  dilationWidth: number;
  filterDepth: number;
  filterHeight: number;
  filterWidth: number;
  effectiveFilterDepth: number;
  effectiveFilterHeight: number;
  effectiveFilterWidth: number;
  padInfo: PadInfo3D;
  inShape: [number, number, number, number, number];
  outShape: [number, number, number, number, number];
  filterShape: [number, number, number, number, number];
};

export const computeConv3DInfo = (
  inShape: [number, number, number, number, number],
  filterShape: [number, number, number, number, number],
  strides: number | [number, number, number],
  dilations: number | [number, number, number],
  pad: number | string | number[],
  depthwise = false,
  dataFormat: 'channelsFirst' | 'channelsLast' = 'channelsLast',
): Conv3DInfo => {
  let batchSize, inDepth, inHeight, inWidth, inChannels;
  if (dataFormat === 'channelsLast') {
    [batchSize, inDepth, inHeight, inWidth, inChannels] = inShape;
  } else if (dataFormat === 'channelsFirst') {
    [batchSize, inChannels, inDepth, inHeight, inWidth] = inShape;
  } else {
    throw new Error(`Unknown dataFormat ${dataFormat}`);
  }
  const [filterChannels, , filterDepth, filterHeight, filterWidth] = filterShape;

  const [strideDepth, strideHeight, strideWidth] = parse3TupleParam(strides);
  const [dilationDepth, dilationHeight, dilationWidth] = parse3TupleParam(dilations);

  const effectiveFilterDepth = getEffectiveFilterSize(filterDepth, dilationDepth);
  const effectiveFilterHeight = getEffectiveFilterSize(filterHeight, dilationHeight);
  const effectiveFilterWidth = getEffectiveFilterSize(filterWidth, dilationWidth);
  const { padInfo, outDepth, outHeight, outWidth } = get3DPadAndOutInfo(
    pad,
    inDepth,
    inHeight,
    inWidth,
    strideDepth,
    strideHeight,
    strideWidth,
    effectiveFilterDepth,
    effectiveFilterHeight,
    effectiveFilterWidth,
  );

  const outChannels = depthwise ? filterChannels * inChannels : filterChannels;

  let outShape: [number, number, number, number, number] = [0, 0, 0, 0, 0];
  if (dataFormat === 'channelsFirst') {
    outShape = [batchSize, outChannels, outDepth, outHeight, outWidth];
  } else if (dataFormat === 'channelsLast') {
    outShape = [batchSize, outDepth, outHeight, outWidth, outChannels];
  }

  return {
    batchSize,
    dataFormat,
    inDepth,
    inHeight,
    inWidth,
    inChannels,
    outDepth,
    outHeight,
    outWidth,
    outChannels,
    padInfo,
    strideDepth,
    strideHeight,
    strideWidth,
    filterDepth,
    filterHeight,
    filterWidth,
    effectiveFilterDepth,
    effectiveFilterHeight,
    effectiveFilterWidth,
    dilationDepth,
    dilationHeight,
    dilationWidth,
    inShape,
    outShape,
    filterShape,
  };
};

export const createConv3DNaiveProgramInfo = (
  inputs: readonly TensorView[],
  attributes: ConvAttributes,
  outputShape: readonly number[],
  filterDims: readonly number[],
  pads: readonly number[],
  dataFormat: string,
): ProgramInfo => {
  const isChannelLast = dataFormat === 'channelsLast';
  const inChannels = isChannelLast ? inputs[0].dims[3] : inputs[0].dims[1];
  // TODO: enable vec4.
  const isVec4 = false;
  const workGroupSize: [number, number, number] = [64, 1, 1];
  const dispatchLayout = { x: outputShape.map((_, i) => i) };
  const dispatch = [Math.ceil(arrayProduct(dispatchLayout.x.map((d) => outputShape[d])) / workGroupSize[0]), 1, 1];

  LOG_DEBUG('verbose', () => `[conv3d_naive_webgpu] dispatch = ${dispatch}`);

  const innerElementSize = isVec4 ? (isChannelLast && inChannels % 4 !== 0 ? 3 : 4) : 1;
  const outputSize = ShapeUtil.size(outputShape);
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: filterDims },
    { type: DataType.uint32, data: pads },
    { type: DataType.uint32, data: attributes.strides },
    { type: DataType.uint32, data: attributes.dilations },
  ];
  appendActivationUniformsData(attributes, programUniforms);
  programUniforms.push(...createTensorShapeVariables(inputs[0].dims, inputs[1].dims));
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['rank', 'rank'];
  const hasBias = inputs.length === 3;
  if (hasBias) {
    programUniforms.push(...createTensorShapeVariables(inputs[2].dims));
    inputDependencies.push('rank');
  }
  programUniforms.push(...createTensorShapeVariables(outputShape));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'filter_dims', type: 'u32', length: filterDims.length },
      { name: 'pads', type: 'u32', length: pads.length },
      { name: 'strides', type: 'u32', length: attributes.strides.length },
      { name: 'dilations', type: 'u32', length: attributes.dilations.length },
    ];
    appendActivationUniforms(attributes, uniforms);
    // TODO: support component 2, 3.
    const components = isVec4 ? 4 : 1;
    const t = tensorTypeToWsglStorageType(inputs[0].dataType);

    const x = inputVariable(
      'x',
      inputs[0].dataType,
      inputs[0].dims.length,
      innerElementSize === 3 ? 1 : innerElementSize,
    );
    const w = inputVariable('W', inputs[1].dataType, inputs[1].dims.length, components);
    const inputVariables = [x, w];
    const output = outputVariable('result', inputs[0].dataType, outputShape.length, components);
    let declareFunctions = '';
    if (hasBias) {
      const bias = inputVariable('bias', inputs[2].dataType, inputs[2].dims.length, components);
      inputVariables.push(bias);
      declareFunctions += `
        fn getBiasByOutputCoords(coords : array<u32, 5>) -> ${isVec4 ? `vec4<${t}>` : t} {
          return bias[${isChannelLast ? getElementAt('coords', 4, 5) : getElementAt('coords', 1, 5)}${
            isVec4 ? '/ 4' : ''
          }];
        }`;
    }
    const resType = typeSnippet(innerElementSize, t);
    const applyActivation = getActivationSnippet(attributes, resType, t);

    return `
            ${declareFunctions}
            fn getX(d0 : u32, d1 : u32, d2 : u32, d3 : u32, d4 : u32) -> f32 {
              let aIndices = array<u32, 5>(d0, d1, d2, d3, d4);
              return ${x.getByIndices('aIndices')};
            }
            fn getW(d0 : u32, d1 : u32, d2 : u32, d3 : u32, d4 : u32) -> f32 {
              let aIndices = array<u32, 5>(d0, d1, d2, d3, d4);
              return ${w.getByIndices('aIndices')};
            }
          ${shaderHelper.registerUniforms(uniforms).declareVariables(...inputVariables, output)}
          ${shaderHelper.mainStart()}
          ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
              let coords = ${output.offsetToIndices('global_idx')};
              let batch = ${getElementAt('coords', 0, x.rank)};
              let d2 = ${
                isChannelLast ? getElementAt('coords', x.rank - 1, x.rank) : getElementAt('coords', 1, x.rank)
              };
              let xFRCCorner = vec3<u32>(${
                isChannelLast ? getElementAt('coords', 1, x.rank) : getElementAt('coords', 2, x.rank)
              },
              ${isChannelLast ? getElementAt('coords', 2, x.rank) : getElementAt('coords', 3, x.rank)},
              ${
                isChannelLast ? getElementAt('coords', 3, x.rank) : getElementAt('coords', 4, x.rank)
              }) * uniforms.strides - uniforms.pads;
              let xFCorner = xFRCCorner.x;
              let xRCorner = xFRCCorner.y;
              let xCCorner = xFRCCorner.z;
              let xShapeY = ${
                isChannelLast
                  ? getElementAt('uniforms.x_shape', 1, x.rank)
                  : getElementAt('uniforms.x_shape', 2, x.rank)
              };
              let xShapeZ = ${
                isChannelLast
                  ? getElementAt('uniforms.x_shape', 2, x.rank)
                  : getElementAt('uniforms.x_shape', 3, x.rank)
              };
              let xShapeW = ${
                isChannelLast
                  ? getElementAt('uniforms.x_shape', 3, x.rank)
                  : getElementAt('uniforms.x_shape', 4, x.rank)
              };
              let xShapeU = ${
                isChannelLast
                  ? getElementAt('uniforms.x_shape', 4, x.rank)
                  : getElementAt('uniforms.x_shape', 1, x.rank)
              };
              let inputDepthNearestVec4 = (xShapeU / 4) * 4;
              let inputDepthVec4Remainder = xShapeU % 4;

              var value = 0.0;
              for (var wF = 0u; wF < uniforms.filter_dims[0]; wF++) {
                let xF = xFCorner + wF * uniforms.dilations[0];
                if (xF < 0 || xF >= xShapeY) {
                  continue;
                }

                for (var wR = 0u; wR < uniforms.filter_dims[1]; wR++) {
                  let xR = xRCorner + wR * uniforms.dilations[1];
                  if (xR < 0 || xR >= xShapeZ) {
                    continue;
                  }

                  for (var wC = 0u; wC < uniforms.filter_dims[2]; wC++) {
                    let xC = xCCorner + wC * uniforms.dilations[2];
                    if (xC < 0 || xC >= xShapeW) {
                      continue;
                    }

                    for (var d1 = 0u; d1 < inputDepthNearestVec4; d1 += 4) {
                      ${
                        isChannelLast
                          ? `let xValues = vec4<f32>(
                               getX(batch, xF, xR, xC, d1),
                               getX(batch, xF, xR, xC, d1 + 1),
                               getX(batch, xF, xR, xC, d1 + 2),
                               getX(batch, xF, xR, xC, d1 + 3));
                            `
                          : `let xValues = vec4<f32>(
                               getX(batch, d1, xF, xR, xC),
                               getX(batch, d1 + 1, xF, xR, xC),
                               getX(batch, d1 + 2, xF, xR, xC),
                               getX(batch, d1 + 3, xF, xR, xC));
                            `
                      }
                            let wValues = vec4<f32>(
                              getW(d2, d1, wF, wR, wC),
                              getW(d2, d1 + 1, wF, wR, wC),
                              getW(d2, d1 + 2, wF, wR, wC),
                              getW(d2, d1 + 3, wF, wR, wC));
                      value += dot(xValues, wValues);
                    }
                    if (inputDepthVec4Remainder == 1) {
                        ${
                          isChannelLast
                            ? `value += getX(batch, xF, xR, xC, inputDepthNearestVec4)
                          * getW(d2, inputDepthNearestVec4, wF, wR, wC);`
                            : `value += getX(batch, inputDepthNearestVec4, xF, xR, xC)
                          * getW(d2, inputDepthNearestVec4, wF, wR, wC);`
                        }
                    } else if (inputDepthVec4Remainder == 2) {
                      ${
                        isChannelLast
                          ? `let xValues = vec2<f32>(
                        getX(batch, xF, xR, xC, inputDepthNearestVec4),
                        getX(batch, xF, xR, xC, inputDepthNearestVec4 + 1));
                      `
                          : `let xValues = vec2<f32>(
                        getX(batch, inputDepthNearestVec4, xF, xR, xC),
                        getX(batch, inputDepthNearestVec4 + 1, xF, xR, xC));
                    `
                      }
                    let wValues = vec2<f32>(
                      getW(d2, inputDepthNearestVec4, wF, wR, wC),
                      getW(d2, inputDepthNearestVec4 + 1, wF, wR, wC));
                      value += dot(xValues, wValues);
                    } else if (inputDepthVec4Remainder == 3) {
                      ${
                        isChannelLast
                          ? `let xValues = vec3<f32>(
                        getX(batch, xF, xR, xC, inputDepthNearestVec4),
                        getX(batch, xF, xR, xC, inputDepthNearestVec4 + 1),
                        getX(batch, xF, xR, xC, inputDepthNearestVec4 + 2));
                      `
                          : `let xValues = vec3<f32>(
                        getX(batch, inputDepthNearestVec4, xF, xR, xC),
                        getX(batch, inputDepthNearestVec4 + 1, xF, xR, xC),
                        getX(batch, inputDepthNearestVec4 + 2, xF, xR, xC));
                    `
                      }
                    let wValues = vec3<f32>(
                      getW(d2, inputDepthNearestVec4, wF, wR, wC),
                      getW(d2, inputDepthNearestVec4 + 1, wF, wR, wC),
                      getW(d2, inputDepthNearestVec4 + 2, wF, wR, wC));
                      value += dot(xValues, wValues);
                    }
                  }
                }
              }
              ${hasBias ? 'value = value + getBiasByOutputCoords(coords)' : ''};
              ${applyActivation}
              result[global_idx] = f32(value);
          }`;
  };
  return {
    name: 'Conv3DNaive',
    shaderCache: { hint: `${attributes.cacheKey};${isChannelLast};${innerElementSize};${hasBias}`, inputDependencies },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: dispatch[0], y: dispatch[1], z: dispatch[2] },
      programUniforms,
    }),
    getShaderSource,
  };
};
