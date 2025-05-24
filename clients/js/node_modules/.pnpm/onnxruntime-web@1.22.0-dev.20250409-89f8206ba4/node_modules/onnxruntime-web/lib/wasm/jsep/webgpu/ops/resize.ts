// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo } from '../types';

import {
  createTensorShapeVariables,
  getElementAt,
  IndicesHelper,
  inputVariable,
  outputVariable,
  ShaderHelper,
} from './common';

type CoordinateTransformMode =
  | 'half_pixel'
  | 'asymmetric'
  | 'pytorch_half_pixel'
  | 'tf_half_pixel_for_nn'
  | 'align_corners'
  | 'tf_crop_and_resize'
  | 'half_pixel_symmetric';

type KeepAspectRatioPolicy = 'stretch' | 'not_smaller' | 'not_larger';

type Mode = 'nearest' | 'linear' | 'cubic';

type NearestMode = 'round_prefer_floor' | 'round_prefer_ceil' | 'floor' | 'ceil' | 'simple';

export interface ResizeAttributes extends AttributeWithCacheKey {
  antialias: number;
  axes: number[];
  coordinateTransformMode: CoordinateTransformMode;
  cubicCoeffA: number;
  excludeOutside: boolean;
  extrapolationValue: number;
  keepAspectRatioPolicy: KeepAspectRatioPolicy;
  mode: Mode;
  nearestMode: NearestMode;
}

const validateScales = (scales: number[], attributes: ResizeAttributes): void => {
  scales.every(
    (value) =>
      value > 0 ||
      (() => {
        throw new Error('Resize requires scales input values to be positive');
      }),
  );
  // Check scales dims based on mode: LINEAR, CUBIC
  if (scales.length > 0) {
    if (attributes.mode === 'linear') {
      if (
        !(
          scales.length === 2 ||
          scales.length === 3 ||
          (scales.length === 4 && scales[0] === 1 && scales[1] === 1) ||
          (scales.length === 4 && scales[0] === 1 && scales[3] === 1) ||
          (scales.length === 5 && scales[0] === 1 && scales[1] === 1)
        )
      ) {
        throw new Error(
          `For linear mode, Resize requires scales to be 2D, 3D, 4D with either two outermost or one innermost and
            one outermost scale values equal to 1, or 5D with two outermost scale values equal to 1`,
        );
      }
    } else if (attributes.mode === 'cubic') {
      if (
        !(
          scales.length === 2 ||
          (scales.length === 4 && scales[0] === 1 && scales[1] === 1) ||
          (scales.length === 4 && scales[0] === 1 && scales[3] === 1)
        )
      ) {
        throw new Error('Resize requires scales input size to be 2 or 4 for cubic mode');
      }
    }
  }
};

const updateScales = (scales: readonly number[], axes: readonly number[], rank: number): number[] => {
  axes.every(
    (value) =>
      (value >= 0 && value < rank) ||
      (() => {
        throw new Error('Resize requires axes input values to be positive and less than rank');
      }),
  );
  const newScales = new Array(rank).fill(1.0);
  axes.forEach((value, index) => (newScales[value] = scales[index]));
  return newScales;
};

const validateInputs = (
  inputs: readonly TensorView[],
  attributes: ResizeAttributes,
  opsetVersion: number,
  scales: number[],
  sizes: number[],
  roi: number[],
): void => {
  const [roiInputIndex, scalesInputIndex, sizesInputIndex] =
    opsetVersion > 10 ? [1, 2, 3] : [-1, inputs.length > 1 ? 1 : -1, -1];
  const rank = inputs[0].dims.length;
  if (roiInputIndex > 0 && inputs.length > roiInputIndex && inputs[roiInputIndex].dims.length > 0) {
    inputs[roiInputIndex].getFloat32Array().forEach((value) => roi.push(value));
  } else if (attributes.coordinateTransformMode === 'tf_crop_and_resize') {
    throw new Error('Resize requires RoI input to be specified when coordinateTransformMode is tfCropAndResize');
  }

  if (
    scalesInputIndex > 0 &&
    inputs.length > scalesInputIndex &&
    inputs[scalesInputIndex].dims.length === 1 &&
    inputs[scalesInputIndex].dims[0] > 0
  ) {
    inputs[scalesInputIndex].getFloat32Array().forEach((value) => scales.push(value));
    if (
      scales.length !== 0 &&
      scales.length !== rank &&
      opsetVersion >= 18 &&
      scales.length !== attributes.axes.length
    ) {
      throw new Error('Resize requires scales input size to be same as input rank or axes size for opset 18 and up');
    }
    validateScales(scales, attributes);
    if (attributes.axes.length > 0) {
      updateScales(scales, attributes.axes, rank).forEach((value, index) => (scales[index] = value));
    }
  }
  if (
    sizesInputIndex > 0 &&
    inputs.length > sizesInputIndex &&
    inputs[sizesInputIndex].dims.length === 1 &&
    inputs[sizesInputIndex].dims[0] > 0
  ) {
    inputs[sizesInputIndex].getBigInt64Array().forEach((value) => sizes.push(Number(value)));
    if (sizes.length !== 0 && sizes.length !== rank && opsetVersion >= 18 && sizes.length !== attributes.axes.length) {
      throw new Error('Resize requires sizes input size to be same as input rank or axes size for opset 18 and up');
    }
  }

  if (attributes.axes.length > 0) {
    if (scales.length !== 0 && scales.length !== attributes.axes.length) {
      throw new Error('Resize requires "scales" input size to be of axes rank when axes attributes is specified');
    }
    if (sizes.length !== 0 && sizes.length !== attributes.axes.length) {
      throw new Error('Resize requires "sizes" input size to be of rank axes rank when axes attributes is specified');
    }
  }
  if (typeof scales !== 'undefined' && typeof sizes !== 'undefined' && scales.length > 0 && sizes.length > rank) {
    throw new Error('Resize requires only of scales or sizes to be specified');
  }
};

const getSafeIntegerDivision = (a: string, b: string, c: string, dType: string): string => `
  // The whole part and the fractional part are calculated separately due to inaccuracy of floating
  // point division. As an example, f32(21) / f32(7) may evaluate to 2.99... instead of 3, causing an
  // offset-by-one error later in floor().
  let big = (${a}) * (${b});
  let whole = ${dType}(big / (${c}));
  let fract = ${dType}(big % (${c})) / ${dType}(${c});
  return whole + fract;
`;

const getOriginalCoordinateFromResizedCoordinate = (
  coordinateTransferMode: CoordinateTransformMode,
  dType: string,
): string =>
  `fn getOriginalCoordinateFromResizedCoordinate(xResized: u32, xScale: f32, lengthResized: u32,
     lengthOriginal: u32, roiStart: f32, roiEnd: f32) -> ${dType} { ` +
  (() => {
    switch (coordinateTransferMode) {
      case 'asymmetric':
        return `
          if (xScale < 1.0 || floor(xScale) != xScale) {
            return ${dType}(xResized) / ${dType}(xScale);
          } else {
            ${getSafeIntegerDivision('xResized', 'lengthOriginal', 'lengthResized', dType)}
          }
        `;
      case 'pytorch_half_pixel':
        return `if (lengthResized > 1) {
                    return (${dType}(xResized) + 0.5) / ${dType}(xScale) - 0.5;
                  } else {
                    return 0.0;
                  }`;
      case 'tf_half_pixel_for_nn':
        return `return (${dType}(xResized) + 0.5) / ${dType}(xScale);`;
      case 'align_corners':
        return `if (lengthResized == 1) {
                    return 0.0;
                  } else {
                    ${getSafeIntegerDivision('xResized', 'lengthOriginal - 1', 'lengthResized - 1', dType)}
                  }`;
      case 'tf_crop_and_resize':
        return `if (lengthResized > 1) {
                    return ${dType}(roiStart) * ${dType}(lengthOriginal - 1) +
                        (${dType}(xResized) * ${dType}(roiEnd - roiStart) * ${dType}(lengthOriginal - 1)) /
                        ${dType}(lengthResized - 1);
                  } else {
                    return 0.5 * ${dType}(roiStart + roiEnd) * ${dType}(lengthOriginal - 1);
                  }`;
      case 'half_pixel_symmetric':
        return `const outputWidth = ${dType}xScale * ${dType}(lengthResized);
                  const adjustment = ${dType}(lengthResized) / outputWidth;
                  const center = ${dType}(lengthOriginal) / 2;
                  const offset = center * (1 - adjustment);
                  return offset + ((${dType}(xResized) + 0.5) / ${dType}(xScale)) - 0.5;`;
      case 'half_pixel':
        return `return ((${dType}(xResized) + 0.5) / ${dType}(xScale)) - 0.5;`;
      default:
        throw new Error(`Coordinate transform mode ${coordinateTransferMode} is not supported`);
    }
  })() +
  '}';

const getNearestPixelFromOriginal = (nearestMode: NearestMode, opsetVersion: number, dType: string): string =>
  `fn getNearestPixelFromOriginal(xOriginal: ${dType}, isDownSample: bool) -> ${dType} {` +
  (() => {
    switch (nearestMode) {
      case 'round_prefer_ceil':
        return 'if (fract(xOriginal) == 0.5) { \
            return ceil(xOriginal); \
          } else { \
            return round(xOriginal); \
          }';
      case 'floor':
        return 'return floor(xOriginal);';
      case 'ceil':
        return 'return ceil(xOriginal);';
      case 'round_prefer_floor':
        return 'if (fract(xOriginal) == 0.5) { \
                    return floor(xOriginal); \
                  } else { \
                    return round(xOriginal); \
                  }';
      case 'simple':
      default:
        if (opsetVersion < 11) {
          return 'if (isDownSample) \
                    { \
                      return ceil(xOriginal); \
                    } else { \
                      return xOriginal; \
                    }';
        }
        throw new Error(`Nearest mode ${nearestMode} is not supported`);
    }
  })() +
  '}';

const updateRoI = (roi: readonly number[], axes: readonly number[], rank: number): number[] => {
  const roiTmp = new Array(rank).fill(0).concat(new Array(rank).fill(1));
  const roiLocal = roi.length === 0 ? roiTmp : roi.slice();
  if (axes.length > 0) {
    axes.forEach((v, i) => {
      roiTmp[v] = roiLocal[i];
      roiTmp[i + rank] = roiLocal[axes.length + i];
    });
    return roiTmp;
  }
  return roiLocal;
};

const initOutputShape = (
  inputShape: readonly number[],
  scales: readonly number[],
  sizes: readonly number[],
  axes: readonly number[],
): number[] => {
  let outputShape: number[] = [];
  if (sizes.length > 0) {
    if (axes.length > 0) {
      inputShape.forEach((v) => outputShape.push(v));
      if (Math.max(...axes) > inputShape.length) {
        throw new Error('axes is out of bound');
      }
      axes.forEach((v, i) => (outputShape[v] = sizes[i]));
    } else {
      sizes.forEach((v) => outputShape.push(v));
    }
  } else {
    if (scales.length === 0) {
      throw new Error('Resize requires either scales or sizes.');
    } else {
      outputShape = inputShape.map((value, index) => Math.round(value * scales[index]));
    }
  }
  return outputShape;
};

const adjustOutputShape = (inputShape: readonly number[], scales: number[], attributes: ResizeAttributes) => {
  const scaleInPolicy = (() => {
    switch (attributes.keepAspectRatioPolicy) {
      case 'not_larger':
        return attributes.axes.length > 0
          ? Math.min(...attributes.axes.map((i) => scales[i]), Number.MAX_VALUE)
          : Math.min(...scales, Number.MAX_VALUE);
      case 'not_smaller':
        return attributes.axes.length > 0
          ? Math.max(...attributes.axes.map((i) => scales[i]), Number.MIN_VALUE)
          : Math.max(...scales, Number.MIN_VALUE);
      default:
        throw new Error(`Keep aspect ratio policy ${attributes.keepAspectRatioPolicy} is not supported`);
    }
  })();
  scales.fill(1.0, 0, scales.length);
  const adjustedOutputShape = inputShape.slice();
  if (attributes.axes.length > 0) {
    attributes.axes.forEach((v) => (scales[v] = scaleInPolicy));
    attributes.axes.forEach((v) => (adjustedOutputShape[v] = Math.round(inputShape[v] * scales[v])));
  } else {
    scales.fill(scaleInPolicy, 0, scales.length);
    adjustedOutputShape.forEach((v, i) => (adjustedOutputShape[i] = Math.round(v * scales[i])));
  }
  return adjustedOutputShape;
};

const calculateOriginalIndicesFromOutputIndices = (
  output: IndicesHelper,
  inputShape: readonly number[],
  outputShape: readonly number[],
  scalesLength: number,
  roiLength: number,
): string => `
    fn calculateOriginalIndicesFromOutputIndices(output_indices: ${output.type.indices}) -> array<${
      output.type.value
    }, ${outputShape.length}> {
      var original_indices: array<${output.type.value}, ${outputShape.length}>;
      for (var i:u32 = 0; i < ${outputShape.length}; i++) {
        var output_index = ${output.indicesGet('output_indices', 'i')};
        var scale = ${getElementAt('uniforms.scales', 'i', scalesLength)};
        var roi_low = ${getElementAt('uniforms.roi', 'i', roiLength)};
        var roi_hi = ${getElementAt('uniforms.roi', `i + ${inputShape.length}`, roiLength)};
        if (scale == 1.0) {
          original_indices[i] = ${output.type.value}(output_index);
        } else {
          var input_shape_i = ${getElementAt('uniforms.input_shape', 'i', inputShape.length)};
          var output_shape_i = ${getElementAt('uniforms.output_shape', 'i', outputShape.length)};
          original_indices[i] = getOriginalCoordinateFromResizedCoordinate(output_index, scale, output_shape_i,
                                                                           input_shape_i, roi_low, roi_hi);
        }
      }
      return original_indices;
    }`;

const calculateInputIndicesFromOutputIndices = (
  input: IndicesHelper,
  output: IndicesHelper,
  inputShape: readonly number[],
  outputShape: readonly number[],
  scalesLength: number,
  roiLength: number,
  useExtrapolation: boolean,
): string => `
    fn calculateInputIndicesFromOutputIndices(output_indices: ${output.type.indices}) -> ${input.type.indices} {
      var input_indices: ${input.type.indices};
      for (var i:u32 = 0; i < ${outputShape.length}; i++) {
        var output_index = ${output.indicesGet('output_indices', 'i')};
        var input_index: u32;
        var scale = ${getElementAt('uniforms.scales', 'i', scalesLength)};
        if (scale == 1.0) {
          input_index = output_index;
        } else {
          var roi_low = ${getElementAt('uniforms.roi', 'i', roiLength)};
          var roi_hi = ${getElementAt('uniforms.roi', `i + ${inputShape.length}`, roiLength)};
          var input_shape_i = ${getElementAt('uniforms.input_shape', 'i', inputShape.length)};
          var output_shape_i = ${getElementAt('uniforms.output_shape', 'i', outputShape.length)};
          var original_idx = getOriginalCoordinateFromResizedCoordinate(output_index, scale, output_shape_i,
                                                                        input_shape_i, roi_low, roi_hi);
          if (!${useExtrapolation} || (original_idx >= 0 && original_idx < ${output.type.value}(input_shape_i))) {
            if (original_idx < 0) {
              input_index = 0;
            } else if (original_idx > ${output.type.value}(input_shape_i - 1)) {
              input_index = input_shape_i - 1;
            } else {
              input_index = u32(getNearestPixelFromOriginal(original_idx, scale < 1));
            }
          } else {
            input_index = u32(original_idx);
          }
        }
        ${input.indicesSet('input_indices', 'i', 'input_index')}
      }
      return input_indices;
    }`;
const checkInputIndices = (input: IndicesHelper, inputShape: readonly number[]): string => `
    fn checkInputIndices(input_indices: ${input.type.indices}) -> bool {
      for (var i:u32 = 0; i < ${inputShape.length}; i++) {
        var input_index = ${input.indicesGet('input_indices', 'i')};
        if (input_index < 0 || input_index >= ${getElementAt('uniforms.input_shape', 'i', inputShape.length)}) {
          return false;
        }
      }
      return true;
    }`;

const setChannelAndBatchIndices = (
  input: IndicesHelper,
  channelIdx: number,
  batchIdx: number,
  spacialDims: number,
): string =>
  input.rank > spacialDims
    ? `
    ${input.indicesSet('input_indices', channelIdx, 'channel')};
    ${input.indicesSet('input_indices', batchIdx, 'batch')};
`
    : '';

const bilinearInterpolation = (
  input: IndicesHelper,
  output: IndicesHelper,
  inputShape: readonly number[],
  useExtrapolation: boolean,
  extrapolationValue: number,
): string => {
  const isNchw = true;
  const [batchIdx, heightIdx, widthIdx, channelIdx] =
    inputShape.length === 2 ? [-1, 0, 1, -1] : isNchw ? [0, 2, 3, 1] : [0, 1, 2, 3];
  const dType = input.type.value;
  return `
    fn getInputValue(batch: u32, channel: u32, row: u32, col: u32) -> ${dType} {
      var input_indices: ${input.type.indices};
      ${input.indicesSet('input_indices', heightIdx, `max(0, min(row, ${inputShape[heightIdx]} - 1))`)};
      ${input.indicesSet('input_indices', widthIdx, `max(0, min(col, ${inputShape[widthIdx]} - 1))`)};
      ${setChannelAndBatchIndices(input, channelIdx, batchIdx, 2)}
      return ${input.getByIndices('input_indices')};
    }

    fn bilinearInterpolation(output_indices: ${output.type.indices}) -> ${dType} {
      var originalIndices = calculateOriginalIndicesFromOutputIndices(output_indices);
      var row:${dType} = originalIndices[${heightIdx}];
      var col:${dType} = originalIndices[${widthIdx}];
      ${
        useExtrapolation
          ? `if (row < 0 || row > (${inputShape[heightIdx]} - 1) || col < 0 || col > (${inputShape[widthIdx]} - 1)) {
        return ${extrapolationValue};
      }`
          : ''
      };
      row = max(0, min(row, ${inputShape[heightIdx]} - 1));
      col = max(0, min(col, ${inputShape[widthIdx]} - 1));
      var row1: u32 = u32(row);
      var col1: u32 = u32(col);
      var row2: u32 = u32(row + 1);
      var col2: u32 = u32(col + 1);
      var channel: u32 = ${inputShape.length > 2 ? `u32(originalIndices[${channelIdx}])` : '0'};
      var batch: u32 =  ${inputShape.length > 2 ? `u32(originalIndices[${batchIdx}])` : '0'};
      var x11: ${dType} = getInputValue(batch, channel, row1, col1);
      var x12: ${dType} = getInputValue(batch, channel, row1, col2);
      var x21: ${dType} = getInputValue(batch, channel, row2, col1);
      var x22: ${dType} = getInputValue(batch, channel, row2, col2);
      var dx1: ${dType} = abs(row - ${dType}(row1));
      var dx2: ${dType} = abs(${dType}(row2) - row);
      var dy1: ${dType} = abs(col - ${dType}(col1));
      var dy2: ${dType} = abs(${dType}(col2) - col);
      if (row1 == row2) {
        dx1 = 0.5;
        dx2 = 0.5;
      }
      if (col1 == col2) {
        dy1 = 0.5;
        dy2 = 0.5;
      }
      return (x11 * dx2 * dy2 + x12 * dx2 * dy1 + x21 * dx1 * dy2 + x22 * dx1 * dy1);
    }`;
};

const bicubicInterpolation = (
  input: IndicesHelper,
  output: IndicesHelper,
  inputShape: readonly number[],
  outputShape: readonly number[],
  scales: readonly number[],
  roi: readonly number[],
  cubicCoeffA: number,
  useExtrapolation: boolean,
  extrapolationValue: number,
  excludeOutside: boolean,
): string => {
  const is2D = inputShape.length === 2;
  const isNchw = true;
  const [heightIdx, widthIdx] = is2D ? [0, 1] : isNchw ? [2, 3] : [1, 2];
  const dType = input.type.value;
  const createCubicInterpolationFunction = (idx: number): string => {
    const direction = idx === heightIdx ? 'row' : 'col';
    return `
      fn ${direction}CubicInterpolation(input_indices: ${input.type.indices}, output_indices: ${
        output.type.indices
      }) -> ${dType} {
        var output_index = ${output.indicesGet('output_indices', idx)};
        var originalIdx: ${dType} = getOriginalCoordinateFromResizedCoordinate(output_index, ${scales[idx]},
        ${outputShape[idx]}, ${inputShape[idx]}, ${roi[idx]}, ${roi[idx]} + ${inputShape.length});
        var fractOriginalIdx: ${dType} = originalIdx - floor(originalIdx);
        var coefs = getCubicInterpolationCoefs(fractOriginalIdx);

        if (${useExtrapolation} && (originalIdx < 0 || originalIdx > (${inputShape[idx]} - 1))) {
          return ${extrapolationValue};
        }
        var data: array<${dType}, 4> = array<${dType}, 4>(0.0, 0.0, 0.0, 0.0);
        for (var i: i32 = -1; i < 3; i++) {
          var ${direction}: ${dType} = originalIdx + ${dType}(i);
          if (${direction} < 0 || ${direction} >= ${inputShape[idx]}) {
            ${(() => {
              if (excludeOutside) {
                return `coefs[i + 1] = 0.0;
                        continue;`;
              } else if (useExtrapolation) {
                return `return ${extrapolationValue};`;
              } else {
                return `${direction} = max(0, min(${direction}, ${inputShape[idx]} - 1));`;
              }
            })()};
          }
        var input_indices_copy: ${input.type.indices} = input_indices;
          ${input.indicesSet('input_indices_copy', idx, `u32(${direction})`)};
          data[i + 1] = ${
            idx === heightIdx
              ? input.getByIndices('input_indices_copy')
              : 'rowCubicInterpolation(input_indices_copy, output_indices)'
          };
        }
        return cubicInterpolation1D(data, coefs);
      }`;
  };

  return `
    ${createCubicInterpolationFunction(heightIdx)};
    ${createCubicInterpolationFunction(widthIdx)};
  fn getCubicInterpolationCoefs(s: ${dType}) -> array<${dType}, 4> {
    var absS = abs(s);
    var coeffs: array<${dType}, 4> = array<${dType}, 4>(0.0, 0.0, 0.0, 0.0);
    var oneMinusAbsS: ${dType} = 1.0 - absS;
    var twoMinusAbsS: ${dType} = 2.0 - absS;
    var onePlusAbsS: ${dType} = 1.0 + absS;
    coeffs[0] = ((${cubicCoeffA} * onePlusAbsS - 5 * ${cubicCoeffA}) * onePlusAbsS + 8 * ${
      cubicCoeffA
    }) * onePlusAbsS - 4 * ${cubicCoeffA};
    coeffs[1] = ((${cubicCoeffA} + 2) * absS - (${cubicCoeffA} + 3)) * absS * absS + 1;
    coeffs[2] = ((${cubicCoeffA} + 2) * oneMinusAbsS - (${cubicCoeffA} + 3)) * oneMinusAbsS * oneMinusAbsS + 1;
    coeffs[3] = ((${cubicCoeffA} * twoMinusAbsS - 5 * ${cubicCoeffA}) * twoMinusAbsS + 8 * ${
      cubicCoeffA
    }) * twoMinusAbsS - 4 * ${cubicCoeffA};
    return coeffs;
  }

  fn cubicInterpolation1D(x: array<${dType}, 4>, coefs: array<${dType}, 4>) -> ${dType} {
    var coefsSum: ${dType} = coefs[0] + coefs[1] + coefs[2] + coefs[3];
    return (x[0] * coefs[0] + x[1] * coefs[1]+ x[2] * coefs[2]+ x[3] * coefs[3]) / coefsSum;
  }

  fn bicubicInterpolation(output_indices: ${output.type.indices}) -> ${dType} {
    var input_indices: ${input.type.indices} = output_indices;
    return colCubicInterpolation(input_indices, output_indices);
  }
    `;
};

const trilinearInterpolation = (
  input: IndicesHelper,
  output: IndicesHelper,
  inputShape: readonly number[],
  useExtrapolation: boolean,
  extrapolationValue: number,
): string => {
  const isNchw = true;
  const [batchIdx, depthIdx, heightIdx, widthIdx, channelIdx] =
    inputShape.length === 3 ? [-1, 0, 1, 2, -1] : isNchw ? [0, 2, 3, 4, 1] : [0, 1, 2, 3, 4];
  const dType = input.type.value;
  return `
    fn getInputValue(batch: u32, channel: u32, depth:u32, height: u32, width: u32) -> ${dType} {
      var input_indices: ${input.type.indices};
      ${input.indicesSet('input_indices', depthIdx, `max(0, min(depth, ${inputShape[depthIdx]} - 1))`)};
      ${input.indicesSet('input_indices', heightIdx, `max(0, min(height, ${inputShape[heightIdx]} - 1))`)};
      ${input.indicesSet('input_indices', widthIdx, `max(0, min(width, ${inputShape[widthIdx]} - 1))`)};
      ${setChannelAndBatchIndices(input, channelIdx, batchIdx, 3)}
      return ${input.getByIndices('input_indices')};
    }

    fn trilinearInterpolation(output_indices: ${output.type.indices}) -> ${dType} {
      var originalIndices = calculateOriginalIndicesFromOutputIndices(output_indices);
      var depth:${dType} = originalIndices[${depthIdx}];
      var height:${dType} = originalIndices[${heightIdx}];
      var width:${dType} = originalIndices[${widthIdx}];
      ${
        useExtrapolation
          ? `if (depth < 0 || depth > (${inputShape[depthIdx]} - 1) || height < 0 || height > (${
              inputShape[heightIdx]
            } - 1) || width < 0 || (width > ${inputShape[widthIdx]} - 1)) {
      return ${extrapolationValue};
        }`
          : ''
      };

    depth = max(0, min(depth, ${inputShape[depthIdx]} - 1));
      height = max(0, min(height, ${inputShape[heightIdx]} - 1));
      width = max(0, min(width, ${inputShape[widthIdx]} - 1));
      var depth1: u32 = u32(depth);
      var height1: u32 = u32(height);
      var width1: u32 = u32(width);
      var depth2: u32 = u32(depth + 1);
      var height2: u32 = u32(height + 1);
      var width2: u32 = u32(width + 1);
      var channel: u32 = ${inputShape.length > 3 ? `u32(originalIndices[${channelIdx}])` : '0'};
      var batch: u32 =  ${inputShape.length > 3 ? `u32(originalIndices[${batchIdx}])` : '0'};

      var x111: ${dType} = getInputValue(batch, channel, depth1, height1, width1);
      var x112: ${dType} = getInputValue(batch, channel, depth1, height1, width2);
      var x121: ${dType} = getInputValue(batch, channel, depth1, height2, width1);
      var x122: ${dType} = getInputValue(batch, channel, depth1, height2, width2);
      var x211: ${dType} = getInputValue(batch, channel, depth2, height1, width1);
      var x212: ${dType} = getInputValue(batch, channel, depth2, height1, width2);
      var x221: ${dType} = getInputValue(batch, channel, depth2, height2, width1);
      var x222: ${dType} = getInputValue(batch, channel, depth2, height2, width2);
      var dx1: ${dType} = abs(depth - ${dType}(depth1));
      var dx2: ${dType} = abs(${dType}(depth2) - depth);
      var dy1: ${dType} = abs(height - ${dType}(height1));
      var dy2: ${dType} = abs(${dType}(height2) - height);
      var dz1: ${dType} = abs(width - ${dType}(width1));
      var dz2: ${dType} = abs(${dType}(width2) - width);
      if (depth1 == depth2) {
        dx1 = 0.5;
        dx2 = 0.5;
      }
      if (height1 == height2) {
        dy1 = 0.5;
        dy2 = 0.5;
      }
      if (width1 == width2) {
        dz1 = 0.5;
        dz2 = 0.5;
      }
      return (x111 * dx2 * dy2 * dz2 + x112 * dx2 * dy2 * dz1 + x121 * dx2 * dy1 *dz2 + x122 * dx2 * dy1 * dz1 +
              x211 * dx1 * dy2 * dz2 + x212 * dx1 * dy2 * dz1 + x221 * dx1 * dy1 *dz2 + x222 * dx1 * dy1 * dz1);
    }`;
};

const createResizeProgramInfo = (
  inputTensor: TensorView,
  attributes: ResizeAttributes,
  opsetVersion: number,
  scalesInput: readonly number[],
  sizes: readonly number[],
  roiInput: readonly number[],
): ProgramInfo => {
  const inputShape = inputTensor.dims;
  const roi = updateRoI(roiInput, attributes.axes, inputShape.length);

  let outputShape = initOutputShape(inputShape, scalesInput, sizes, attributes.axes);
  let scales = scalesInput.slice();
  if (scalesInput.length === 0) {
    scales = inputShape.map((value, index) => (value === 0 ? 1.0 : outputShape[index] / value));
    if (attributes.keepAspectRatioPolicy !== 'stretch') {
      outputShape = adjustOutputShape(inputShape, scales, attributes);
    }
  }
  const output = outputVariable('output', inputTensor.dataType, outputShape.length);
  const input = inputVariable('input', inputTensor.dataType, inputShape.length);
  const outputSize = ShapeUtil.size(outputShape);
  const noScale = inputShape.length === outputShape.length && inputShape.every((d, i) => d === outputShape[i]);
  const useExtrapolation = attributes.coordinateTransformMode === 'tf_crop_and_resize';
  const extrapolationValue = attributes.extrapolationValue;
  const dataType = input.type.value;
  const getShaderSource = (shaderHelper: ShaderHelper) => `
      ${
        noScale
          ? ''
          : `
      ${getOriginalCoordinateFromResizedCoordinate(attributes.coordinateTransformMode, dataType)};
      ${(() => {
        switch (attributes.mode) {
          case 'nearest':
            return `
              ${checkInputIndices(input, inputShape)};
              ${getNearestPixelFromOriginal(attributes.nearestMode, opsetVersion, dataType)};
              ${calculateInputIndicesFromOutputIndices(
                input,
                output,
                inputShape,
                outputShape,
                scales.length,
                roi.length,
                useExtrapolation,
              )};
              `;
          case 'linear':
            return `
              ${calculateOriginalIndicesFromOutputIndices(output, inputShape, outputShape, scales.length, roi.length)};
              ${(() => {
                if (inputShape.length === 2 || inputShape.length === 4) {
                  return `${bilinearInterpolation(input, output, inputShape, useExtrapolation, extrapolationValue)}`;
                } else if (inputShape.length === 3 || inputShape.length === 5) {
                  return `${trilinearInterpolation(input, output, inputShape, useExtrapolation, extrapolationValue)}`;
                } else {
                  throw Error('Linear mode only supports input dims 2, 3, 4 and 5 are supported in linear mode.');
                }
              })()};
            `;
          case 'cubic':
            return `
            ${(() => {
              if (inputShape.length === 2 || inputShape.length === 4) {
                return `${bicubicInterpolation(
                  input,
                  output,
                  inputShape,
                  outputShape,
                  scales,
                  roi,
                  attributes.cubicCoeffA,
                  useExtrapolation,
                  attributes.extrapolationValue,
                  attributes.excludeOutside,
                )}`;
              } else {
                throw Error('Cubic mode only supports input dims 2 and 4 are supported in linear mode.');
              }
            })()};
            `;
          default:
            throw Error('Invalid resize mode');
        }
      })()};
      `
      }
      ${shaderHelper
        .registerUniform('output_size', 'u32')
        .registerUniform('scales', 'f32', scales.length)
        .registerUniform('roi', 'f32', roi.length)
        .declareVariables(input, output)}
      ${shaderHelper.mainStart()}
        ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
        ${
          noScale
            ? 'output[global_idx] = input[global_idx];'
            : `
        let output_indices = ${output.offsetToIndices('global_idx')};
        var input_indices: ${input.type.indices};
        ${(() => {
          switch (attributes.mode) {
            case 'nearest':
              return `input_indices = calculateInputIndicesFromOutputIndices(output_indices);
                if (checkInputIndices(input_indices)) {
                  output[global_idx] = ${input.getByIndices('input_indices')};
                } else {
                  output[global_idx] = ${attributes.extrapolationValue};
                }`;
            case 'linear':
              return `output[global_idx] = ${
                inputShape.length === 2 || inputShape.length === 4 ? 'bilinearInterpolation' : 'trilinearInterpolation'
              }(output_indices);`;
            case 'cubic':
              return 'output[global_idx] = bicubicInterpolation(output_indices);';
            default:
              throw Error(`Unsupported resize mode: ${attributes.mode}`);
          }
        })()};
`
        }
      }`;

  return {
    name: 'Resize',
    shaderCache: {
      hint: `${attributes.cacheKey}|${opsetVersion}|${
        scales.length > 0 ? (attributes.mode === 'cubic' ? scales : scales.length) : ''
      }|${sizes.length > 0 ? sizes : ''}|${roi.length > 0 ? roi : ''}|${noScale}|${
        attributes.mode === 'nearest' ? inputShape.length : inputShape
      }`,
      inputDependencies: ['rank'],
    },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputTensor.dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms: [
        { type: DataType.uint32, data: outputSize },
        { type: DataType.float, data: scales },
        { type: DataType.float, data: roi },
        ...createTensorShapeVariables(inputShape, outputShape),
      ],
    }),
  };
};

const getOpsetVersionFromCustomDataBuffer = (context: ComputeContext): number => {
  const customDataBuffer = context.customDataBuffer;
  const customDataBuffer32 = new Uint32Array(customDataBuffer, customDataBuffer.byteOffset, 1);
  const opsetVersion = customDataBuffer32[0];
  return opsetVersion;
};

export const resize = (context: ComputeContext, attributes: ResizeAttributes): void => {
  const scales: number[] = [];
  const sizes: number[] = [];
  const roi: number[] = [];

  // Note that scales in resize are always f32. roi can be f32 or f16.
  // TODO: Currently this code does not support f16 for roi when passed as optional input.

  const opsetVersion = getOpsetVersionFromCustomDataBuffer(context);
  if (attributes.antialias !== 0) {
    throw Error('Only default value (0) for Antialias attribute is supported');
  }
  validateInputs(context.inputs, attributes, opsetVersion, scales, sizes, roi);
  context.compute(createResizeProgramInfo(context.inputs[0], attributes, opsetVersion, scales, sizes, roi), {
    inputs: [0],
  });
};

export const parseResizeAttributes = (attributes: Record<string, unknown>): ResizeAttributes => {
  const antialias = attributes.antialias as number;
  const axes = attributes.axes as number[];
  const coordinateTransformMode: CoordinateTransformMode =
    attributes.coordinateTransformMode as CoordinateTransformMode;
  const cubicCoeffA = attributes.cubicCoeffA as number;
  const excludeOutside = (attributes.excludeOutside as number) !== 0;
  const extrapolationValue = attributes.extrapolationValue as number;
  const keepAspectRatioPolicy: KeepAspectRatioPolicy = attributes.keepAspectRatioPolicy as KeepAspectRatioPolicy;
  const mode: Mode = attributes.mode as Mode;
  // If nearestMode is not specified, use simple mode.
  const nearestMode: NearestMode = (attributes.nearestMode === '' ? 'simple' : attributes.nearestMode) as NearestMode;
  return createAttributeWithCacheKey({
    antialias,
    axes,
    coordinateTransformMode,
    cubicCoeffA,
    excludeOutside,
    extrapolationValue,
    keepAspectRatioPolicy,
    mode,
    nearestMode,
  });
};
