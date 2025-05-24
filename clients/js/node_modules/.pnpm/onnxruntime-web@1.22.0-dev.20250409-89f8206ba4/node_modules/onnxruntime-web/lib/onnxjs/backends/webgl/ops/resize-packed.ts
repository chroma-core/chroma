// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, TextureType } from '../types';
import { getCoordsDataType } from '../utils';

import { unpackFromChannel } from './packing-utils';
import { parseUpsampleAttributes, scalesValidation, UpsampleAttributes, validateInputs } from './upsample';

const resizeProgramMetadata = {
  name: 'Resize',
  inputNames: ['A'],
  inputTypes: [TextureType.packed],
};

export const resize: OperatorImplementation<UpsampleAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: UpsampleAttributes,
): Tensor[] => {
  validateInputs(inputs, attributes);
  const output = inferenceHandler.run(
    {
      ...resizeProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createPackedResizeProgramInfo(inferenceHandler, inputs, attributes),
    },
    inputs,
  );
  return [output];
};

export const parseResizeAttributesV10: OperatorInitialization<UpsampleAttributes> = (
  node: Graph.Node,
): UpsampleAttributes => parseUpsampleAttributes(node, 10);

export const parseResizeAttributesV11: OperatorInitialization<UpsampleAttributes> = (
  node: Graph.Node,
): UpsampleAttributes => parseUpsampleAttributes(node, 11);

const createPackedResizeProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: UpsampleAttributes,
): ProgramInfo => {
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const [scales, outputShape] = prepareInputs(inputs, attributes);

  const isSame = scales.every((s: number) => s === 1) && attributes.coordinateTransformMode !== 'tf_crop_and_resize';
  if (isSame) {
    return {
      ...resizeProgramMetadata,
      output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.packed },
      hasMain: true,
      shaderSource: `void main() {
                    vec4 v = ${glsl.texture2D}(X, TexCoords);
                    ${glsl.output} = v;
                }`,
    };
  }

  const dim = outputShape.length;
  if (dim < 2) {
    throw new Error(`output dimension should be at least 2, but got ${dim}`);
  }

  const outputHeight = outputShape[dim - 2];
  const outputWidth = outputShape[dim - 1];

  const inputShape = inputs[0].dims;
  if (dim !== inputShape.length) {
    throw new Error(`output dimension should match input ${inputShape.length}, but got ${dim}`);
  }
  const inputHeight = inputShape[dim - 2];
  const inputWidth = inputShape[dim - 1];

  const scalesHeight = scales[dim - 2];
  const scalesWidth = scales[dim - 1];

  let getSourceFracIndex = '';

  if (attributes.mode !== 'linear') {
    // TODO: support other modes
    throw new Error(`resize (packed) does not support mode: '${attributes.mode}'`);
  }
  switch (attributes.coordinateTransformMode) {
    case 'asymmetric':
      getSourceFracIndex = `
                    vec4 getSourceFracIndex(ivec4 coords) {
                        return vec4(coords) / scaleWHWH;
                    }
                `;
      break;
    case 'half_pixel':
      getSourceFracIndex = `
                    vec4 getSourceFracIndex(ivec4 coords) {
                        return (vec4(coords) + 0.5) / scaleWHWH - 0.5;
                    }
                `;
      break;
    case 'pytorch_half_pixel':
      getSourceFracIndex = `
                    vec4 getSourceFracIndex(ivec4 coords) {
                        vec4 fcoords = vec4(coords);
                        return vec4(
                            ${outputWidth}.0 > 1.0 ? (fcoords.x + 0.5) / scaleWHWH.x - 0.5 : 0.0,
                            ${outputHeight}.0 > 1.0 ? (fcoords.y + 0.5) / scaleWHWH.y - 0.5 : 0.0,
                            ${outputWidth}.0 > 1.0 ? (fcoords.z + 0.5) / scaleWHWH.z - 0.5 : 0.0,
                            ${outputHeight}.0 > 1.0 ? (fcoords.w + 0.5) / scaleWHWH.w - 0.5 : 0.0
                          );
                    }
                `;
      break;
    case 'align_corners':
      getSourceFracIndex = `
                    vec4 getSourceFracIndex(ivec4 coords) {
                        vec4 resized = vec4(${outputWidth}.0 - 1.0, ${outputHeight}.0 - 1.0, ${outputWidth}.0 - 1.0,
                            ${outputHeight}.0 - 1.0);
                        vec4 original = vec4(${inputWidth}.0 - 1.0, ${inputHeight}.0 - 1.0, ${inputWidth}.0 - 1.0,
                            ${inputHeight}.0 - 1.0);
                        vec4 new_scale = original / resized;
                        return vec4(coords) * new_scale;
                    }
                `;
      break;
    default:
      // TODO:supporting other coordinateTransformModes
      throw new Error(`resize (packed) does not support coordinateTransformMode: \
                                '${attributes.coordinateTransformMode}'`);
  }

  const coordsDataType = getCoordsDataType(dim);
  const unpackChannel = unpackFromChannel();
  const shaderSource = `
            const vec2 inputWH = vec2(${inputHeight}.0, ${inputWidth}.0);
            const vec4 scaleWHWH = vec4(float(${scalesHeight}), float(${scalesWidth}), float(${scalesHeight}), float(${
              scalesWidth
            }));
            ${unpackChannel}
            ${getSourceFracIndex}
            float getAValue(int x10, int r, int c, int d) {
                return getChannel(getA(x10, r, c, d), vec2(c, d));
            }
            void main() {
                ${coordsDataType} rc = getOutputCoords();

                int batch = rc[0];
                int depth = rc[1];

                // retrieve the 4 coordinates that is used in the 4 packed output values.
                ivec4 coords = ivec4(rc.wz, rc.w + 1, rc.z + 1);

                // calculate the source index in fraction
                vec4 sourceFrac = getSourceFracIndex(coords);

                // get the lower and upper bound of the 4 values that will be packed into one texel.
                ivec4 x00 = ivec4(max(sourceFrac.xy, vec2(0.0)), min(inputWH - 1.0, ceil(sourceFrac.xy)));
                ivec4 x01 = ivec4(max(sourceFrac.xw, vec2(0.0)), min(inputWH - 1.0, ceil(sourceFrac.xw)));
                ivec4 x10 = ivec4(max(sourceFrac.zy, vec2(0.0)), min(inputWH - 1.0, ceil(sourceFrac.zy)));
                ivec4 x11 = ivec4(max(sourceFrac.zw, vec2(0.0)), min(inputWH - 1.0, ceil(sourceFrac.zw)));

                bool hasNextRow = rc.w < ${outputHeight - 1};
                bool hasNextCol = rc.z < ${outputWidth - 1};

                // pack x00, x01, x10, x11's top-left corner into one vec4 structure
                vec4 topLeft = vec4(
                    getAValue(batch, depth, x00.x, x00.y),
                    hasNextCol ? getAValue(batch, depth, x01.x, x01.y) : 0.0,
                    hasNextRow ? getAValue(batch, depth, x10.x, x10.y) : 0.0,
                    (hasNextRow && hasNextCol) ? getAValue(batch, depth, x11.x, x11.y) : 0.0);

                // pack x00, x01, x10, x11's top-right corner into one vec4 structure
                vec4 topRight = vec4(
                    getAValue(batch, depth, x00.x, x00.w),
                    hasNextCol ? getAValue(batch, depth, x01.x, x01.w) : 0.0,
                    hasNextRow ? getAValue(batch, depth, x10.x, x10.w) : 0.0,
                    (hasNextRow && hasNextCol) ? getAValue(batch, depth, x11.x, x11.w) : 0.0);

                // pack x00, x01, x10, x11's bottom-left corner into one vec4 structure
                vec4 bottomLeft = vec4(
                    getAValue(batch, depth, x00.z, x00.y),
                    hasNextCol ? getAValue(batch, depth, x01.z, x01.y) : 0.0,
                    hasNextRow ? getAValue(batch, depth, x10.z, x10.y) : 0.0,
                    (hasNextRow && hasNextCol) ? getAValue(batch, depth, x11.z, x11.y) : 0.0);

                // pack x00, x01, x10, x11's bottom-right corner into one vec4 structure
                vec4 bottomRight = vec4(
                    getAValue(batch, depth, x00.z, x00.w),
                    hasNextCol ? getAValue(batch, depth, x01.z, x01.w) : 0.0,
                    hasNextRow ? getAValue(batch, depth, x10.z, x10.w) : 0.0,
                    (hasNextRow && hasNextCol) ? getAValue(batch, depth, x11.z, x11.w) : 0.0);

                // calculate the interpolation fraction on u and v direction
                vec4 frac = vec4(sourceFrac) - floor(sourceFrac);
                vec4 clampFrac = clamp(frac, vec4(0.0), vec4(1.0));

                vec4 top = mix(topLeft, topRight, clampFrac.ywyw);
                vec4 bottom = mix(bottomLeft, bottomRight, clampFrac.ywyw);
                vec4 newValue = mix(top, bottom, clampFrac.xxzz);

                ${glsl.output} = vec4(newValue);
            }
        `;
  return {
    ...resizeProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.packed },
    hasMain: true,
    shaderSource,
  };
};

const prepareInputs = (inputs: Tensor[], attributes: UpsampleAttributes): [readonly number[], readonly number[]] => {
  const x = inputs[0];
  const xDims = x.dims;

  let scales = attributes.scales;
  let outputSizes: number[] | undefined;
  if (scales.length === 0) {
    const scalesTensor = inputs[attributes.scalesInputIdx];
    if (scalesTensor && scalesTensor.size !== 0) {
      if (inputs[attributes.sizesInputIdx]) {
        throw new Error('Only one of scales or sizes must be provided as input.');
      }
      scales = parseScalesData(scalesTensor, attributes.mode, attributes.isResize);
    } else {
      const sizesTensor = inputs[attributes.sizesInputIdx];
      if (!sizesTensor || sizesTensor.size === 0) {
        throw new Error('Either scales or sizes MUST be provided as input.');
      }

      outputSizes = Array.from(sizesTensor.integerData);
      scales = parseScalesDataFromOutputSize(outputSizes, xDims, attributes.mode, attributes.isResize);
    }
  } else {
    if (inputs[attributes.sizesInputIdx]) {
      throw new Error('Only one of scales or sizes must be provided as input.');
    }
  }

  const yDims = outputSizes || xDims.map((dim, i) => Math.floor(dim * scales[i]));

  return [scales, yDims];
};

const parseScalesData = (scale: Tensor, mode: string, isResize: boolean): number[] => {
  const scales = Array.from(scale.floatData);
  scalesValidation(scales, mode, isResize);
  return scales;
};

const parseScalesDataFromOutputSize = (
  yDims: readonly number[],
  xDims: readonly number[],
  mode: string,
  isResize: boolean,
): number[] => {
  const length = xDims.length;
  const scales = new Array<number>(length);

  for (let i = 0, end = length; i < end; i++) {
    if (xDims[i] === 0) {
      if (yDims[i] !== 0) {
        throw new Error('Input dim is zero but required output dim is non-zero.');
      }
      scales[i] = 1;
    } else {
      scales[i] = yDims[i] / xDims[i];
    }
  }
  scalesValidation(scales, mode, isResize);
  return scales;
};

// roi data is not used yet. but leave here for future usage.
// const getRoi = (inputs: Tensor[], attributes: UpsampleAttributes) : number[] => {
//     let roi: number[] = [];
//     if (attributes.needRoiInput) {
//         if (attributes.roiInputIdx <= 0) {
//             throw new Error('Invalid roi input index.');
//         }
//         const roiTensor = inputs[attributes.roiInputIdx];
//         roi = roiTensor.size > 0 ? Array.from(roiTensor.floatData) : [];
//     } else {
//         roi = new Array(inputs[0].dims.length * 2).fill(0);
//     }
//     return roi;
// };
