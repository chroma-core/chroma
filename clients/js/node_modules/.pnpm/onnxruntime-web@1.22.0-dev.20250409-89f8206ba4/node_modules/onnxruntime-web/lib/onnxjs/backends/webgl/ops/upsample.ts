// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, TextureType } from '../types';

export interface UpsampleAttributes extends AttributeWithCacheKey {
  readonly opset: number;
  readonly isResize: boolean;
  readonly mode: string;
  readonly scales: number[];
  readonly extrapolationValue: number;
  readonly coordinateTransformMode: string;
  readonly useExtrapolation: boolean;
  readonly needRoiInput: boolean;
  readonly nearestMode: string;
  readonly cubicCoefficientA: number;
  readonly excludeOutside: boolean;
  readonly useNearest2xOptimization: boolean;
  readonly roiInputIdx: number;
  readonly scalesInputIdx: number;
  readonly sizesInputIdx: number;
}

const upsampleProgramMetadata = {
  name: 'Upsample',
  inputNames: ['X'],
  inputTypes: [TextureType.unpacked],
};

export const upsample: OperatorImplementation<UpsampleAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: UpsampleAttributes,
): Tensor[] => {
  validateInputs(inputs, attributes);
  const output = inferenceHandler.run(
    {
      ...upsampleProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createUpsampleProgramInfo(inferenceHandler, inputs, attributes),
    },
    inputs,
  );
  return [output];
};

export const parseUpsampleAttributesV7: OperatorInitialization<UpsampleAttributes> = (
  node: Graph.Node,
): UpsampleAttributes => parseUpsampleAttributes(node, 7);

export const parseUpsampleAttributesV9: OperatorInitialization<UpsampleAttributes> = (
  node: Graph.Node,
): UpsampleAttributes => parseUpsampleAttributes(node, 9);

export const parseUpsampleAttributes = (node: Graph.Node, opset: number): UpsampleAttributes => {
  const isResize = opset >= 10;

  // processing node attributes
  const mode = node.attributes.getString('mode', 'nearest');
  if (mode !== 'nearest' && mode !== 'linear' && (opset < 11 || mode !== 'cubic')) {
    throw new Error(`unrecognized mode: ${mode}`);
  }

  let scales: number[] = [];
  if (opset < 9) {
    scales = node.attributes.getFloats('scales');
    scalesValidation(scales, mode, isResize);
  }

  const extrapolationValue = node.attributes.getFloat('extrapolation_value', 0.0);

  const coordinateTransformMode =
    opset > 10 ? node.attributes.getString('coordinate_transformation_mode', 'half_pixel') : 'asymmetric';
  if (
    [
      'asymmetric',
      'pytorch_half_pixel',
      'tf_half_pixel_for_nn',
      'align_corners',
      'tf_crop_and_resize',
      'half_pixel',
    ].indexOf(coordinateTransformMode) === -1
  ) {
    throw new Error(`coordinate_transform_mode '${coordinateTransformMode}' is not supported`);
  }
  const needRoiInput = coordinateTransformMode === 'tf_crop_and_resize';
  const useExtrapolation = needRoiInput;

  const nearestMode =
    mode === 'nearest' && opset >= 11 ? node.attributes.getString('nearest_mode', 'round_prefer_floor') : '';
  if (['round_prefer_floor', 'round_prefer_ceil', 'floor', 'ceil', ''].indexOf(nearestMode) === -1) {
    throw new Error(`nearest_mode '${nearestMode}' is not supported`);
  }

  const cubicCoefficientA = node.attributes.getFloat('cubic_coeff_a', -0.75);
  const excludeOutside = node.attributes.getInt('exclude_outside', 0) !== 0;
  if (excludeOutside && mode !== 'cubic') {
    throw new Error('exclude_outside can be set to 1 only when mode is CUBIC.');
  }

  const useNearest2xOptimization =
    opset < 11 ? true : mode === 'nearest' && coordinateTransformMode === 'asymmetric' && nearestMode === 'floor';

  let roiInputIdx = 0;
  let scalesInputIdx = 0;
  let sizesInputIdx = 0;

  if (opset > 10) {
    // handle when roiInput is not given
    if (node.inputs.length > 2) {
      roiInputIdx = 1;
      scalesInputIdx = 2;
      sizesInputIdx = 3;
    } else {
      scalesInputIdx = 1;
      sizesInputIdx = 2;
    }
  } else if (opset === 9) {
    scalesInputIdx = 1;
  }

  return createAttributeWithCacheKey({
    opset,
    isResize,
    mode,
    scales,
    extrapolationValue,
    coordinateTransformMode,
    useExtrapolation,
    needRoiInput,
    nearestMode,
    cubicCoefficientA,
    excludeOutside,
    useNearest2xOptimization,
    roiInputIdx,
    scalesInputIdx,
    sizesInputIdx,
  });
};

const createUpsampleProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: UpsampleAttributes,
): ProgramInfo => {
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const [inputWidth, inputHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    inputs[0].dims,
    TextureType.unpacked,
  );

  const outputShape = inputs[0].dims.map((dim, i) => Math.floor(dim * attributes.scales[i]));
  const [outputWidth, outputHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    outputShape,
    TextureType.unpacked,
  );
  const dim = outputShape.length;

  const outputPitches = new Array<number>(dim);
  const inputPitches = new Array<number>(dim);
  let precalculatedPitches = `
      int output_pitches[${dim}];
      int input_pitches[${dim}];
      `;
  for (let d = dim - 1; d >= 0; d--) {
    outputPitches[d] = d === dim - 1 ? 1 : outputPitches[d + 1] * outputShape[d + 1];
    inputPitches[d] = d === dim - 1 ? 1 : inputPitches[d + 1] * inputs[0].dims[d + 1];

    precalculatedPitches += `
        output_pitches[${d}] = ${outputPitches[d]};
        input_pitches[${d}] = ${inputPitches[d]};
        `;
  }
  const getInputFloatFunction = `
      float getInputFloat(int index) {
        vec2 coords = offsetToCoords(index, ${inputWidth}, ${inputHeight});
        float value = getColorAsFloat(${glsl.texture2D}(X, coords));
        return value;
      }
      `;

  const shaderSource =
    attributes.mode === 'nearest'
      ? // nearest
        `
    ${getInputFloatFunction}
    float process(int indices[${dim}]) {
      int input_index = 0;
      int output_index = coordsToOffset(TexCoords, ${outputWidth}, ${outputHeight});

      ${precalculatedPitches}

      int d, m;
      for (int dim = 0; dim < ${dim}; ++dim) {
        d = output_index / output_pitches[dim];
        m = output_index - d * output_pitches[dim];
        output_index = m;

        if (scales[dim] != 1 && d > 0) {
          int d2 = d / scales[dim];
          m = d - d2 * scales[dim];
          d = d2;
        }
        input_index += input_pitches[dim] * d;
      }

      return getInputFloat(input_index);
    }`
      : dim === 4
        ? // bilinear 4D
          `
    ${getInputFloatFunction}
    float process(int indices[4]) {
      int input_index = 0;
      int output_index = coordsToOffset(TexCoords, ${outputWidth}, ${outputHeight});

      ${precalculatedPitches}

      int m;
      int index_of_dim0, index_of_dim1, index_of_dim2, index_of_dim3;
      index_of_dim0 = output_index / output_pitches[0];
      m = output_index - index_of_dim0 * output_pitches[0];
      index_of_dim1 = m / output_pitches[1];
      m = m - index_of_dim1 * output_pitches[1];
      index_of_dim2 = m / output_pitches[2];
      m = m - index_of_dim2 * output_pitches[2];
      index_of_dim3 = m;

      int index_of_input_dim2, index_of_input_dim3, x_offset, y_offset;
      index_of_input_dim2 = index_of_dim2 / scales[2];
      y_offset = index_of_dim2 - index_of_input_dim2 * scales[2];
      index_of_input_dim3 = index_of_dim3 / scales[3];
      x_offset = index_of_dim3 - index_of_input_dim3 * scales[3];

      input_index = index_of_dim0 * input_pitches[0] +
            index_of_dim1 * input_pitches[1] +
            index_of_input_dim2 * input_pitches[2] +
            index_of_input_dim3;

      float x00 = getInputFloat(input_index);
      float x10, x01, x11;

      bool end_of_dim2 = false;
      if (index_of_input_dim2 == (${inputs[0].dims[2]} - 1)) {
        // It's the end in dimension 2
        x01 = x00;
        end_of_dim2 = true;
      } else {
        x01 = getInputFloat(input_index + input_pitches[2]);
      }

      if (index_of_input_dim3 == (input_pitches[2] - 1)) {
        // It's the end in dimension 3
        x10 = x00;
        x11 = x01;
      }
      else {
        x10 = getInputFloat(input_index + 1);
        x11 = end_of_dim2 ? x10 : getInputFloat(input_index + input_pitches[2] + 1);
      }

      float y0 = x00 + float(y_offset) * (x01 - x00) / float(scales[2]);
      float y1 = x10 + float(y_offset) * (x11 - x10) / float(scales[2]);
      return y0 + float(x_offset) * (y1 - y0) / float(scales[3]);
    }`
        : // bilinear 2D
          `
    ${getInputFloatFunction}
    float process(int indices[2]) {
      int input_index = 0;
      int output_index = coordsToOffset(TexCoords, ${outputWidth}, ${outputHeight});

      ${precalculatedPitches}

      int m;
      int index_of_dim0, index_of_dim1;
      index_of_dim0 = output_index / output_pitches[0];
      m = output_index - index_of_dim0 * output_pitches[0];
      index_of_dim1 = m;

      int index_of_input_dim0, index_of_input_dim1, x_offset, y_offset;
      index_of_input_dim0 = index_of_dim0 / scales[0];
      y_offset = index_of_dim0 - index_of_input_dim0 * scales[0];
      index_of_input_dim1 = index_of_dim1 / scales[1];
      x_offset = index_of_dim1 - index_of_input_dim1 * scales[1];

      input_index = index_of_input_dim0 * input_pitches[0] + index_of_input_dim1;

      float x00 = getInputFloat(input_index);
      float x10, x01, x11;

      bool end_of_dim0 = false;
      if (index_of_input_dim0 == (${inputs[0].dims[0]} - 1)) {
        // It's the end in dimension 0
        x01 = x00;
        end_of_dim0 = true;
      } else {
        x01 = getInputFloat(input_index + input_pitches[0]);
      }

      if (index_of_input_dim1 == (input_pitches[0] - 1)) {
        // It's the end in dimension 1
        x10 = x00;
        x11 = x01;
      }
      else {
        x10 = getInputFloat(input_index + 1);
        x11 = end_of_dim0 ? x10 : getInputFloat(input_index + input_pitches[0] + 1);
      }

      float y0 = x00 + float(y_offset) * (x01 - x00) / float(scales[0]);
      float y1 = x10 + float(y_offset) * (x11 - x10) / float(scales[0]);
      return y0 + float(x_offset) * (y1 - y0) / float(scales[1]);
    }`;
  return {
    ...upsampleProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
    variables: [
      {
        name: 'scales',
        type: 'int',
        arrayLength: attributes.scales.length,
        data: attributes.scales.map((x) => Math.ceil(x)),
      },
    ],
  };
};

export const validateInputs = (inputs: Tensor[], attribute: UpsampleAttributes): void => {
  if (
    !inputs ||
    (attribute.opset < 9 && inputs.length !== 1) ||
    (attribute.opset >= 9 && attribute.opset < 11 && inputs.length !== 2) ||
    (attribute.opset >= 11 && inputs.length < 2)
  ) {
    throw new Error('invalid inputs.');
  }

  if (attribute.scales.length > 0 && inputs[0].dims.length !== attribute.scales.length) {
    throw new Error('Invalid input shape.');
  }

  if (inputs[0].type === 'string') {
    throw new Error('Invalid input tensor types.');
  }
};

export const scalesValidation = (scales: number[], mode: string, isResize: boolean): void => {
  if (!isResize) {
    for (const scale of scales) {
      if (scale < 1) {
        throw new Error('Scale value should be greater than or equal to 1.');
      }
    }
  } else {
    for (const scale of scales) {
      if (scale <= 0) {
        throw new Error('Scale value should be greater than 0.');
      }
    }
  }
  if (mode === 'linear' || mode === 'cubic') {
    if (scales.length !== 2 && (scales.length !== 4 || scales[0] !== 1 || scales[1] !== 1)) {
      throw new Error(`'Linear' mode and 'Cubic' mode only support 2-D inputs ('Bilinear', 'Bicubic') \
        or 4-D inputs with the corresponding outermost 2 scale values being 1 \
        in the ${isResize ? 'Resize' : 'Upsample'} opeartor.`);
    }
  }
};
