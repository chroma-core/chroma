// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { getGlsl, Glsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, TextureType } from '../types';

export interface PadAttributes extends AttributeWithCacheKey {
  readonly mode: string;
  readonly pads: number[];
  readonly value: number;
}

const padProgramMetadata = {
  name: 'Pad',
  inputNames: ['A'],
  inputTypes: [TextureType.unpacked],
};

export const padV2: OperatorImplementation<PadAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: PadAttributes,
): Tensor[] => {
  validateInputsV2(inputs);
  const output = inferenceHandler.run(
    {
      ...padProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createPadProgramInfo(inferenceHandler, inputs[0], attributes),
    },
    inputs,
  );
  return [output];
};

export const parsePadAttributesV2: OperatorInitialization<PadAttributes> = (node: Graph.Node): PadAttributes => {
  const mode = node.attributes.getString('mode', 'constant');
  const value = node.attributes.getFloat('value', 0.0);
  const pads = node.attributes.getInts('pads');
  return createAttributeWithCacheKey({ mode, value, pads });
};

export const padV11: OperatorImplementation<string> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  mode: string,
): Tensor[] => {
  validateInputsV11(inputs);
  const attrubutes = generatePadAttributesFromInputs(inferenceHandler, inputs, mode);
  return padV2(inferenceHandler, [inputs[0]], attrubutes);
};

export const parsePadAttributesV11: OperatorInitialization<string> = (node: Graph.Node): string =>
  node.attributes.getString('mode', 'constant');

const generatePadAttributesFromInputs = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  mode: string,
): PadAttributes => {
  if (
    !inferenceHandler.session.isInitializer(inputs[1].dataId) ||
    (inputs.length >= 3 && !inferenceHandler.session.isInitializer(inputs[2].dataId))
  ) {
    throw new Error('dynamic pad attributes are not allowed');
  }

  const pads = Array.from(inputs[1].integerData);
  const value = inputs.length >= 3 ? inputs[2].floatData[0] : 0.0;

  return createAttributeWithCacheKey({ mode, pads, value });
};

const createPadProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  input: Tensor,
  attributes: PadAttributes,
): ProgramInfo => {
  const outputShape = ShapeUtil.padShape(input.dims.slice(), attributes.pads);
  const rank = outputShape.length;
  const padFunction = getPadFunction(inferenceHandler, input, attributes);
  const shaderSource = `
      ${padFunction}
      float process(int[${rank}] indices) {
          return padA(indices);
      }`;
  return {
    name: 'Pad',
    inputNames: ['A'],
    inputTypes: [TextureType.unpacked],
    output: { dims: outputShape, type: input.type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

const validateInputsV2 = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Pad requires 1 input');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
};

const validateInputsV11 = (inputs: Tensor[]): void => {
  if (!inputs || (inputs.length !== 2 && inputs.length !== 3)) {
    throw new Error('Pad requires 2 or 3 inputs');
  }
  if (inputs[1].type !== 'int32') {
    throw new Error('Invalid input type.');
  }
  if (inputs.length >= 3 && inputs[2].type === 'string') {
    throw new Error('Invalid input type.');
  }
};

const getPadFunction = (inferenceHandler: WebGLInferenceHandler, input: Tensor, attributes: PadAttributes): string => {
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const [width, height] = inferenceHandler.calculateTextureWidthAndHeight(input.dims, TextureType.unpacked);
  const strides = ShapeUtil.computeStrides(input.dims);

  switch (attributes.mode) {
    case 'constant':
      return getPadConstant(glsl, input.dims, strides, width, height, attributes.pads, attributes.value);
    case 'reflect':
      return getPadReflect(glsl, input.dims, strides, width, height, attributes.pads);
    case 'edge':
      return getPadEdge(glsl, input.dims, strides, width, height, attributes.pads);
    default:
      throw new Error('Invalid mode');
  }
};

const getPadConstant = (
  glsl: Glsl,
  shape: readonly number[],
  strides: readonly number[],
  width: number,
  height: number,
  pads: number[],
  value: number,
): string => {
  const rank = shape.length;
  let block = '';
  for (let i = rank - 1; i >= 0; --i) {
    block += `
        k = m[${i}] - ${pads[i]};
        if (k < 0)  return constant;
        if (k >= ${shape[i]}) return constant;
        offset += k * ${strides[i]};
        `;
  }
  return `
      float padA(int m[${rank}]) {
        const float constant = float(${value});
        int offset = 0;
        int k = 0;
        ${block}
        vec2 coords = offsetToCoords(offset, ${width}, ${height});
        float value = getColorAsFloat(${glsl.texture2D}(A, coords));
        return value;
      }
      `;
};

const getPadReflect = (
  glsl: Glsl,
  shape: readonly number[],
  strides: readonly number[],
  width: number,
  height: number,
  pads: number[],
): string => {
  const rank = shape.length;

  let block = '';
  for (let i = rank - 1; i >= 0; --i) {
    block += `
        k = m[${i}] - ${pads[i]};
        if (k < 0) { k = -k; }
        {
          const int _2n_1 = ${2 * (shape[i] - 1)};
          k = int( mod( float(k), float(_2n_1) ) ) ;
          if(k >= ${shape[i]}) { k = _2n_1 - k; }
        }
        offset += k * ${strides[i]};
        `;
  }
  return `
      float padA(int m[${rank}]) {
        int offset = 0;
        int k = 0;
        ${block}
        vec2 coords = offsetToCoords(offset, ${width}, ${height});
        float value = getColorAsFloat(${glsl.texture2D}(A, coords));
        return value;
      }
      `;
};

const getPadEdge = (
  glsl: Glsl,
  shape: readonly number[],
  strides: readonly number[],
  width: number,
  height: number,
  pads: number[],
): string => {
  const rank = shape.length;

  let block = '';
  for (let i = rank - 1; i >= 0; --i) {
    block += `
        k = m[${i}] - ${pads[i]};
        if (k < 0)  k = 0;
        if (k >= ${shape[i]}) k = ${shape[i] - 1};
        offset += k * ${strides[i]};
      `;
  }
  return `
      float padA(int m[${rank}]) {
        int offset = 0;
        int k = 0;
        ${block}
        vec2 coords = offsetToCoords(offset, ${width}, ${height});
        float value = getColorAsFloat(${glsl.texture2D}(A, coords));
        return value;
      }
      `;
};
