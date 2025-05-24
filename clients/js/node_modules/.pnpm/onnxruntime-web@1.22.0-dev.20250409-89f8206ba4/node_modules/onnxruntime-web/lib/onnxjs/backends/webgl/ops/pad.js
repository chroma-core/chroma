'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parsePadAttributesV11 = exports.padV11 = exports.parsePadAttributesV2 = exports.padV2 = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const padProgramMetadata = {
  name: 'Pad',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpacked],
};
const padV2 = (inferenceHandler, inputs, attributes) => {
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
exports.padV2 = padV2;
const parsePadAttributesV2 = (node) => {
  const mode = node.attributes.getString('mode', 'constant');
  const value = node.attributes.getFloat('value', 0.0);
  const pads = node.attributes.getInts('pads');
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ mode, value, pads });
};
exports.parsePadAttributesV2 = parsePadAttributesV2;
const padV11 = (inferenceHandler, inputs, mode) => {
  validateInputsV11(inputs);
  const attrubutes = generatePadAttributesFromInputs(inferenceHandler, inputs, mode);
  return (0, exports.padV2)(inferenceHandler, [inputs[0]], attrubutes);
};
exports.padV11 = padV11;
const parsePadAttributesV11 = (node) => node.attributes.getString('mode', 'constant');
exports.parsePadAttributesV11 = parsePadAttributesV11;
const generatePadAttributesFromInputs = (inferenceHandler, inputs, mode) => {
  if (
    !inferenceHandler.session.isInitializer(inputs[1].dataId) ||
    (inputs.length >= 3 && !inferenceHandler.session.isInitializer(inputs[2].dataId))
  ) {
    throw new Error('dynamic pad attributes are not allowed');
  }
  const pads = Array.from(inputs[1].integerData);
  const value = inputs.length >= 3 ? inputs[2].floatData[0] : 0.0;
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ mode, pads, value });
};
const createPadProgramInfo = (inferenceHandler, input, attributes) => {
  const outputShape = util_1.ShapeUtil.padShape(input.dims.slice(), attributes.pads);
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
    inputTypes: [types_1.TextureType.unpacked],
    output: { dims: outputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputsV2 = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Pad requires 1 input');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
};
const validateInputsV11 = (inputs) => {
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
const getPadFunction = (inferenceHandler, input, attributes) => {
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const [width, height] = inferenceHandler.calculateTextureWidthAndHeight(input.dims, types_1.TextureType.unpacked);
  const strides = util_1.ShapeUtil.computeStrides(input.dims);
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
const getPadConstant = (glsl, shape, strides, width, height, pads, value) => {
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
const getPadReflect = (glsl, shape, strides, width, height, pads) => {
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
const getPadEdge = (glsl, shape, strides, width, height, pads) => {
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
//# sourceMappingURL=pad.js.map
