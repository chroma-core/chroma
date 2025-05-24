'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseImageScalerAttributes = exports.imageScaler = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const types_1 = require('../types');
const imageScaler = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const output = inferenceHandler.run(createImageScalerProgramInfoLoader(inferenceHandler, inputs, attributes), inputs);
  return [output];
};
exports.imageScaler = imageScaler;
const parseImageScalerAttributes = (node) => {
  const scale = node.attributes.getFloat('scale');
  const bias = node.attributes.getFloats('bias');
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ scale, bias });
};
exports.parseImageScalerAttributes = parseImageScalerAttributes;
const imageScalerProgramMetadata = {
  name: 'ImageScaler',
  inputNames: ['X'],
  inputTypes: [types_1.TextureType.unpacked],
};
const createImageScalerProgramInfo = (_handler, metadata, inputs, attributes) => {
  const outputShape = inputs[0].dims.slice();
  const rank = outputShape.length;
  const getBiasMethod = createGetBiasMethod(attributes.bias.length);
  const shaderSource = `
      ${getBiasMethod}
      float process(int indices[${rank}]) {
        return _X(indices) * scale + getBias(bias, indices[1]);
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    variables: [
      { name: 'bias', type: 'float', arrayLength: attributes.bias.length, data: attributes.bias },
      { name: 'scale', type: 'float', data: attributes.scale },
    ],
    shaderSource,
  };
};
const createImageScalerProgramInfoLoader = (handler, inputs, attributes) => {
  const metadata = { ...imageScalerProgramMetadata, cacheHint: attributes.cacheKey };
  return { ...metadata, get: () => createImageScalerProgramInfo(handler, metadata, inputs, attributes) };
};
const createGetBiasMethod = (numChannels) => {
  const codeLines = [`float getBias(float bias[${numChannels}], int channel) {`];
  for (let i = 0; i < numChannels; ++i) {
    if (i === 0) {
      codeLines.push('\t' + `if (channel == ${i}) { return bias[${i}]; }`);
    } else if (i === numChannels - 1) {
      codeLines.push('\t' + `else { return bias[${i}]; }`);
    } else {
      codeLines.push('\t' + `else if (channel == ${i}) { return bias[${i}]; }`);
    }
  }
  codeLines.push('\t' + '}');
  return codeLines.join('\n');
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('ImageScaler requires 1 input.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('Invalid input shape.');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=image-scaler.js.map
