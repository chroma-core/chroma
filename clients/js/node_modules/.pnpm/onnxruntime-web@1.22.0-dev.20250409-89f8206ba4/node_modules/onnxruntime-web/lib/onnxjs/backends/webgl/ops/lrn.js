'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createLrnProgramInfoLoader = exports.parseLrnAttributes = exports.lrn = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const types_1 = require('../types');
const lrn = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  // if (inferenceHandler.session.pack) {
  //   return [inferenceHandler.run(createPackedLrnProgramInfoLoader(inferenceHandler, inputs, attributes),
  //   inputs)];
  // } else {
  return [inferenceHandler.run(createLrnProgramInfoLoader(inputs, attributes), inputs)];
  //}
};
exports.lrn = lrn;
const parseLrnAttributes = (node) => {
  const alpha = node.attributes.getFloat('alpha', 0.0001);
  const beta = node.attributes.getFloat('beta', 0.75);
  const bias = node.attributes.getFloat('bias', 1.0);
  const size = node.attributes.getInt('size');
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ alpha, beta, bias, size });
};
exports.parseLrnAttributes = parseLrnAttributes;
const lrnProgramMetadata = {
  name: 'LRN',
  inputNames: ['X'],
  inputTypes: [types_1.TextureType.unpacked],
};
function createLrnProgramInfo(inputs, attributes) {
  const C = inputs[0].dims[1];
  const rank = inputs[0].dims.length;
  const from = -Math.floor((attributes.size - 1) / 2);
  const to = Math.ceil((attributes.size - 1) / 2);
  const alpha = `float(${attributes.alpha}) / float(${attributes.size})`;
  const bias = `float(${attributes.bias})`;
  const beta = `float(${attributes.beta})`;
  const shaderSource = `
    float process(int indices[${rank}]) {
        int c = indices[1];
        float x = _X(indices);
        float square_sum = 0.0;

        for (int i = ${from}; i <= ${to}; i++) {
          int idx = c + i;
          if (c >= 0 && c < ${C}) {
            indices[1] = idx;
            float j = _X(indices);
            square_sum += j * j;
          }
        }
        return x / pow(${bias} + ${alpha} * square_sum, ${beta});
    }`;
  return {
    ...lrnProgramMetadata,
    cacheHint: attributes.cacheKey,
    output: { dims: inputs[0].dims, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
}
function createLrnProgramInfoLoader(inputs, attributes) {
  return { ...lrnProgramMetadata, cacheHint: attributes.cacheKey, get: () => createLrnProgramInfo(inputs, attributes) };
}
exports.createLrnProgramInfoLoader = createLrnProgramInfoLoader;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('LRN requires 1 input.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('currently only support LRN for input with "NCHW" format');
  }
  if (inputs[0].type !== 'float32') {
    throw new Error('input should be float type');
  }
};
//# sourceMappingURL=lrn.js.map
