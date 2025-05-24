'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseGatherAttributes = exports.gather = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const operators_1 = require('../../../operators');
const util_1 = require('../../../util');
const types_1 = require('../types');
const gather = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs, attributes.axis);
  const output = inferenceHandler.run(createGatherProgramInfoLoader(inferenceHandler, inputs, attributes), inputs);
  return [output];
};
exports.gather = gather;
const parseGatherAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ axis: node.attributes.getInt('axis', 0) });
exports.parseGatherAttributes = parseGatherAttributes;
const gatherProgramMetadata = {
  name: 'Gather',
  inputNames: ['A', 'B'],
  inputTypes: [types_1.TextureType.unpacked, types_1.TextureType.unpacked],
};
const createGatherProgramInfo = (_handler, metadata, inputs, axis) => {
  const inputShape = inputs[0].dims.slice();
  const indexDataShape = inputs[1].dims.slice();
  const outputShape = new Array(inputShape.length + indexDataShape.length - 1);
  axis = util_1.ShapeUtil.normalizeAxis(axis, inputShape.length);
  const indexCopyOps = [];
  for (let i = 0; i < outputShape.length; i++) {
    // outputShape is divided into three parts: A, B, C
    // |0        axis|  axis + indexDataShape.length |          end|
    // |     A       |             B                 |      C      |
    //
    // inputIdx: [A, inputs[1][B], C]
    if (i < axis) {
      // A
      outputShape[i] = inputShape[i];
      indexCopyOps.push(`inputIdx[${i}] = outputIdx[${i}];`);
    } else {
      if (i < axis + indexDataShape.length) {
        // B
        outputShape[i] = indexDataShape[i - axis];
        indexCopyOps.push(`indexDataIdx[${i - axis}] = outputIdx[${i}];`);
      } else {
        // C
        outputShape[i] = inputShape[i - indexDataShape.length + 1]; // skip 1 for axis
        indexCopyOps.push(`inputIdx[${i - indexDataShape.length + 1}] = outputIdx[${i}];`);
      }
    }
  }
  const orank = outputShape.length || 1;
  const irank = inputShape.length;
  const iDrank = indexDataShape.length || 1;
  const shaderSource = `
      float process(int outputIdx[${orank}]) {
        int inputIdx[${irank}];
        int indexDataIdx[${iDrank}];
        indexDataIdx[0] = 0;
        ${indexCopyOps.join('\n        ')}
        int idx = int(_B(indexDataIdx));
        inputIdx[${axis}] = idx < 0 ? idx + ${inputShape[axis]} : idx;
        return _A(inputIdx);
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const createGatherProgramInfoLoader = (handler, inputs, attributes) => {
  const metadata = { ...gatherProgramMetadata, cacheHint: attributes.cacheKey };
  return { ...metadata, get: () => createGatherProgramInfo(handler, metadata, inputs, attributes.axis) };
};
const validateInputs = (inputs, axis) => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Gather requires 2 inputs.');
  }
  const tensorRank = inputs[0].dims.length;
  if (tensorRank < 1) {
    throw new Error('Invalid input shape.');
  }
  if (axis < -tensorRank || axis > tensorRank - 1) {
    throw new Error('Invalid axis.');
  }
  if (operators_1.NUMBER_TYPES.indexOf(inputs[0].type) === -1) {
    throw new Error('Invaid input type.');
  }
  if (inputs[1].type !== 'int32' && inputs[1].type !== 'int16') {
    throw new Error('Invaid input type.');
  }
};
//# sourceMappingURL=gather.js.map
