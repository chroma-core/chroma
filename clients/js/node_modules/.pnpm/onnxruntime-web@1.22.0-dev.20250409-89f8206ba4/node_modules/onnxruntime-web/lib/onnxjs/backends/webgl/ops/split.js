'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseSplitAttributes = exports.split = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const types_1 = require('../types');
const splitProgramMetadata = {
  name: 'Split',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpacked],
};
const split = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const axis = util_1.ShapeUtil.normalizeAxis(attributes.axis, inputs[0].dims.length);
  const count = getProgramCount(inferenceHandler, inputs, axis, attributes);
  const output = [];
  for (let i = 0; i < count; ++i) {
    output.push(
      inferenceHandler.run(
        {
          ...splitProgramMetadata,
          cacheHint: `${attributes.cacheKey};${i}`,
          get: () => createSplitProgramInfo(inferenceHandler, inputs[0], attributes, axis, i),
        },
        inputs,
      ),
    );
  }
  return output;
};
exports.split = split;
const parseSplitAttributes = (node) => {
  const axis = node.attributes.getInt('axis', 0);
  const split = node.attributes.getInts('split', []);
  const numOutputs = node.outputs.length;
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ axis, split, numOutputs });
};
exports.parseSplitAttributes = parseSplitAttributes;
const getProgramCount = (_inferenceHandler, inputs, axis, attributes) => {
  const [, offsets] = util_1.SplitUtil.splitShape(inputs[0].dims, axis, attributes.split, attributes.numOutputs);
  return offsets.length;
};
const createSplitProgramInfo = (_inferenceHandler, input, attributes, axis, index) => {
  const [shapes, offsets] = util_1.SplitUtil.splitShape(input.dims, axis, attributes.split, attributes.numOutputs);
  const offset = offsets[index];
  const outputShape = shapes[index];
  const rank = outputShape.length;
  const shaderSource = `
      float process(int indices[${rank}]) {
        indices[${axis}] += ${offset};
        return _A(indices);
      }
    `;
  return {
    ...splitProgramMetadata,
    cacheHint: `${attributes.cacheKey}:${index}`,
    output: { dims: outputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Split requires one input.');
  }
  if (
    inputs[0].type !== 'int8' &&
    inputs[0].type !== 'uint8' &&
    inputs[0].type !== 'int16' &&
    inputs[0].type !== 'uint16' &&
    inputs[0].type !== 'int32' &&
    inputs[0].type !== 'uint32' &&
    inputs[0].type !== 'float32' &&
    inputs[0].type !== 'float64' &&
    inputs[0].type !== 'bool'
  ) {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=split.js.map
