'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseUnsqueezeAttributes = exports.unsqueezeV13 = exports.unsqueeze = void 0;
const util_1 = require('../../../util');
const unsqueeze = (inferenceHandler, inputs, axes) => {
  validateInputs(inputs);
  const outputShape = util_1.ShapeUtil.unsqueezeShape(inputs[0].dims, axes);
  const output = inferenceHandler.reshapeUnpacked(inputs[0], outputShape);
  return [output];
};
exports.unsqueeze = unsqueeze;
const unsqueezeV13 = (inferenceHandler, inputs) => {
  validateInputsV13(inputs);
  return (0, exports.unsqueeze)(inferenceHandler, [inputs[0]], Array.from(inputs[1].integerData));
};
exports.unsqueezeV13 = unsqueezeV13;
const parseUnsqueezeAttributes = (node) => node.attributes.getInts('axes');
exports.parseUnsqueezeAttributes = parseUnsqueezeAttributes;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Unsqueeze requires 1 input.');
  }
  if (inputs[0].type === 'string') {
    throw new Error('invalid input tensor types.');
  }
};
const validateInputsV13 = (inputs) => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Unsqueeze requires 2 inputs.');
  }
  if (inputs[1].type !== 'int32') {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=unsqueeze.js.map
