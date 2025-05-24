'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseSqueezeAttributes = exports.squeezeV13 = exports.squeeze = void 0;
const util_1 = require('../../../util');
const squeeze = (inferenceHandler, inputs, axes) => {
  validateInputs(inputs);
  const outputShape = util_1.ShapeUtil.squeezeShape(inputs[0].dims, axes);
  const output = inferenceHandler.reshapeUnpacked(inputs[0], outputShape);
  return [output];
};
exports.squeeze = squeeze;
const squeezeV13 = (inferenceHandler, inputs) => {
  validateInputsV13(inputs);
  return (0, exports.squeeze)(inferenceHandler, [inputs[0]], Array.from(inputs[1].integerData));
};
exports.squeezeV13 = squeezeV13;
const parseSqueezeAttributes = (node) => node.attributes.getInts('axes');
exports.parseSqueezeAttributes = parseSqueezeAttributes;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Squeeze requires 1 input.');
  }
  if (inputs[0].type === 'string') {
    throw new Error('invalid input tensor types.');
  }
};
const validateInputsV13 = (inputs) => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Squeeze requires 2 inputs.');
  }
  if (inputs[1].type !== 'int32') {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=squeeze.js.map
