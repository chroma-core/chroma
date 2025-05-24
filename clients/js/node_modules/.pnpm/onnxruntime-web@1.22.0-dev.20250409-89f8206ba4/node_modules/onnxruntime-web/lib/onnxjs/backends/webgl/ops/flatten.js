'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseFlattenAttributes = exports.flatten = void 0;
const util_1 = require('../../../util');
const flatten = (inferenceHandler, inputs, axis) => {
  validateInputs(inputs, axis);
  const outputDims = util_1.ShapeUtil.flattenShape(inputs[0].dims, axis);
  return [inferenceHandler.reshapeUnpacked(inputs[0], outputDims)];
};
exports.flatten = flatten;
const parseFlattenAttributes = (node) => node.attributes.getInt('axis', 1); // default axis is 1
exports.parseFlattenAttributes = parseFlattenAttributes;
const validateInputs = (inputs, axis) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Flatten requires 1 input.');
  }
  const r = inputs[0].dims.length;
  if (r === 0) {
    throw new Error('scalar tensor is not supported.');
  }
  if (axis < -r || axis > r) {
    throw new Error('Invalid axis');
  }
  // TODO: Support string type
  if (inputs[0].type === 'string') {
    throw new Error('string tensor is not supported.');
  }
};
//# sourceMappingURL=flatten.js.map
