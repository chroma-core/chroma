'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseCastAttributes = exports.cast = void 0;
const util_1 = require('../../../util');
const cast = (handler, inputs, to) => {
  validateInputs(inputs);
  return [handler.cast(inputs[0], to)];
};
exports.cast = cast;
const parseCastAttributes = (node) => util_1.ProtoUtil.tensorDataTypeFromProto(node.attributes.getInt('to'));
exports.parseCastAttributes = parseCastAttributes;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Cast requires 1 input.');
  }
  if (inputs[0].type === 'string') {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=cast.js.map
