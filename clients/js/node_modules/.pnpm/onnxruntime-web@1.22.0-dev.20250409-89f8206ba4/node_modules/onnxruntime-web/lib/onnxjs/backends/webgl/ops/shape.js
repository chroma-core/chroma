'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.shape = void 0;
const tensor_1 = require('../../../tensor');
const shape = (_inferenceHandler, inputs) => {
  validateInputs(inputs);
  return [new tensor_1.Tensor([inputs[0].dims.length], 'int32', undefined, undefined, new Int32Array(inputs[0].dims))];
};
exports.shape = shape;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Shape requires 1 input.');
  }
};
//# sourceMappingURL=shape.js.map
