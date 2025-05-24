'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseInternalActivationAttributes = exports.getActivationSnippet = void 0;
const util_1 = require('../../../util');
const unary_op_1 = require('./unary-op');
function getActivationSnippet(attributes) {
  let func;
  switch (attributes.activation) {
    case 'Relu':
      func = (0, unary_op_1.glslRelu)();
      break;
    case 'Sigmoid':
      func = (0, unary_op_1.glslSigmoid)();
      break;
    case 'Clip':
      func = (0, unary_op_1.glslClip)(attributes.clipMin, attributes.clipMax);
      break;
    // TODO: adding other activations that can be fused.
    default:
      return { activationFunction: '', applyActivation: '' };
  }
  const activationName = func.name;
  const activationFunction = func.body;
  const applyActivation = `value = ${activationName}_(value);`;
  return { activationFunction, applyActivation };
}
exports.getActivationSnippet = getActivationSnippet;
const parseInternalActivationAttributes = (attributes) => {
  const activation = attributes.getString('activation', '');
  if (activation === 'Clip') {
    const [clipMin, clipMax] = attributes.getFloats('activation_params', [util_1.MIN_CLIP, util_1.MAX_CLIP]);
    return { activation, clipMax, clipMin, activationCacheKey: `${activation}:${clipMin},${clipMax}` };
  }
  return { activation, activationCacheKey: activation };
};
exports.parseInternalActivationAttributes = parseInternalActivationAttributes;
//# sourceMappingURL=fuse-utils.js.map
