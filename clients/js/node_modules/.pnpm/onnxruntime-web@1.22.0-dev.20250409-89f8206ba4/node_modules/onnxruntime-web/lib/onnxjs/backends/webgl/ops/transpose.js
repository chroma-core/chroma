'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseTransposeAttributes = exports.transpose = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const types_1 = require('../types');
const transposeProgramMetadata = {
  name: 'Transpose',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpacked],
};
const transpose = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const output = inferenceHandler.run(
    {
      ...transposeProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createTransposeProgramInfo(inferenceHandler, inputs[0], attributes.perm),
    },
    inputs,
  );
  return [output];
};
exports.transpose = transpose;
const parseTransposeAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ perm: node.attributes.getInts('perm', []) });
exports.parseTransposeAttributes = parseTransposeAttributes;
const createTransposeProgramInfo = (_inferenceHandler, input, perm) => {
  const inputShape = input.dims;
  perm = getAdjustedPerm(inputShape, perm);
  const unpackedOutputShape = getOutputShape(inputShape, perm);
  const rank = inputShape.length;
  // A dims=[${inputs[0].dims.toString()}]
  // out Dims=[${unpackedOutputShape.toString()}]
  // based on perm=[${perm.toString()}]
  const shaderSource = `
      ${getPermFunctionBody('perm', perm, rank)}
      float process(int indices[${rank}]) {
        int a[${rank}];
        perm(a, indices);
        return _A(a);
      }`;
  return {
    ...transposeProgramMetadata,
    output: { dims: unpackedOutputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const getAdjustedPerm = (inputShape, perm) => {
  if (perm && perm.length !== inputShape.length) {
    perm = [...inputShape.keys()].reverse();
  }
  return perm;
};
const getOutputShape = (inputShape, perm) => {
  perm = getAdjustedPerm(inputShape, perm);
  return util_1.ShapeUtil.sortBasedOnPerm(inputShape, perm);
};
const getPermFunctionBody = (name, perm, rank) => {
  const reverseFunc = [];
  reverseFunc.push(`void ${name}(out int a[${rank}], int src[${rank}]) {`);
  for (let i = 0; i < rank; ++i) {
    reverseFunc.push(`\ta[${perm[i]}]=src[${i}];`);
  }
  reverseFunc.push('\t}');
  return reverseFunc.join('\n');
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Transpose requires 1 input.');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('input should be float tensor');
  }
};
//# sourceMappingURL=transpose.js.map
