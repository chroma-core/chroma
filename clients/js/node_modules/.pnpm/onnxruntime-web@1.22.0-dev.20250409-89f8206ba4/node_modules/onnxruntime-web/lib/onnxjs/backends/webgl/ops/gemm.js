'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseGemmAttributesV11 = exports.parseGemmAttributesV7 = exports.gemm = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const types_1 = require('../types');
const gemm = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs, attributes);
  const output = inferenceHandler.run(createGemmProgramInfoLoader(inputs, attributes), inputs);
  return [output];
};
exports.gemm = gemm;
const parseGemmAttributes = (node, isOptionalC) => {
  const transA = node.attributes.getInt('transA', 0) !== 0;
  const transB = node.attributes.getInt('transB', 0) !== 0;
  const alpha = node.attributes.getFloat('alpha', 1.0);
  const beta = node.attributes.getFloat('beta', 1.0);
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ transA, transB, alpha, beta, isOptionalC });
};
const parseGemmAttributesV7 = (node) => parseGemmAttributes(node, false);
exports.parseGemmAttributesV7 = parseGemmAttributesV7;
const parseGemmAttributesV11 = (node) => parseGemmAttributes(node, true);
exports.parseGemmAttributesV11 = parseGemmAttributesV11;
const createGemmProgramInfoLoader = (inputs, attributes) => {
  const metadata = {
    name: 'Gemm',
    inputNames: inputs.length === 3 ? ['A', 'B', 'C'] : ['A', 'B'],
    inputTypes:
      inputs.length === 3
        ? [types_1.TextureType.unpacked, types_1.TextureType.unpacked, types_1.TextureType.unpacked]
        : [types_1.TextureType.unpacked, types_1.TextureType.unpacked],
    key: attributes.cacheKey,
  };
  return { ...metadata, get: () => createGemmProgramInfo(metadata, inputs, attributes) };
};
const createGemmProgramInfo = (metadata, inputs, attributes) => {
  const aShape = inputs[0].dims.slice();
  const bShape = inputs[1].dims.slice();
  const [M, N] = util_1.GemmUtil.getShapeOfGemmResult(
    aShape,
    attributes.transA,
    bShape,
    attributes.transB,
    inputs.length === 3 ? inputs[2].dims : undefined,
  );
  const outputShape = [M, N];
  if (!outputShape) {
    throw new Error("Can't use gemm on the given tensors");
  }
  let sharedDim = aShape[aShape.length - 1];
  let line = '';
  if (attributes.transA) {
    sharedDim = aShape[0];
  }
  if (attributes.transA && attributes.transB) {
    line = 'value += _A_T(a) * _B_T(b);';
  } else if (attributes.transA && !attributes.transB) {
    line = 'value += _A_T(a) * _B(b);';
  } else if (!attributes.transA && attributes.transB) {
    line = 'value += _A(a) * _B_T(b);';
  } else if (!attributes.transA && !attributes.transB) {
    line = 'value += _A(a) * _B(b);';
  }
  const rank = outputShape.length;
  const declareC = inputs.length === 3 ? `int c[${inputs[2].dims.length}];` : '';
  const broadcastC = inputs.length === 3 ? 'bcastIndices_C(indices, c);' : '';
  const calculateC = inputs.length === 3 ? 'value += beta * _C(c);' : '';
  const shaderSource = `
      float process(int indices[${rank}]) {
          int a[${rank}];
          int b[${rank}];
          ${declareC}

          copyVec(indices, a);
          copyVec(indices, b);
          ${broadcastC}

          float value = 0.0;
          for (int k=0; k<${sharedDim}; ++k) {
              a[${rank - 1}] = k;
              b[${rank - 2}] = k;
              ${line}
          }

          value = value * alpha;
          ${calculateC}
          return value;
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    variables: [
      { name: 'alpha', type: 'float', data: attributes.alpha },
      { name: 'beta', type: 'float', data: attributes.beta },
    ],
    shaderSource,
  };
};
const validateInputs = (inputs, attributes) => {
  if (!inputs) {
    throw new Error('Input is missing');
  }
  if (attributes.isOptionalC && (inputs.length < 2 || inputs.length > 3)) {
    throw new Error('Invaid input shape.');
  }
  if (!attributes.isOptionalC && inputs.length !== 3) {
    throw new Error('Gemm requires 3 inputs');
  }
  // 'C' can be of dimensionality 1 or 2 only
  if (inputs.length === 3 && inputs[2].dims.length !== 1 && inputs[2].dims.length !== 2) {
    throw new Error('Invalid input shape of C');
  }
  if (
    (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') ||
    (inputs[1].type !== 'float32' && inputs[1].type !== 'float64') ||
    (inputs.length === 3 && inputs[2].type !== 'float32' && inputs[2].type !== 'float64')
  ) {
    throw new Error('Invalid input type.');
  }
  if (inputs[0].type !== inputs[1].type || (inputs.length === 3 && inputs[0].type !== inputs[2].type)) {
    throw new Error('Input types are mismatched');
  }
};
//# sourceMappingURL=gemm.js.map
