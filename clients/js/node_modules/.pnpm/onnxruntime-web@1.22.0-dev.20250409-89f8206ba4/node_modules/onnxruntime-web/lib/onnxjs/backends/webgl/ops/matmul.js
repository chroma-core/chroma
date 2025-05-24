'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.getBiasForMatmul =
  exports.createMatmulProgramInfoLoader =
  exports.parseMatMulAttributes =
  exports.matMul =
    void 0;
const util_1 = require('../../../util');
const types_1 = require('../types');
const utils_1 = require('../utils');
const fuse_utils_1 = require('./fuse-utils');
const matmul_pack_1 = require('./matmul-pack');
const matMul = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  if (inferenceHandler.session.pack) {
    return [
      inferenceHandler.run(
        (0, matmul_pack_1.createPackedMatmulProgramInfoLoader)(inferenceHandler, inputs, attributes),
        inputs,
      ),
    ];
  } else {
    return [inferenceHandler.run(createMatmulProgramInfoLoader(inputs, attributes), inputs)];
  }
};
exports.matMul = matMul;
const parseMatMulAttributes = (node) => (0, fuse_utils_1.parseInternalActivationAttributes)(node.attributes);
exports.parseMatMulAttributes = parseMatMulAttributes;
const createMatmulProgramMetadata = (hasBias, cacheHint) => ({
  name: 'MatMul',
  inputNames: hasBias ? ['A', 'B', 'Bias'] : ['A', 'B'],
  inputTypes: hasBias
    ? [types_1.TextureType.unpacked, types_1.TextureType.unpacked, types_1.TextureType.unpacked]
    : [types_1.TextureType.unpacked, types_1.TextureType.unpacked],
  cacheHint,
});
function createMatmulProgramInfo(metadata, inputs, activationAttributes) {
  const aShape = inputs[0].dims;
  const bShape = inputs[1].dims;
  const outputShape = util_1.BroadcastUtil.calcShape(aShape, bShape, true);
  if (!outputShape) {
    throw new Error("Can't use matmul on the given tensors");
  }
  const coordsDataType = (0, utils_1.getCoordsDataType)(outputShape.length);
  const allGlChannels = (0, utils_1.getGlChannels)();
  const { activationFunction, applyActivation } = (0, fuse_utils_1.getActivationSnippet)(activationAttributes);
  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += getBiasForMatmul();' : '';
  const getBiasForMatmulSnippet = hasBias
    ? `${getBiasForMatmul(coordsDataType, allGlChannels, inputs[2].dims, outputShape, false)}`
    : '';
  const rank = outputShape.length;
  const arank = aShape.length;
  const brank = bShape.length;
  const sharedDim = aShape[aShape.length - 1];
  const shaderSource = `
    ${activationFunction}
    ${getBiasForMatmulSnippet}
    float process(int indices[${rank}]) {
        int a[${arank}];
        int b[${brank}];
        bcastMatmulIndices_A(indices, a);
        bcastMatmulIndices_B(indices, b);

        float value;
        for (int k=0; k<${sharedDim}; ++k) {
            a[${arank - 1}] = k;
            b[${brank - 2}] = k;
            value += _A(a) * _B(b);
        }
        ${processBias}
        ${applyActivation}
        return value;
    }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
}
function createMatmulProgramInfoLoader(inputs, activationAttributes) {
  const metadata = createMatmulProgramMetadata(inputs.length > 2, activationAttributes.activationCacheKey);
  return { ...metadata, get: () => createMatmulProgramInfo(metadata, inputs, activationAttributes) };
}
exports.createMatmulProgramInfoLoader = createMatmulProgramInfoLoader;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('MatMul requires 2 inputs.');
  }
  if (inputs[0].dims[inputs[0].dims.length - 1] !== inputs[1].dims[inputs[1].dims.length - 2]) {
    throw new Error('shared dimension does not match.');
  }
  if (
    (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') ||
    (inputs[1].type !== 'float32' && inputs[1].type !== 'float64')
  ) {
    throw new Error('inputs should be float type');
  }
  if (inputs[0].type !== inputs[1].type) {
    throw new Error('inputs types should match');
  }
};
function getBiasForMatmul(coordsDataType, allGlChannels, inShape, outShape, isPacked) {
  let unpackedCoordsSnippet = '';
  const inRank = inShape.length;
  const outRank = outShape.length;
  const rankDiff = outRank - inRank;
  if (outRank < 2 && inRank > 0) {
    unpackedCoordsSnippet = 'coords';
  } else {
    unpackedCoordsSnippet = inShape.map((_s, i) => `coords.${allGlChannels[i + rankDiff]}`).join(', ');
  }
  const broadcastDims = util_1.BroadcastUtil.getBroadcastDims(inShape, outShape);
  const coordsSnippet = broadcastDims.map((d) => `coords.${allGlChannels[d + rankDiff]} = 0;`).join('\n');
  const inSize = util_1.ShapeUtil.size(inShape);
  const isInputScalar = inSize === 1;
  let output = 'vec4(outputValue.xx, outputValue.yy)';
  if (isInputScalar) {
    output = 'vec4(outputValue.x)';
  }
  const getBiasForMatmulSource = isPacked
    ? `
vec4 getBiasForMatmul() {
  ${coordsDataType} coords = getOutputCoords();
  ${coordsSnippet}
  vec4 outputValue = getBias(${unpackedCoordsSnippet});
  return ${output};
}`
    : `
float getBiasForMatmul() {
  ${coordsDataType} coords = getOutputCoords();
  ${coordsSnippet}
  return getBias(coords.x);
}`;
  return getBiasForMatmulSource;
}
exports.getBiasForMatmul = getBiasForMatmul;
//# sourceMappingURL=matmul.js.map
