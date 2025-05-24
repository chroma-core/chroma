'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createDotProductProgramInfoLoader = void 0;
const util_1 = require('../../../util');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const fuse_utils_1 = require('./fuse-utils');
const im2col_1 = require('./im2col');
const createDotProductProgramMetadata = (hasBias, attributes) => ({
  name: 'ConvDotProduct',
  inputNames: hasBias ? ['Im2Col', 'K', 'B'] : ['Im2Col', 'K'],
  inputTypes: hasBias
    ? [types_1.TextureType.unpacked, types_1.TextureType.packedLastDimension, types_1.TextureType.unpacked]
    : [types_1.TextureType.unpacked, types_1.TextureType.packedLastDimension],
  cacheKey: attributes.activationCacheKey,
});
const createDotProductProgramInfo = (inferenceHandler, metadata, inputs, outputShape, attributes) => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const adjustedKernelShape = [kshape[0], Math.ceil((xshape[1] * kshape[2] * kshape[3]) / 4)];
  const im2colShape = (0, im2col_1.calculateIm2ColDims)(xshape, kshape, outputShape);
  const [kWidth, kHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    adjustedKernelShape,
    types_1.TextureType.packedLastDimension,
  );
  const im2colStrides = util_1.ShapeUtil.computeStrides(im2colShape);
  const [im2colWidth, im2colHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    im2colShape,
    types_1.TextureType.packedLastDimension,
  );
  const rank = outputShape.length;
  const initValue = inputs.length < 3 ? '0.0' : '_B(b)';
  const sharedDim = Math.ceil((xshape[1] * kshape[2] * kshape[3]) / 4);
  const { activationFunction, applyActivation } = (0, fuse_utils_1.getActivationSnippet)(attributes);
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const shaderSource = `
${activationFunction}
float process(int indices[${rank}]) {
  int b[1];
  b[0] = indices[1];
  int im2col[4];
  im2col[0] = indices[0];
  im2col[1] = indices[2];
  im2col[2] = indices[3];
  int im2colOffset = im2col[0] * ${im2colStrides[0]} + im2col[1] * ${im2colStrides[1]} + im2col[2] * ${im2colStrides[2]};
  int kernelOffset = indices[1] * ${adjustedKernelShape[1]};
  float value = ${initValue};
  for (int i = 0; i < ${sharedDim}; ++i) {
    vec2 im2colCoords = offsetToCoords(im2colOffset, ${im2colWidth}, ${im2colHeight});
    vec2 kernelCoords = offsetToCoords(kernelOffset, ${kWidth}, ${kHeight});
    value += dot(${glsl.texture2D}(Im2Col, im2colCoords), ${glsl.texture2D}(K, kernelCoords));
    ++im2colOffset;
    ++kernelOffset;
  }
  ${applyActivation}
  return value;
}`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const createDotProductProgramInfoLoader = (inferenceHandler, inputs, outputShape, attributes) => {
  const metadata = createDotProductProgramMetadata(inputs.length > 2, attributes);
  return {
    ...metadata,
    get: () => createDotProductProgramInfo(inferenceHandler, metadata, inputs, outputShape, attributes),
  };
};
exports.createDotProductProgramInfoLoader = createDotProductProgramInfoLoader;
//# sourceMappingURL=dot-product.js.map
