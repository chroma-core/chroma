'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseBatchNormalizationAttributes = exports.batchNormalization = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const batchNormalizationProgramMetadata = {
  name: 'BatchNormalization',
  inputNames: ['A', 'Scale', 'B', 'Mean', 'Variance'],
  inputTypes: [
    types_1.TextureType.unpacked,
    types_1.TextureType.unpacked,
    types_1.TextureType.unpacked,
    types_1.TextureType.unpacked,
    types_1.TextureType.unpacked,
  ],
};
const batchNormalization = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const output = inferenceHandler.run(
    {
      ...batchNormalizationProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createBatchNormalizationProgramInfo(inferenceHandler, inputs, attributes),
    },
    inputs,
  );
  return [output];
};
exports.batchNormalization = batchNormalization;
const parseBatchNormalizationAttributes = (node) => {
  const epsilon = node.attributes.getFloat('epsilon', 1e-5);
  const momentum = node.attributes.getFloat('momentum', 0.9);
  const spatial = node.attributes.getInt('spatial', 1);
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ epsilon, momentum, spatial });
};
exports.parseBatchNormalizationAttributes = parseBatchNormalizationAttributes;
const createBatchNormalizationProgramInfo = (inferenceHandler, inputs, attributes) => {
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const rank = inputs[0].dims.length;
  const [scaleWidth, scaleHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    inputs[1].dims,
    types_1.TextureType.unpacked,
  );
  const shaderSource = `
  float process(int[${rank}] indices) {
    vec2 position = offsetToCoords(indices[1], ${scaleWidth}, ${scaleHeight});
    float scale = getColorAsFloat(${glsl.texture2D}(Scale, position));
    float mean = getColorAsFloat(${glsl.texture2D}(Mean, position));
    float variance = getColorAsFloat(${glsl.texture2D}(Variance, position));
    float b = getColorAsFloat(${glsl.texture2D}(B, position));

    return scale * ( (_A(indices) - mean) / sqrt(variance + float(${attributes.epsilon})) ) + b;
  }`;
  return {
    ...batchNormalizationProgramMetadata,
    output: { dims: inputs[0].dims, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 5) {
    throw new Error('BatchNormalization requires 5 inputs.');
  }
  const X = inputs[0];
  const scale = inputs[1];
  const B = inputs[2];
  const mean = inputs[3];
  const var_ = inputs[4];
  // input should atleast have three dimensions - N,C,dim1,...,dimn
  // other inputs can have only one dimensions
  if (
    X.dims.length < 3 ||
    scale.dims.length !== 1 ||
    B.dims.length !== 1 ||
    mean.dims.length !== 1 ||
    var_.dims.length !== 1
  ) {
    throw new Error('invalid input shape.');
  }
  if (
    scale.dims[0] !== X.dims[1] ||
    B.dims[0] !== X.dims[1] ||
    mean.dims[0] !== X.dims[1] ||
    var_.dims[0] !== X.dims[1]
  ) {
    throw new Error('invalid input shape.');
  }
  if (
    (X.type !== 'float32' && X.type !== 'float64') ||
    (scale.type !== 'float32' && scale.type !== 'float64') ||
    (B.type !== 'float32' && B.type !== 'float64') ||
    (mean.type !== 'float32' && mean.type !== 'float64') ||
    (var_.type !== 'float32' && var_.type !== 'float64')
  ) {
    throw new Error('invalid input tensor types.');
  }
};
//# sourceMappingURL=batch-normalization.js.map
