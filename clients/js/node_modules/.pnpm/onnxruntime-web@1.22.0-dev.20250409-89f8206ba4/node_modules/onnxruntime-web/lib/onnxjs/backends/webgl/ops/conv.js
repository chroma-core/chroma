'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseConvAttributes = exports.conv = exports.calculateOutputShape = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const conv_grouped_1 = require('./conv-grouped');
const conv_pack_1 = require('./conv-pack');
const dot_product_1 = require('./dot-product');
const fuse_utils_1 = require('./fuse-utils');
const im2col_1 = require('./im2col');
const matmul_1 = require('./matmul');
const calculateOutputShape = (inputShape, kernelShape, dilations, adjustPads, strides) => {
  const batchSize = inputShape[0];
  const inputSpatialShape = inputShape.slice(2);
  const spatialRank = inputSpatialShape.length;
  const outChannels = kernelShape[0];
  const kernelSpatialShape = kernelShape.slice(2);
  const dilatedKernelShape = kernelSpatialShape.map((v, i) => v + (v - 1) * (dilations[i] - 1));
  const inputSpatialShapeWithPad = inputSpatialShape.map((v, i) => v + adjustPads[i] + adjustPads[i + spatialRank]);
  const outputSpatialShape = inputSpatialShapeWithPad.map((v, i) =>
    Math.floor((v - dilatedKernelShape[i] + strides[i]) / strides[i]),
  );
  const outputShape = [batchSize, outChannels].concat(...outputSpatialShape);
  return outputShape;
};
exports.calculateOutputShape = calculateOutputShape;
const conv = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs, attributes); // currently will fail if not conv2D
  return conv2d(inferenceHandler, inputs, attributes);
};
exports.conv = conv;
const conv2d = (inferenceHandler, inputs, attributes) => {
  const adjustedAttributes = getAdjustedConvAttributes(attributes, inputs);
  const packMode = inferenceHandler.session.pack;
  const isPointwise = adjustedAttributes.kernelShape[0] === 1 && adjustedAttributes.kernelShape[1] === 1;
  if (adjustedAttributes.group > 1) {
    const result = inferenceHandler.run(
      (0, conv_grouped_1.createUnpackedGroupedConvProgramInfoLoader)(inferenceHandler, inputs, adjustedAttributes),
      inputs,
    );
    return [result];
  } else if (isPointwise && packMode) {
    return [conv2DUnpackedPointwise(inferenceHandler, inputs, adjustedAttributes)];
  } else if (packMode && inputs[0].dims.length === 4 && inputs[0].dims[0] === 1 && !isPointwise) {
    return [(0, conv_pack_1.conv2DPacked)(inferenceHandler, inputs, adjustedAttributes)];
  } else {
    return [conv2DUnpacked(inferenceHandler, inputs, adjustedAttributes)];
  }
};
const conv2DUnpackedPointwise = (inferenceHandler, inputs, attributes) => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const outputShape = (0, exports.calculateOutputShape)(
    xshape,
    kshape,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
  );
  const reshapedX = inferenceHandler.reshapeUnpacked(inputs[0], [xshape[1], xshape[2] * xshape[3]]);
  const reshapedK = inferenceHandler.reshapeUnpacked(inputs[1], [kshape[0], kshape[1]]);
  const matmulInputs = inputs.length > 2 ? [reshapedK, reshapedX, inputs[2]] : [reshapedK, reshapedX];
  const matmulOutput = inferenceHandler.run(
    (0, matmul_1.createMatmulProgramInfoLoader)(matmulInputs, attributes),
    matmulInputs,
  );
  return inferenceHandler.reshapeUnpacked(matmulOutput, outputShape);
};
const conv2DUnpacked = (inferenceHandler, inputs, attributes) => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const outputShape = (0, exports.calculateOutputShape)(
    xshape,
    kshape,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
  );
  const xIm2Col = inferenceHandler.run(
    (0, im2col_1.createIm2ColProgramInfoLoader)(inferenceHandler, inputs[0], inputs[1], outputShape, attributes),
    [inputs[0]],
  );
  const dotProductInputs = inputs.length === 3 ? [xIm2Col, inputs[1], inputs[2]] : [xIm2Col, inputs[1]];
  const output = inferenceHandler.run(
    (0, dot_product_1.createDotProductProgramInfoLoader)(inferenceHandler, inputs, outputShape, attributes),
    dotProductInputs,
  );
  return output;
};
const getAdjustedConvAttributes = (attributes, inputs) => {
  const kernelShape = attributes.kernelShape.slice();
  // if kernelShape is not specified in the attributes of this op, infer it from the weight tensor dims
  if (attributes.kernelShape.length === 0) {
    for (let i = 2; i < inputs[1].dims.length; ++i) {
      kernelShape.push(inputs[1].dims[i]);
    }
  }
  const pads = attributes.pads.slice();
  util_1.PoolConvUtil.adjustPadsBasedOnAutoPad(
    inputs[0].dims,
    attributes.strides,
    attributes.dilations,
    kernelShape,
    pads,
    attributes.autoPad,
  );
  // always return a new object so does not modify the original attributes
  const newAttributes = Object.assign({}, attributes);
  Object.assign(newAttributes, { kernelShape, pads, cacheKey: attributes.cacheKey });
  return newAttributes;
};
const parseConvAttributes = (node) => {
  const attributes = node.attributes;
  const activationAttributes = (0, fuse_utils_1.parseInternalActivationAttributes)(attributes);
  // TODO : Make this generic enough to compute default attributes for multi-dimensional conv
  const autoPad = attributes.getString('auto_pad', 'NOTSET');
  const dilations = attributes.getInts('dilations', [1, 1]);
  const group = attributes.getInt('group', 1);
  const kernelShape = attributes.getInts('kernel_shape', []);
  const pads = attributes.getInts('pads', [0, 0, 0, 0]);
  const strides = attributes.getInts('strides', [1, 1]);
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({
    autoPad,
    dilations,
    group,
    kernelShape,
    pads,
    strides,
    ...activationAttributes,
  });
};
exports.parseConvAttributes = parseConvAttributes;
const validateInputs = (inputs, attributes) => {
  // Refer to the below link for all input checks
  // https://github.com/onnx/onnx/blob/main/docs/Operators.md#Conv
  if (!inputs || (inputs.length !== 2 && inputs.length !== 3)) {
    throw new Error('Conv requires 2 or 3 inputs');
  }
  // TODO : Need to add support for multi-dimensional conv
  if (inputs[0].dims.length !== 4 || inputs[1].dims.length !== 4) {
    throw new Error('currently only support 2-dimensional conv');
  }
  // FILTER_IN_CHANNEL should be equal to DATA_CHANNEL
  const dataChannel = inputs[0].dims[1];
  const filterInChannel = inputs[1].dims[1] * attributes.group;
  if (dataChannel !== filterInChannel) {
    throw new Error('FILTER_IN_CHANNEL should be equal to DATA_CHANNEL');
  }
  // if bias is provided it should be 1D and the number of elements should be equal to the number of feature maps
  if (inputs.length === 3 && (inputs[2].dims.length !== 1 || inputs[1].dims[0] !== inputs[2].dims[0])) {
    throw new Error('invalid bias');
  }
  const spatialRank = inputs[0].dims.length - 2;
  // wrong dilations dimension
  if (attributes.dilations.length !== spatialRank) {
    throw new Error(`dilations should be ${spatialRank}D`);
  }
  // Wrong strides dimension
  if (attributes.strides.length !== spatialRank) {
    throw new Error(`strides should be ${spatialRank}D`);
  }
  // Wrong pads dimension
  if (attributes.pads.length !== spatialRank * 2) {
    throw new Error(`pads should be ${spatialRank * 2}D`);
  }
  // if kernelShape is specified, it's data length must be 2 less than dims length of the weights tensor
  // (the first 2 dims are batch_size and channels)
  if (attributes.kernelShape.length !== 0 && attributes.kernelShape.length !== inputs[1].dims.length - 2) {
    throw new Error('invalid kernel shape');
  }
  // TODO : Need to add support for float64
  if (inputs[0].type !== 'float32' || inputs[1].type !== 'float32') {
    throw new Error('Conv input(X,W) should be float tensor');
  }
  if (inputs.length === 3 && inputs[2].type !== 'float32') {
    throw new Error('Conv input(bias) should be float tensor');
  }
};
//# sourceMappingURL=conv.js.map
