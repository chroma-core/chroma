'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createTextureLayoutFromShape =
  exports.calculateTextureWidthAndHeight =
  exports.createTextureLayoutFromTextureType =
    void 0;
const util_1 = require('../../util');
const types_1 = require('./types');
const createTextureLayoutFromTextureType = (textureLayoutStrategy, shape, textureType) => {
  const channel =
    textureType === types_1.TextureType.unpacked || textureType === types_1.TextureType.unpackedReversed ? 1 : 4;
  const isPacked = textureType === types_1.TextureType.packed;
  const reverseWH = textureType === types_1.TextureType.unpackedReversed || textureType === types_1.TextureType.packed;
  const breakAxis = textureType === types_1.TextureType.packedLastDimension ? shape.length - 1 : undefined;
  const unpackedShape =
    textureType === types_1.TextureType.packedLastDimension
      ? shape.map((d, i) => (i === shape.length - 1 ? d * 4 : d))
      : undefined;
  return (0, exports.createTextureLayoutFromShape)(textureLayoutStrategy, shape, channel, unpackedShape, {
    isPacked,
    reverseWH,
    breakAxis,
  });
};
exports.createTextureLayoutFromTextureType = createTextureLayoutFromTextureType;
const calculateTextureWidthAndHeight = (textureLayoutStrategy, shape, textureType) => {
  const layout = (0, exports.createTextureLayoutFromTextureType)(textureLayoutStrategy, shape, textureType);
  return [layout.width, layout.height];
};
exports.calculateTextureWidthAndHeight = calculateTextureWidthAndHeight;
/**
 * Create a TextureLayout object from shape.
 */
const createTextureLayoutFromShape = (textureLayoutStrategy, shape, channels = 1, unpackedShape, prefs) => {
  const isPacked = !!(prefs && prefs.isPacked);
  const [width, height] = textureLayoutStrategy.computeTextureWH(isPacked ? unpackedShape || shape : shape, prefs);
  const rank = shape.length;
  let inferredDims = shape.slice(0);
  if (rank === 0) {
    inferredDims = [1];
  }
  if (channels === 1) {
    // unpackedShape will take `shape` and not `inferredDims` so as to create a scalar Tensor if need be
    unpackedShape = shape;
  } else if (isPacked) {
    if (channels !== 4) {
      throw new Error('a packed texture must be 4-channel');
    }
    unpackedShape = shape;
    if (rank > 0) {
      inferredDims[rank - 1] = Math.ceil(inferredDims[rank - 1] / 2);
    }
    if (rank > 1) {
      inferredDims[rank - 2] = Math.ceil(inferredDims[rank - 2] / 2);
    }
  } else if (!unpackedShape) {
    throw new Error('Unpacked shape is needed when using channels > 1');
  }
  return {
    width,
    height,
    channels,
    isPacked,
    shape: inferredDims,
    strides: util_1.ShapeUtil.computeStrides(inferredDims),
    unpackedShape,
    reversedWH: prefs && prefs.reverseWH,
  };
};
exports.createTextureLayoutFromShape = createTextureLayoutFromShape;
//# sourceMappingURL=texture-layout.js.map
