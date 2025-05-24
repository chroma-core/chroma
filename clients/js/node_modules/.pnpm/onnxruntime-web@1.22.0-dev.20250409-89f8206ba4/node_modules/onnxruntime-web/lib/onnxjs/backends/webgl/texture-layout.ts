// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { ShapeUtil } from '../../util';

import { TextureLayoutStrategy, WidthHeightPrefs } from './texture-layout-strategy';
import { TextureLayout, TextureType } from './types';

export const createTextureLayoutFromTextureType = (
  textureLayoutStrategy: TextureLayoutStrategy,
  shape: readonly number[],
  textureType: TextureType,
): TextureLayout => {
  const channel = textureType === TextureType.unpacked || textureType === TextureType.unpackedReversed ? 1 : 4;
  const isPacked = textureType === TextureType.packed;
  const reverseWH = textureType === TextureType.unpackedReversed || textureType === TextureType.packed;
  const breakAxis = textureType === TextureType.packedLastDimension ? shape.length - 1 : undefined;
  const unpackedShape =
    textureType === TextureType.packedLastDimension
      ? shape.map((d, i) => (i === shape.length - 1 ? d * 4 : d))
      : undefined;
  return createTextureLayoutFromShape(textureLayoutStrategy, shape, channel, unpackedShape, {
    isPacked,
    reverseWH,
    breakAxis,
  });
};

export const calculateTextureWidthAndHeight = (
  textureLayoutStrategy: TextureLayoutStrategy,
  shape: readonly number[],
  textureType: TextureType,
): [number, number] => {
  const layout = createTextureLayoutFromTextureType(textureLayoutStrategy, shape, textureType);
  return [layout.width, layout.height];
};

/**
 * Create a TextureLayout object from shape.
 */
export const createTextureLayoutFromShape = (
  textureLayoutStrategy: TextureLayoutStrategy,
  shape: readonly number[],
  channels: 1 | 4 = 1,
  unpackedShape?: readonly number[],
  prefs?: WidthHeightPrefs,
): TextureLayout => {
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
    strides: ShapeUtil.computeStrides(inferredDims),
    unpackedShape,
    reversedWH: prefs && prefs.reverseWH,
  };
};
