// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { assert } from '../../util';
/**
 * Given a non RGBA shape calculate the R version
 * It is assumed that the dimensions are multiples of given channels
 * NOTE: it is always the last dim that gets packed.
 * @param unpackedShape original shape to create a packed version from
 */
export function getPackedShape(unpackedShape: readonly number[]): readonly number[] {
  const len = unpackedShape.length;
  return unpackedShape.slice(0, len - 1).concat(unpackedShape[len - 1] / 4);
}

export async function repeatedTry(
  checkFn: () => boolean,
  delayFn = (_counter: number) => 0,
  maxCounter?: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let tryCount = 0;

    const tryFn = () => {
      if (checkFn()) {
        resolve();
        return;
      }

      tryCount++;

      const nextBackoff = delayFn(tryCount);

      if (maxCounter != null && tryCount >= maxCounter) {
        reject();
        return;
      }
      setTimeout(tryFn, nextBackoff);
    };

    tryFn();
  });
}

/**
 * Generates the function name from an input sampler name.
 * @param samplerName Name of the sampler.
 */
export function generateShaderFuncNameFromInputSamplerName(samplerName: string): string {
  assert(typeof samplerName !== 'undefined' && samplerName.length !== 0, () => 'empty string found for sampler name');
  return 'get' + samplerName.charAt(0).toUpperCase() + samplerName.slice(1);
}

/**
 * Generates the function name from an input sampler name at output coordinates.
 * @param samplerName Name of the sampler.
 */
export function generateShaderFuncNameFromInputSamplerNameAtOutCoords(samplerName: string): string {
  assert(typeof samplerName !== 'undefined' && samplerName.length !== 0, () => 'empty string found for sampler name');
  return 'get' + samplerName.charAt(0).toUpperCase() + samplerName.slice(1) + 'AtOutCoords';
}

/** Returns a new input shape (a copy) that has a squeezed logical shape. */
export function squeezeInputShape(inputShape: readonly number[], squeezedShape: number[]): number[] {
  // Deep copy.
  let newInputShape: number[] = JSON.parse(JSON.stringify(inputShape));
  newInputShape = squeezedShape;
  return newInputShape;
}

/** Returns a list of squeezed parameters for shader functions */
export function getSqueezedParams(params: string[], keptDims: number[]): string {
  return keptDims.map((d) => params[d]).join(', ');
}

/** Returns the data type for different ranks. */
export function getCoordsDataType(rank: number): string {
  if (rank <= 1) {
    return 'int';
  } else if (rank === 2) {
    return 'ivec2';
  } else if (rank === 3) {
    return 'ivec3';
  } else if (rank === 4) {
    return 'ivec4';
  } else if (rank === 5) {
    return 'ivec5';
  } else if (rank === 6) {
    return 'ivec6';
  } else {
    throw Error(`GPU for rank ${rank} is not yet supported`);
  }
}

export function getGlChannels(rank = 6): string[] {
  return ['x', 'y', 'z', 'w', 'u', 'v'].slice(0, rank);
}
