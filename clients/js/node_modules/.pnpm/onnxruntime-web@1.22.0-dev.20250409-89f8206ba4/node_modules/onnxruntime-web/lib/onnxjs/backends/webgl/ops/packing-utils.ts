// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { getGlChannels } from '../utils';

export function getVecChannels(name: string, rank: number): string[] {
  return getGlChannels(rank).map((d) => `${name}.${d}`);
}

export function getChannels(name: string, rank: number): string[] {
  if (rank === 1) {
    return [name];
  }
  return getVecChannels(name, rank);
}

export function unpackFromChannel(): string {
  return `
    float getChannel(vec4 frag, int dim) {
      int modCoord = imod(dim, 2);
      return modCoord == 0 ? frag.r : frag.g;
    }

    float getChannel(vec4 frag, vec2 innerDims) {
      vec2 modCoord = mod(innerDims, 2.);
      return modCoord.x == 0. ?
        (modCoord.y == 0. ? frag.r : frag.g) :
        (modCoord.y == 0. ? frag.b : frag.a);
    }
  `;
}
