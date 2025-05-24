'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.unpackFromChannel = exports.getChannels = exports.getVecChannels = void 0;
const utils_1 = require('../utils');
function getVecChannels(name, rank) {
  return (0, utils_1.getGlChannels)(rank).map((d) => `${name}.${d}`);
}
exports.getVecChannels = getVecChannels;
function getChannels(name, rank) {
  if (rank === 1) {
    return [name];
  }
  return getVecChannels(name, rank);
}
exports.getChannels = getChannels;
function unpackFromChannel() {
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
exports.unpackFromChannel = unpackFromChannel;
//# sourceMappingURL=packing-utils.js.map
