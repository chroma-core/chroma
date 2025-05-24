'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createUnpackProgramInfoLoader = exports.createUnpackProgramInfo = void 0;
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const utils_1 = require('../utils');
const packing_utils_1 = require('./packing-utils');
const unpackProgramMetadata = {
  name: 'unpack',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.packed],
};
const createUnpackProgramInfo = (handler, input) => {
  const rank = input.dims.length;
  const channels = (0, packing_utils_1.getChannels)('rc', rank);
  const innerDims = channels.slice(-2);
  const coordsDataType = (0, utils_1.getCoordsDataType)(rank);
  const unpackChannel = (0, packing_utils_1.unpackFromChannel)();
  const isScalar = input.dims.length === 0;
  const sourceCoords = isScalar ? '' : getSourceCoords(rank, channels);
  const coords = rank <= 1 ? 'rc' : `vec2(${innerDims.join(',')})`;
  const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
  const shaderSource = `
    ${unpackChannel}
    void main() {
      ${coordsDataType} rc = getOutputCoords();

       // Sample the texture with the coords to get the rgba channel value.
       vec4 packedInput = getA(${sourceCoords});

       ${glsl.output} = vec4(getChannel(packedInput, ${coords}), 0, 0, 0);
     }
   `;
  return {
    ...unpackProgramMetadata,
    hasMain: true,
    output: { dims: input.dims, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
exports.createUnpackProgramInfo = createUnpackProgramInfo;
const createUnpackProgramInfoLoader = (handler, input) => ({
  ...unpackProgramMetadata,
  get: () => (0, exports.createUnpackProgramInfo)(handler, input),
});
exports.createUnpackProgramInfoLoader = createUnpackProgramInfoLoader;
function getSourceCoords(rank, dims) {
  if (rank === 1) {
    return 'rc';
  }
  let coords = '';
  for (let i = 0; i < rank; i++) {
    coords += dims[i];
    if (i < rank - 1) {
      coords += ',';
    }
  }
  return coords;
}
//# sourceMappingURL=unpack.js.map
