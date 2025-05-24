'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createPackProgramInfoLoader = void 0;
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const utils_1 = require('../utils');
const packing_utils_1 = require('./packing-utils');
const packProgramMetadata = {
  name: 'pack',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpackedReversed],
};
const createPackProgramInfo = (handler, input) => {
  const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
  const inputShape = input.dims;
  const inputRank = inputShape.length;
  // createTextureLayoutFromShape won't change output rank. Need to verify by running tests
  const outputRank = input.dims.length;
  const coordsDataType = (0, utils_1.getCoordsDataType)(outputRank);
  const channels = (0, packing_utils_1.getChannels)('rc', outputRank);
  const setup = getSetup(outputRank, channels, inputShape[inputShape.length - 2], inputShape[inputShape.length - 1]);
  let reversedInputWH;
  if (inputRank === 0) {
    reversedInputWH = [1, 1];
  } else if (inputRank === 1) {
    reversedInputWH = [inputShape[0], 1];
  } else {
    reversedInputWH = [inputShape[outputRank - 1], inputShape[outputRank - 2]];
  }
  const outOfBoundsCondition = getOutOfBoundsCondition(outputRank, reversedInputWH, channels);
  const output = getOutput(inputShape, channels);
  const shaderSource = `
        void main() {
          ${coordsDataType} rc = getOutputCoords();

          if(${outOfBoundsCondition}) {
            ${glsl.output} = vec4(0);
          } else {
            ${setup}

            ${glsl.output} = vec4(${output});
          }
        }
      `;
  return {
    ...packProgramMetadata,
    hasMain: true,
    output: { dims: input.dims, type: input.type, textureType: types_1.TextureType.packed },
    shaderSource,
  };
};
const createPackProgramInfoLoader = (handler, input) => ({
  ...packProgramMetadata,
  get: () => createPackProgramInfo(handler, input),
});
exports.createPackProgramInfoLoader = createPackProgramInfoLoader;
/**
 * check output coordinate location and return false if it is outside input's width/height boundary
 */
function getOutOfBoundsCondition(rank, shape, dims) {
  if (rank === 0) {
    return 'false';
  }
  if (rank === 1) {
    return `rc > ${shape[0]}`;
  }
  let cond = '';
  for (let i = rank - 2; i < rank; i++) {
    cond += `${dims[i]} >= ${shape[i - rank + 2]}`;
    if (i < rank - 1) {
      cond += '||';
    }
  }
  return cond;
}
/**
 * code snippet to sample input texture with output coordinates
 */
function getOutput(shape, dims) {
  const rank = shape.length;
  if (rank === 0) {
    return 'getA(), 0, 0, 0';
  }
  if (rank === 1) {
    return `getA(rc),
            rc + 1 >= ${shape[0]} ? 0. : getA(rc + 1),
            0, 0`;
  }
  const coord00 = 'r, c';
  const coord01 = 'r, cp1';
  const coord10 = 'rp1, c';
  const coord11 = 'rp1, cp1';
  let D = '';
  if (rank > 2) {
    for (let i = 0; i < rank - 2; ++i) {
      D = D + `${dims[i]},`;
    }
  }
  return `getA(${D}${coord00}),
          rEdge ? 0. : getA(${D}${coord10}),
          cEdge ? 0. : getA(${D}${coord01}),
          rEdge || cEdge ? 0. : getA(${D}${coord11})`;
}
/**
 * code snippet to setup 4 coordinates and edge conditions
 */
function getSetup(rank, dims, rows, cols) {
  if (rank === 0 || rank === 1) {
    return '';
  }
  // rank >= 2 for width+height pack.
  else {
    const setup = `
    int r = ${dims[rank - 2]};
    int c = ${dims[rank - 1]};
    int rp1 = ${dims[rank - 2]} + 1;
    int cp1 = ${dims[rank - 1]} + 1;
    bool rEdge = rp1 >= ${cols};
    bool cEdge = cp1 >= ${rows};
    `;
    return setup;
  }
}
//# sourceMappingURL=pack.js.map
