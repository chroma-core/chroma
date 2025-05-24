// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, TextureType } from '../types';
import { getCoordsDataType } from '../utils';

import { getChannels } from './packing-utils';

const packProgramMetadata = {
  name: 'pack',
  inputNames: ['A'],
  inputTypes: [TextureType.unpackedReversed],
};

const createPackProgramInfo = (handler: WebGLInferenceHandler, input: Tensor): ProgramInfo => {
  const glsl = getGlsl(handler.session.backend.glContext.version);
  const inputShape = input.dims;

  const inputRank = inputShape.length;
  // createTextureLayoutFromShape won't change output rank. Need to verify by running tests
  const outputRank = input.dims.length;

  const coordsDataType = getCoordsDataType(outputRank);
  const channels = getChannels('rc', outputRank);
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
    output: { dims: input.dims, type: input.type, textureType: TextureType.packed },
    shaderSource,
  };
};

export const createPackProgramInfoLoader = (handler: WebGLInferenceHandler, input: Tensor): ProgramInfoLoader => ({
  ...packProgramMetadata,
  get: () => createPackProgramInfo(handler, input),
});

/**
 * check output coordinate location and return false if it is outside input's width/height boundary
 */
function getOutOfBoundsCondition(rank: number, shape: readonly number[], dims: string[]): string {
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
function getOutput(shape: readonly number[], dims: string[]): string {
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
function getSetup(rank: number, dims: string[], rows: number, cols: number): string {
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
