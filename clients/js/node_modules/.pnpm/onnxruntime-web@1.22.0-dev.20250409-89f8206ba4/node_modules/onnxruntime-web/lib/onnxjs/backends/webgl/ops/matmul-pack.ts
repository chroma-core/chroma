// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { BroadcastUtil, ShapeUtil } from '../../../util';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';
import { getCoordsDataType, getGlChannels } from '../utils';

import { getActivationSnippet, InternalActivationAttributes } from './fuse-utils';
import { getBiasForMatmul } from './matmul';

const createPackedMatmulProgramMetadata = (hasBias: boolean, cacheHint: string) => ({
  name: 'MatMul (packed)',
  inputNames: hasBias ? ['A', 'B', 'Bias'] : ['A', 'B'],
  inputTypes: hasBias
    ? [TextureType.packed, TextureType.packed, TextureType.packed]
    : [TextureType.packed, TextureType.packed],
  cacheHint,
});

const createPackedMatmulProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  inputs: Tensor[],
  activationAttributes: InternalActivationAttributes,
): ProgramInfo => {
  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += getBiasForMatmul();' : '';
  const aShape = inputs[0].dims;
  const bShape = inputs[1].dims;
  const outputShape = BroadcastUtil.calcShape(aShape, bShape, true);
  const isBroadcast = !ShapeUtil.areEqual(inputs[0].dims, inputs[1].dims);

  if (!outputShape) {
    throw new Error("Can't use matmul on the given tensors");
  }
  const sharedDim = aShape[aShape.length - 1];
  const sharedDimIndex = Math.ceil(sharedDim / 2);
  const aRank = aShape.length;
  const bRank = bShape.length;

  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const coordsDataType = getCoordsDataType(outputShape.length);
  const outRank = outputShape.length;
  const allGlChannels = getGlChannels();
  const { activationFunction, applyActivation } = getActivationSnippet(activationAttributes);

  const getBiasForMatmulSnippet = hasBias
    ? `${getBiasForMatmul(coordsDataType, allGlChannels, inputs[2].dims, outputShape, true)}`
    : '';

  const getBcastedSamplerForMatmulSnippet = isBroadcast
    ? `${getBcastSamplerForMatmul(coordsDataType, allGlChannels, inputs, outputShape)}`
    : '';

  const getSamplerAInLoopSnippet = isBroadcast ? 'getAAtOutCoordsMatmul(i)' : `getA(${getA(allGlChannels, aRank)})`;
  const getSamplerBInLoopSnippet = isBroadcast ? 'getBAtOutCoordsMatmul(i)' : `getB(${getB(allGlChannels, bRank)})`;
  const getOutputCoordsSnippet = isBroadcast
    ? ''
    : `${coordsDataType} rc =
          getOutputCoords(); int lastDim = rc.${allGlChannels[outRank - 1]}; rc.${allGlChannels[outRank - 1]} =
          rc.${allGlChannels[outRank - 2]}; rc.${allGlChannels[outRank - 2]} = lastDim;
      `;
  const shaderSource = `
            ${getBcastedSamplerForMatmulSnippet}
            ${getBiasForMatmulSnippet}
            ${activationFunction}
            void main() {
              ${getOutputCoordsSnippet}

              vec4 value = vec4(0);
              for (int i = 0; i < ${sharedDimIndex}; i++) {
                vec4 a = ${getSamplerAInLoopSnippet};
                vec4 b = ${getSamplerBInLoopSnippet};

                value += (a.rrbb * b.rgrg);
                value += (a.ggaa * b.baba);
              }
              ${processBias}
              ${applyActivation}
              ${glsl.output} = value;
            }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.packed },
    shaderSource,
    hasMain: true,
  };
};

export const createPackedMatmulProgramInfoLoader = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  activationAttributes: InternalActivationAttributes,
): ProgramInfoLoader => {
  const metadata = createPackedMatmulProgramMetadata(inputs.length > 2, activationAttributes.activationCacheKey);
  return {
    ...metadata,
    get: () => createPackedMatmulProgramInfo(inferenceHandler, metadata, inputs, activationAttributes),
  };
};

function getBcastSamplerForMatmul(
  coordsDataType: string,
  allGlChannels: readonly string[],
  inputs: Tensor[],
  outShape: readonly number[],
): string {
  let unpackedACoordsSnippet = [];
  let unpackedBCoordsSnippet = [];

  const inAShape = inputs[0].dims;
  const inBShape = inputs[1].dims;

  const inARank = inAShape.length;
  const inBRank = inBShape.length;

  const outRank = outShape.length;
  const rankADiff = outRank - inARank;
  const rankBDiff = outRank - inBRank;

  unpackedACoordsSnippet = inAShape.map((_s, i) => `coords.${allGlChannels[i + rankADiff]}`);
  unpackedACoordsSnippet[inARank - 1] = 'i*2';
  unpackedACoordsSnippet.join(', ');
  unpackedBCoordsSnippet = inBShape.map((_s, i) => `coords.${allGlChannels[i + rankBDiff]}`);
  unpackedBCoordsSnippet[inBRank - 2] = 'i*2';
  unpackedBCoordsSnippet.join(', ');

  const broadcastADims = BroadcastUtil.getBroadcastDims(inAShape, outShape);
  const broadcastBDims = BroadcastUtil.getBroadcastDims(inBShape, outShape);

  const coordsASnippet = broadcastADims.map((d) => `coords.${allGlChannels[d + rankADiff]} = 0;`).join('\n');
  const coordsBSnippet = broadcastBDims.map((d) => `coords.${allGlChannels[d + rankBDiff]} = 0;`).join('\n');
  const swapDimSnippet = `int lastDim = coords.${allGlChannels[outRank - 1]};
  coords.${allGlChannels[outRank - 1]} = coords.${allGlChannels[outRank - 2]};
  coords.${allGlChannels[outRank - 2]} = lastDim;`;

  const getBcastSamplerMatmulSource = `
vec4 getAAtOutCoordsMatmul(int i) {
  ${coordsDataType} coords = getOutputCoords();
  ${swapDimSnippet}
  ${coordsASnippet}
  vec4 outputValue = getA(${unpackedACoordsSnippet});
  return outputValue;
}

vec4 getBAtOutCoordsMatmul(int i) {
  ${coordsDataType} coords = getOutputCoords();
  ${swapDimSnippet}
  ${coordsBSnippet}
  vec4 outputValue = getB(${unpackedBCoordsSnippet});
  return outputValue;
}`;

  return getBcastSamplerMatmulSource;
}

function getA(allGlChannels: string[], rank: number): string {
  let res = '';
  for (let i = 0; i < rank - 2; i++) {
    res += `rc.${allGlChannels[i]}, `;
  }
  res += `rc.${allGlChannels[rank - 2]}, ` + 'i*2';
  return res;
}

function getB(allGlChannels: string[], rank: number): string {
  let res = '';
  for (let i = 0; i < rank - 2; i++) {
    res += `rc.${allGlChannels[i]}, `;
  }
  res += 'i*2, ' + `rc.${allGlChannels[rank - 1]}`;
  return res;
}
