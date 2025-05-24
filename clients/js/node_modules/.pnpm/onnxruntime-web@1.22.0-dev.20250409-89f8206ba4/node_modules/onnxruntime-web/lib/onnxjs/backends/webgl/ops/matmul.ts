// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { BroadcastUtil, ShapeUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';
import { getCoordsDataType, getGlChannels } from '../utils';

import { getActivationSnippet, InternalActivationAttributes, parseInternalActivationAttributes } from './fuse-utils';
import { createPackedMatmulProgramInfoLoader } from './matmul-pack';

export const matMul: OperatorImplementation<InternalActivationAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: InternalActivationAttributes,
): Tensor[] => {
  validateInputs(inputs);

  if (inferenceHandler.session.pack) {
    return [inferenceHandler.run(createPackedMatmulProgramInfoLoader(inferenceHandler, inputs, attributes), inputs)];
  } else {
    return [inferenceHandler.run(createMatmulProgramInfoLoader(inputs, attributes), inputs)];
  }
};

export const parseMatMulAttributes: OperatorInitialization<InternalActivationAttributes> = (
  node: Graph.Node,
): InternalActivationAttributes => parseInternalActivationAttributes(node.attributes);

const createMatmulProgramMetadata = (hasBias: boolean, cacheHint: string) => ({
  name: 'MatMul',
  inputNames: hasBias ? ['A', 'B', 'Bias'] : ['A', 'B'],
  inputTypes: hasBias
    ? [TextureType.unpacked, TextureType.unpacked, TextureType.unpacked]
    : [TextureType.unpacked, TextureType.unpacked],
  cacheHint,
});

function createMatmulProgramInfo(
  metadata: ProgramMetadata,
  inputs: Tensor[],
  activationAttributes: InternalActivationAttributes,
): ProgramInfo {
  const aShape = inputs[0].dims;
  const bShape = inputs[1].dims;
  const outputShape = BroadcastUtil.calcShape(aShape, bShape, true);
  if (!outputShape) {
    throw new Error("Can't use matmul on the given tensors");
  }
  const coordsDataType = getCoordsDataType(outputShape.length);
  const allGlChannels = getGlChannels();
  const { activationFunction, applyActivation } = getActivationSnippet(activationAttributes);

  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += getBiasForMatmul();' : '';
  const getBiasForMatmulSnippet = hasBias
    ? `${getBiasForMatmul(coordsDataType, allGlChannels, inputs[2].dims, outputShape, false)}`
    : '';

  const rank = outputShape.length;
  const arank = aShape.length;
  const brank = bShape.length;
  const sharedDim = aShape[aShape.length - 1];
  const shaderSource = `
    ${activationFunction}
    ${getBiasForMatmulSnippet}
    float process(int indices[${rank}]) {
        int a[${arank}];
        int b[${brank}];
        bcastMatmulIndices_A(indices, a);
        bcastMatmulIndices_B(indices, b);

        float value;
        for (int k=0; k<${sharedDim}; ++k) {
            a[${arank - 1}] = k;
            b[${brank - 2}] = k;
            value += _A(a) * _B(b);
        }
        ${processBias}
        ${applyActivation}
        return value;
    }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
  };
}

export function createMatmulProgramInfoLoader(
  inputs: Tensor[],
  activationAttributes: InternalActivationAttributes,
): ProgramInfoLoader {
  const metadata = createMatmulProgramMetadata(inputs.length > 2, activationAttributes.activationCacheKey);
  return { ...metadata, get: () => createMatmulProgramInfo(metadata, inputs, activationAttributes) };
}

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('MatMul requires 2 inputs.');
  }

  if (inputs[0].dims[inputs[0].dims.length - 1] !== inputs[1].dims[inputs[1].dims.length - 2]) {
    throw new Error('shared dimension does not match.');
  }

  if (
    (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') ||
    (inputs[1].type !== 'float32' && inputs[1].type !== 'float64')
  ) {
    throw new Error('inputs should be float type');
  }

  if (inputs[0].type !== inputs[1].type) {
    throw new Error('inputs types should match');
  }
};

export function getBiasForMatmul(
  coordsDataType: string,
  allGlChannels: readonly string[],
  inShape: readonly number[],
  outShape: readonly number[],
  isPacked: boolean,
): string {
  let unpackedCoordsSnippet = '';
  const inRank = inShape.length;
  const outRank = outShape.length;
  const rankDiff = outRank - inRank;
  if (outRank < 2 && inRank > 0) {
    unpackedCoordsSnippet = 'coords';
  } else {
    unpackedCoordsSnippet = inShape.map((_s, i) => `coords.${allGlChannels[i + rankDiff]}`).join(', ');
  }
  const broadcastDims = BroadcastUtil.getBroadcastDims(inShape, outShape);
  const coordsSnippet = broadcastDims.map((d) => `coords.${allGlChannels[d + rankDiff]} = 0;`).join('\n');
  const inSize = ShapeUtil.size(inShape);
  const isInputScalar = inSize === 1;
  let output = 'vec4(outputValue.xx, outputValue.yy)';
  if (isInputScalar) {
    output = 'vec4(outputValue.x)';
  }
  const getBiasForMatmulSource = isPacked
    ? `
vec4 getBiasForMatmul() {
  ${coordsDataType} coords = getOutputCoords();
  ${coordsSnippet}
  vec4 outputValue = getBias(${unpackedCoordsSnippet});
  return ${output};
}`
    : `
float getBiasForMatmul() {
  ${coordsDataType} coords = getOutputCoords();
  ${coordsSnippet}
  return getBias(coords.x);
}`;

  return getBiasForMatmulSource;
}
