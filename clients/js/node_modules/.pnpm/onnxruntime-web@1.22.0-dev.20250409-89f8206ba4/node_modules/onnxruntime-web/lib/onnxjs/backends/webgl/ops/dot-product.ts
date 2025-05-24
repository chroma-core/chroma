// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

import { getActivationSnippet, InternalActivationAttributes } from './fuse-utils';
import { calculateIm2ColDims } from './im2col';

const createDotProductProgramMetadata = (hasBias: boolean, attributes: InternalActivationAttributes) => ({
  name: 'ConvDotProduct',
  inputNames: hasBias ? ['Im2Col', 'K', 'B'] : ['Im2Col', 'K'],
  inputTypes: hasBias
    ? [TextureType.unpacked, TextureType.packedLastDimension, TextureType.unpacked]
    : [TextureType.unpacked, TextureType.packedLastDimension],
  cacheKey: attributes.activationCacheKey,
});

const createDotProductProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  inputs: readonly Tensor[],
  outputShape: number[],
  attributes: InternalActivationAttributes,
): ProgramInfo => {
  const xshape = inputs[0].dims;
  const kshape = inputs[1].dims;
  const adjustedKernelShape = [kshape[0], Math.ceil((xshape[1] * kshape[2] * kshape[3]) / 4)];
  const im2colShape = calculateIm2ColDims(xshape, kshape, outputShape);
  const [kWidth, kHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    adjustedKernelShape,
    TextureType.packedLastDimension,
  );

  const im2colStrides = ShapeUtil.computeStrides(im2colShape);
  const [im2colWidth, im2colHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    im2colShape,
    TextureType.packedLastDimension,
  );
  const rank = outputShape.length;

  const initValue = inputs.length < 3 ? '0.0' : '_B(b)';
  const sharedDim = Math.ceil((xshape[1] * kshape[2] * kshape[3]) / 4);
  const { activationFunction, applyActivation } = getActivationSnippet(attributes);
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const shaderSource = `
${activationFunction}
float process(int indices[${rank}]) {
  int b[1];
  b[0] = indices[1];
  int im2col[4];
  im2col[0] = indices[0];
  im2col[1] = indices[2];
  im2col[2] = indices[3];
  int im2colOffset = im2col[0] * ${im2colStrides[0]} + im2col[1] * ${im2colStrides[1]} + im2col[2] * ${
    im2colStrides[2]
  };
  int kernelOffset = indices[1] * ${adjustedKernelShape[1]};
  float value = ${initValue};
  for (int i = 0; i < ${sharedDim}; ++i) {
    vec2 im2colCoords = offsetToCoords(im2colOffset, ${im2colWidth}, ${im2colHeight});
    vec2 kernelCoords = offsetToCoords(kernelOffset, ${kWidth}, ${kHeight});
    value += dot(${glsl.texture2D}(Im2Col, im2colCoords), ${glsl.texture2D}(K, kernelCoords));
    ++im2colOffset;
    ++kernelOffset;
  }
  ${applyActivation}
  return value;
}`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

export const createDotProductProgramInfoLoader = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: readonly Tensor[],
  outputShape: number[],
  attributes: InternalActivationAttributes,
): ProgramInfoLoader => {
  const metadata = createDotProductProgramMetadata(inputs.length > 2, attributes);
  return {
    ...metadata,
    get: () => createDotProductProgramInfo(inferenceHandler, metadata, inputs, outputShape, attributes),
  };
};
