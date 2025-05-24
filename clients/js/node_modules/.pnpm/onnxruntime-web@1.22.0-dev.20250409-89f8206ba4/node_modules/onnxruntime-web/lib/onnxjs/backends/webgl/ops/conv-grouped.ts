// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Logger } from '../../../instrument';
import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

import { calculateOutputShape, ConvAttributes } from './conv';
import { getActivationSnippet } from './fuse-utils';

const createUnpackedGroupedConvProgramMetadata = (hasBias: boolean, cacheHint: string): ProgramMetadata => ({
  name: 'GroupedConv',
  inputNames: hasBias ? ['X', 'W', 'Bias'] : ['X', 'W'],
  inputTypes: hasBias
    ? [TextureType.unpacked, TextureType.unpacked, TextureType.unpacked]
    : [TextureType.unpacked, TextureType.unpacked],
  cacheHint,
});

const createUnpackedGroupedConvProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: readonly Tensor[],
  metadata: ProgramMetadata,
  attributes: ConvAttributes,
): ProgramInfo => {
  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += getBias(output_channel);' : '';
  const xShape = inputs[0].dims.slice();
  const wShape = inputs[1].dims.slice();
  const outputChannelsPerGroup = wShape[0] / attributes.group;
  Logger.verbose(
    'GroupedConv',
    `autpPad:${attributes.autoPad}, dilations:${attributes.dilations}, group:${attributes.group}, kernelShape:${
      attributes.kernelShape
    }, pads:${attributes.pads}, strides:${attributes.strides}`,
  );
  const outputShape = calculateOutputShape(xShape, wShape, attributes.dilations, attributes.pads, attributes.strides);
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const { activationFunction, applyActivation } = getActivationSnippet(attributes);

  const shaderSource = `
  const ivec2 strides = ivec2(${attributes.strides[0]}, ${attributes.strides[1]});
  const ivec2 pads = ivec2(${attributes.pads[0]}, ${attributes.pads[1]});
  ${activationFunction}
  void main() {
    ivec4 coords = getOutputCoords();
    int batch = coords.x;
    int output_channel = coords.y;
    ivec2 xRCCorner = coords.zw * strides - pads;
    int group_id = output_channel / ${outputChannelsPerGroup};

    float value = 0.0;
    for (int wInChannel = 0; wInChannel < ${wShape[1]}; wInChannel++) {
      int input_channel = group_id * ${wShape[1]} + wInChannel;
      for (int wHeight = 0; wHeight < ${wShape[2]}; wHeight++) {
        int xHeight = xRCCorner.x + wHeight * ${attributes.dilations[0]};

        if (xHeight < 0 || xHeight >= ${xShape[2]}) {
          continue;
        }

        for (int wWidth = 0; wWidth < ${wShape[3]}; wWidth++) {
          int xWidth = xRCCorner.y + wWidth * ${attributes.dilations[1]};
          if (xWidth < 0 || xWidth >= ${xShape[3]}) {
            continue;
          }

          float xVal = getX(batch, input_channel, xWidth, xHeight);
          float wVal = getW(output_channel, wInChannel, wWidth, wHeight);
          value += xVal*wVal;
        }
      }
    }
    ${processBias}
    ${applyActivation}
    ${glsl.output} = vec4(value, .0, .0, .0);
  }
`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
    hasMain: true,
  };
};

export const createUnpackedGroupedConvProgramInfoLoader = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: readonly Tensor[],
  attributes: ConvAttributes,
): ProgramInfoLoader => {
  const metadata = createUnpackedGroupedConvProgramMetadata(inputs.length > 2, attributes.cacheKey);
  return {
    ...metadata,
    get: () => createUnpackedGroupedConvProgramInfo(inferenceHandler, inputs, metadata, attributes),
  };
};
