'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createUnpackedGroupedConvProgramInfoLoader = void 0;
const instrument_1 = require('../../../instrument');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const conv_1 = require('./conv');
const fuse_utils_1 = require('./fuse-utils');
const createUnpackedGroupedConvProgramMetadata = (hasBias, cacheHint) => ({
  name: 'GroupedConv',
  inputNames: hasBias ? ['X', 'W', 'Bias'] : ['X', 'W'],
  inputTypes: hasBias
    ? [types_1.TextureType.unpacked, types_1.TextureType.unpacked, types_1.TextureType.unpacked]
    : [types_1.TextureType.unpacked, types_1.TextureType.unpacked],
  cacheHint,
});
const createUnpackedGroupedConvProgramInfo = (inferenceHandler, inputs, metadata, attributes) => {
  const hasBias = inputs.length > 2;
  const processBias = hasBias ? 'value += getBias(output_channel);' : '';
  const xShape = inputs[0].dims.slice();
  const wShape = inputs[1].dims.slice();
  const outputChannelsPerGroup = wShape[0] / attributes.group;
  instrument_1.Logger.verbose(
    'GroupedConv',
    `autpPad:${attributes.autoPad}, dilations:${attributes.dilations}, group:${attributes.group}, kernelShape:${attributes.kernelShape}, pads:${attributes.pads}, strides:${attributes.strides}`,
  );
  const outputShape = (0, conv_1.calculateOutputShape)(
    xShape,
    wShape,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
  );
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const { activationFunction, applyActivation } = (0, fuse_utils_1.getActivationSnippet)(attributes);
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
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
    hasMain: true,
  };
};
const createUnpackedGroupedConvProgramInfoLoader = (inferenceHandler, inputs, attributes) => {
  const metadata = createUnpackedGroupedConvProgramMetadata(inputs.length > 2, attributes.cacheKey);
  return {
    ...metadata,
    get: () => createUnpackedGroupedConvProgramInfo(inferenceHandler, inputs, metadata, attributes),
  };
};
exports.createUnpackedGroupedConvProgramInfoLoader = createUnpackedGroupedConvProgramInfoLoader;
//# sourceMappingURL=conv-grouped.js.map
