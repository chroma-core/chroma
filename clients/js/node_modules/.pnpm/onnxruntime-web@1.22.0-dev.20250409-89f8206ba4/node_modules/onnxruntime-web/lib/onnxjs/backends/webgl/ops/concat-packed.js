'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.createPackedConcatProgramInfoLoader = void 0;
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const utils_1 = require('../utils');
const packing_utils_1 = require('./packing-utils');
const createPackedConcatProgramMetadata = (inputCount, cacheHint) => ({
  name: 'Concat (packed)',
  inputNames: Array.from({ length: inputCount }, (_v, i) => `X${i}`),
  inputTypes: Array(inputCount).fill(types_1.TextureType.packed),
  cacheHint,
});
const createPackedConcatProgramInfo = (handler, metadata, inputs, axis) => {
  const inputShape = inputs[0].dims.slice();
  if (axis >= inputShape.length || axis < -1 * inputShape.length) {
    throw new Error("axis specified for concat doesn't match input dimensionality");
  }
  if (axis < 0) {
    axis = inputShape.length + axis;
  }
  // ensure all of the non-concatenated axes match each other
  // calculate the shape of the output tensor while we do that
  const outputShape = inputShape.slice(0);
  for (let i = 1; i < inputs.length; i++) {
    const dataNShape = inputs[i].dims.slice();
    for (let axisIndex = 0; axisIndex < inputShape.length; axisIndex++) {
      // add to the placeholder for computing output shape
      if (axisIndex === axis) {
        outputShape[axis] += dataNShape[axisIndex];
      }
      // ensure all non-cancatenated axes match each other
      else if (inputShape[axisIndex] !== dataNShape[axisIndex]) {
        throw new Error('non concat dimensions must match');
      }
    }
  }
  const rank = outputShape.length;
  const coords = (0, packing_utils_1.getChannels)('coords', rank);
  const dtype = (0, utils_1.getCoordsDataType)(rank);
  const unpackChannel = (0, packing_utils_1.unpackFromChannel)();
  const shapes = inputs.map((i) => i.dims);
  const channels = (0, utils_1.getGlChannels)(rank);
  const offsets = new Array(shapes.length - 1);
  offsets[0] = shapes[0][axis];
  for (let i = 1; i < offsets.length; i++) {
    offsets[i] = offsets[i - 1] + shapes[i][axis];
  }
  const channel = channels[axis];
  const lastChannels = channels.slice(-2);
  const allChannels = channels.join();
  let getValueSnippet = `if (${channel} < ${offsets[0]}) {
        return getChannel(
            getX0(${allChannels}), vec2(${lastChannels.join()}));
        }`;
  for (let i = 1; i < offsets.length; i++) {
    const shift = offsets[i - 1];
    getValueSnippet += `
            if (${channel} < ${offsets[i]}  && ${channel} >= ${offsets[i - 1]}) {
              return getChannel(
                getX${i}(${getShiftedChannelsSnippet(channels, channel, shift)}),
                vec2(${getShiftedChannelsSnippet(lastChannels, channel, shift)}));
            }`;
  }
  const lastIndex = offsets.length;
  const shift = offsets[offsets.length - 1];
  getValueSnippet += `
            return getChannel(
              getX${lastIndex}(${getShiftedChannelsSnippet(channels, channel, shift)}),
              vec2(${getShiftedChannelsSnippet(lastChannels, channel, shift)}));`;
  const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
  const shaderSource = `
          ${unpackChannel}
          float getValue(${channels.map((x) => 'int ' + x)}) {
            ${getValueSnippet}
          }

          void main() {
            ${dtype} coords = getOutputCoords();
            int lastDim = coords.${channels[rank - 1]};
            coords.${channels[rank - 1]} = coords.${channels[rank - 2]};
            coords.${channels[rank - 2]} = lastDim;

            vec4 result = vec4(getValue(${coords}), 0., 0., 0.);

            ${coords[rank - 1]} = ${coords[rank - 1]} + 1;
            if (${coords[rank - 1]} < ${outputShape[rank - 1]}) {
              result.g = getValue(${coords});
            }

            ${coords[rank - 2]} = ${coords[rank - 2]} + 1;
            if (${coords[rank - 2]} < ${outputShape[rank - 2]}) {
              result.a = getValue(${coords});
            }

            ${coords[rank - 1]} = ${coords[rank - 1]} - 1;
            if (${coords[rank - 2]} < ${outputShape[rank - 2]} &&
                ${coords[rank - 1]} < ${outputShape[rank - 1]}) {
              result.b = getValue(${coords});
            }
            ${glsl.output} = result;
          }
        `;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.packed },
    shaderSource,
    hasMain: true,
  };
};
const createPackedConcatProgramInfoLoader = (handler, inputs, attributes) => {
  const metadata = createPackedConcatProgramMetadata(inputs.length, attributes.cacheKey);
  return { ...metadata, get: () => createPackedConcatProgramInfo(handler, metadata, inputs, attributes.axis) };
};
exports.createPackedConcatProgramInfoLoader = createPackedConcatProgramInfoLoader;
const getShiftedChannelsSnippet = (channels, channel, shift) => {
  const channelIdx = channels.indexOf(channel);
  const res = channels.map((c, idx) => {
    if (idx === channelIdx) {
      return `${c} - ${shift}`;
    } else {
      return c;
    }
  });
  return res.join();
};
//# sourceMappingURL=concat-packed.js.map
