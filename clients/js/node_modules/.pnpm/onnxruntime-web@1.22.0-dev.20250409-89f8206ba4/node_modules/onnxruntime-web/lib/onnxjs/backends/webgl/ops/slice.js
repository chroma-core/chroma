'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.sliceV10 = exports.parseSliceAttributes = exports.slice = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const operators_1 = require('../../../operators');
const util_1 = require('../../../util');
const types_1 = require('../types');
const sliceProgramMetadata = {
  name: 'Slice',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpacked],
};
const slice = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const output = inferenceHandler.run(
    {
      ...sliceProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createSliceProgramInfo(inferenceHandler, inputs[0], attributes),
    },
    inputs,
  );
  return [output];
};
exports.slice = slice;
const parseSliceAttributes = (node) => {
  const starts = node.attributes.getInts('starts');
  const ends = node.attributes.getInts('ends');
  const axes = node.attributes.getInts('axes', []);
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ starts, ends, axes });
};
exports.parseSliceAttributes = parseSliceAttributes;
const createSliceProgramInfo = (_inferenceHandler, input, attributes) => {
  const axes = attributes.axes.length === 0 ? input.dims.slice(0).map((_val, i) => i) : attributes.axes;
  const normalizedAxes = util_1.ShapeUtil.normalizeAxes(axes, input.dims.length);
  const starts = attributes.starts.map((start, i) => {
    if (start > input.dims[normalizedAxes[i]] - 1) {
      return input.dims[normalizedAxes[i]];
    }
    return util_1.ShapeUtil.normalizeAxis(start, input.dims[normalizedAxes[i]]);
  });
  const ends = attributes.ends.map((end, i) => {
    if (end > input.dims[normalizedAxes[i]] - 1) {
      return input.dims[normalizedAxes[i]];
    }
    return util_1.ShapeUtil.normalizeAxis(end, input.dims[normalizedAxes[i]]);
  });
  const outputShape = input.dims.slice();
  const sliceOps = [];
  for (let i = 0; i < normalizedAxes.length; i++) {
    outputShape[normalizedAxes[i]] = ends[i] - starts[i];
    if (starts[i] > 0) {
      sliceOps.push(`outputIdx[${normalizedAxes[i]}] += ${starts[i]};`);
    } // else { sliceOps.push(`outputIdx[${normalizedAxes[i]}] += 0;`); }
  }
  const rank = outputShape.length;
  const shaderSource = `
      float process(int outputIdx[${rank}]) {
        ${sliceOps.join('\n      ')}
        return _A(outputIdx);
      }`;
  return {
    ...sliceProgramMetadata,
    output: { dims: outputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Slice requires 1 input.');
  }
  if (operators_1.NUMBER_TYPES.indexOf(inputs[0].type) === -1) {
    throw new Error('Invalid input type.');
  }
};
const sliceV10 = (inferenceHandler, inputs) => {
  validateInputsV10(inputs);
  const attributes = generateSliceAttributesFromInputs(inferenceHandler, inputs);
  const output = inferenceHandler.run(
    {
      ...sliceProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createSliceProgramInfo(inferenceHandler, inputs[0], attributes),
    },
    [inputs[0]],
  );
  return [output];
};
exports.sliceV10 = sliceV10;
const generateSliceAttributesFromInputs = (inferenceHandler, inputs) => {
  if (
    !inferenceHandler.session.isInitializer(inputs[1].dataId) ||
    !inferenceHandler.session.isInitializer(inputs[2].dataId) ||
    (inputs.length >= 4 && !inferenceHandler.session.isInitializer(inputs[3].dataId)) ||
    (inputs.length >= 5 && !inferenceHandler.session.isInitializer(inputs[4].dataId))
  ) {
    throw new Error('dynamic slice attributes are not allowed');
  }
  if (inputs.length >= 5 && inputs[4].integerData.some((i) => i !== 1)) {
    throw new Error('currently non-1 steps is not supported for Slice');
  }
  const starts = Array.from(inputs[1].integerData);
  const ends = Array.from(inputs[2].integerData);
  const axes = inputs.length >= 4 ? Array.from(inputs[3].integerData) : [];
  const cacheKey = `${axes};${starts};${ends}`;
  return { starts, ends, axes, cacheKey };
};
const validateInputsV10 = (inputs) => {
  if (!inputs || inputs.length < 3 || inputs.length > 5) {
    throw new Error('Invalid input number.');
  }
  if (inputs[1].type !== 'int32' || inputs[1].dims.length !== 1) {
    throw new Error('Invalid input type.');
  }
  if (inputs[2].type !== 'int32' || inputs[2].dims.length !== 1) {
    throw new Error('Invalid input type.');
  }
  if (inputs.length >= 4 && (inputs[3].type !== 'int32' || inputs[3].dims.length !== 1)) {
    throw new Error('Invalid input type.');
  }
  if (inputs.length >= 5 && (inputs[4].type !== 'int32' || inputs[4].dims.length !== 1)) {
    throw new Error('Invalid input type.');
  }
};
//# sourceMappingURL=slice.js.map
