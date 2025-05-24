'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.softmaxV13 = exports.parseSoftmaxAttributesV13 = exports.parseSoftmaxAttributes = exports.softmax = void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
const transpose_1 = require('./transpose');
const softmaxComputeMaxProgramMetadata = {
  name: 'SoftmaxComputeMax',
  inputNames: ['A'],
  inputTypes: [types_1.TextureType.unpacked],
};
const softmaxComputeScaleProgramMetadata = {
  name: 'SoftmaxComputeScale',
  inputNames: ['A', 'Max'],
  inputTypes: [types_1.TextureType.unpacked, types_1.TextureType.unpacked],
};
const softmaxProgramMetadata = {
  name: 'SoftMax',
  inputNames: ['A', 'Max', 'Norm'],
  inputTypes: [types_1.TextureType.unpacked, types_1.TextureType.unpacked, types_1.TextureType.unpacked],
};
const softmax = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const inputShape = inputs[0].dims.slice();
  const axis = util_1.ShapeUtil.normalizeAxis(attributes.axis, inputShape.length);
  const logicalRowCount = util_1.ShapeUtil.sizeToDimension(inputShape, axis);
  const featureCount = util_1.ShapeUtil.sizeFromDimension(inputShape, axis);
  const output = computeSoftmax(inferenceHandler, inputs, attributes, logicalRowCount, featureCount);
  return output;
};
exports.softmax = softmax;
const parseSoftmaxAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ axis: node.attributes.getInt('axis', 1) });
exports.parseSoftmaxAttributes = parseSoftmaxAttributes;
const parseSoftmaxAttributesV13 = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ axis: node.attributes.getInt('axis', -1) });
exports.parseSoftmaxAttributesV13 = parseSoftmaxAttributesV13;
// The "semantic" meaning of axis has changed in opset-13.
// Please compare: https://github.com/onnx/onnx/blob/main/docs/Operators.md#Softmax
// with https://github.com/onnx/onnx/blob/main/docs/Changelog.md#Softmax-11 for detailed explanations
// To account for the opset-13 behavior, our plan will be to transpose the "axis" dim to the innermost dim
// and perform softmax and then reverse the transpose. We can skip the transposing aspect if the axis is already
// the innermost dim
const softmaxV13 = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const inputShape = inputs[0].dims.slice();
  const axis = util_1.ShapeUtil.normalizeAxis(attributes.axis, inputShape.length);
  const rank = inputShape.length;
  const isTransposeRequired = axis !== rank - 1 ? true : false;
  const transposedInputShape = [];
  let perm = [];
  let transposedInputs = [];
  let transposeAttribute;
  if (isTransposeRequired) {
    perm = Array.from({ length: rank }).map((_, i) => i);
    // swap the innermost dim with the dim corresponding to axis
    perm[axis] = rank - 1;
    perm[rank - 1] = axis;
    perm.map((p) => transposedInputShape.push(inputShape[p]));
    transposeAttribute = (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ perm });
    transposedInputs = (0, transpose_1.transpose)(inferenceHandler, inputs, transposeAttribute);
  }
  const logicalRowCount = isTransposeRequired
    ? util_1.ShapeUtil.sizeToDimension(transposedInputShape, rank - 1)
    : util_1.ShapeUtil.sizeToDimension(inputShape, rank - 1);
  const featureCount = isTransposeRequired
    ? util_1.ShapeUtil.sizeFromDimension(transposedInputShape, rank - 1)
    : util_1.ShapeUtil.sizeFromDimension(inputShape, rank - 1);
  const output = computeSoftmax(
    inferenceHandler,
    isTransposeRequired ? transposedInputs : inputs,
    attributes,
    logicalRowCount,
    featureCount,
  );
  if (isTransposeRequired) {
    const reversedOutput = (0, transpose_1.transpose)(inferenceHandler, output, transposeAttribute);
    return reversedOutput;
  } else {
    return output;
  }
};
exports.softmaxV13 = softmaxV13;
const computeSoftmax = (inferenceHandler, inputs, attributes, logicalRowCount, featureCount) => {
  const computeMaxProgramInfo = createComputeMaxProgramInfo(
    inferenceHandler,
    inputs[0],
    logicalRowCount,
    featureCount,
    [logicalRowCount],
  );
  const max = inferenceHandler.run(
    { ...softmaxComputeMaxProgramMetadata, cacheHint: attributes.cacheKey, get: () => computeMaxProgramInfo },
    inputs,
  );
  const computeScaleProgramInfo = createComputScaleProgramInfo(
    inferenceHandler,
    inputs[0],
    logicalRowCount,
    featureCount,
    computeMaxProgramInfo.output.dims,
    [logicalRowCount],
  );
  const scale = inferenceHandler.run(
    { ...softmaxComputeScaleProgramMetadata, cacheHint: attributes.cacheKey, get: () => computeScaleProgramInfo },
    [inputs[0], max],
  );
  const softMaxProgramInfo = createSoftMaxProgramInfo(
    inferenceHandler,
    inputs[0],
    logicalRowCount,
    featureCount,
    computeMaxProgramInfo.output.dims,
    computeScaleProgramInfo.output.dims,
  );
  const output = inferenceHandler.run(
    { ...softmaxProgramMetadata, cacheHint: attributes.cacheKey, get: () => softMaxProgramInfo },
    [inputs[0], max, scale],
  );
  return [output];
};
/**
 * Create a texture that contains the maximum value of each of the 'N' rows
 */
const createComputeMaxProgramInfo = (inferenceHandler, input, logicalRowCount, featureCount, outputShape) => {
  const [textureWidth, textureHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    input.dims,
    types_1.TextureType.unpacked,
  );
  const rank = outputShape.length;
  if (logicalRowCount < 1 || featureCount < 1) {
    throw new Error('Logical row count N and feature count D must be greater than or equal to 1');
  }
  if (outputShape.length !== 1) {
    throw new Error('Dimensionality of the output should be 1');
  }
  if (outputShape[0] !== logicalRowCount) {
    throw new Error('Shape of the output should be equal to logical row count');
  }
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const shaderSource = `
      float process(int[${rank}] indices) {
        int logical_row_start_offset = indices[0] * ${featureCount};

        float max = getColorAsFloat(${glsl.texture2D}(A, offsetToCoords(logical_row_start_offset, ${textureWidth},
        ${textureHeight} )));
        for(int i=1; i<${featureCount}; ++i)
        {
          float current = getColorAsFloat(${glsl.texture2D}(A, offsetToCoords(logical_row_start_offset + i,
            ${textureWidth}, ${textureHeight})));
          if(current > max)
          max = current;
        }

        return max;
      }`;
  return {
    ...softmaxComputeMaxProgramMetadata,
    output: { dims: outputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
/**
 * Create a texture that contains the normalization factor for each of the 'N' rows
 */
const createComputScaleProgramInfo = (
  inferenceHandler,
  input,
  logicalRowCount,
  featureCount,
  maxElementPerLogicalRow,
  outputShape,
) => {
  const [textureWidth, textureHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    input.dims,
    types_1.TextureType.unpacked,
  );
  const rank = outputShape.length;
  if (logicalRowCount < 1 || featureCount < 1) {
    throw new Error('Logical row count N and feature count D must be greater than or equal to 1');
  }
  if (outputShape.length !== 1) {
    throw new Error('Dimensionality of the output should be 1');
  }
  if (outputShape[0] !== logicalRowCount) {
    throw new Error('Shape of the output should be equal to logical row count');
  }
  if (maxElementPerLogicalRow.length !== 1) {
    throw new Error('Dimensionality of the intermediate results should be 1');
  }
  if (maxElementPerLogicalRow[0] !== logicalRowCount) {
    throw new Error('Shape of the intermediate results should be equal to logical row count');
  }
  const glsl = (0, glsl_source_1.getGlsl)(inferenceHandler.session.backend.glContext.version);
  const shaderSource = `
      float process(int[${rank}] indices) {
        int logical_row_start_offset = indices[0] * ${featureCount};

        float norm_factor = 0.0;
        float max = _Max(indices);
        for(int i=0; i<${featureCount}; ++i)
        {
          norm_factor += exp(getColorAsFloat(${glsl.texture2D}(A, offsetToCoords(logical_row_start_offset + i,
            ${textureWidth}, ${textureHeight}))) - max);
        }

        return norm_factor;
      }`;
  return {
    ...softmaxComputeScaleProgramMetadata,
    output: { dims: outputShape, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const createSoftMaxProgramInfo = (
  inferenceHandler,
  input,
  logicalRowCount,
  featureCount,
  maxElementPerLogicalRow,
  normalizationPerLogicalRow,
) => {
  const [textureWidth, textureHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    input.dims,
    types_1.TextureType.unpacked,
  );
  const rank = input.dims.length;
  if (logicalRowCount < 1 || featureCount < 1) {
    throw new Error('Logical row count N and feature count D must be greater than or equal to 1');
  }
  if (maxElementPerLogicalRow.length !== 1 || normalizationPerLogicalRow.length !== 1) {
    throw new Error('Dimensionality of the intermediate results should be 1');
  }
  if (maxElementPerLogicalRow[0] !== logicalRowCount || normalizationPerLogicalRow[0] !== logicalRowCount) {
    throw new Error('Shape of the intermediate results should be equal to logical row count');
  }
  const shaderSource = `
      float process(int[${rank}] indices) {

      // get offset of current logical tensor index from the 2-D texture coordinates (TexCoords)
      int offset = coordsToOffset(TexCoords, ${textureWidth}, ${textureHeight});

      //determine the logical row for this index
      int logical_row_index[1];
      logical_row_index[0] = offset / ${featureCount};

      float norm_factor = _Norm(logical_row_index);

      // avoid possible division by 0
      // if norm_facor is 0, all elements are zero
      // if so, return 0
      if(norm_factor == 0.0)
        return 0.0;

      return exp(_A(indices) - _Max(logical_row_index)) / norm_factor;
    }`;
  return {
    ...softmaxProgramMetadata,
    output: { dims: input.dims, type: input.type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Softmax requires 1 input.');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type');
  }
};
//# sourceMappingURL=softmax.js.map
