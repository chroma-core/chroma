'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.globalMaxPool =
  exports.parseMaxPoolAttributes =
  exports.maxPool =
  exports.parseGlobalAveragePoolAttributes =
  exports.globalAveragePool =
  exports.parseAveragePoolAttributes =
  exports.averagePool =
    void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const types_1 = require('../types');
const averagePool = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const metadata = {
    name: 'AveragePool',
    inputNames: ['X'],
    inputTypes: [types_1.TextureType.unpacked],
    cacheHint: attributes.cacheKey,
  };
  const output = inferenceHandler.run(
    { ...metadata, get: () => createAveragePoolProgramInfo(inputs, metadata, false, attributes) },
    inputs,
  );
  return [output];
};
exports.averagePool = averagePool;
const parseAveragePoolAttributes = (node) => {
  const autoPad = node.attributes.getString('auto_pad', 'NOTSET');
  const ceilMode = node.attributes.getInt('ceil_mode', 0);
  const countIncludePad = node.attributes.getInt('count_include_pad', 0) === 0 ? false : true;
  const kernelShape = node.attributes.getInts('kernel_shape');
  const strides = node.attributes.getInts('strides', []);
  const pads = node.attributes.getInts('pads', []);
  // TODO: support attribute 'ceil_mode'
  if (ceilMode !== 0) {
    throw new Error('using ceil() in shape computation is not yet supported for AveragePool');
  }
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({
    autoPad,
    ceilMode,
    countIncludePad,
    kernelShape,
    strides,
    pads,
  });
};
exports.parseAveragePoolAttributes = parseAveragePoolAttributes;
const createAveragePoolProgramInfo = (inputs, metadata, isGlobalOperator, attributes) => {
  const [adjustedAttributes, outputShape] = getAdjustedPoolAttributesAndOutputShape(
    inputs,
    attributes,
    isGlobalOperator,
  );
  const kernelSize = util_1.ShapeUtil.size(adjustedAttributes.kernelShape);
  const op1 = 'value += _X(x);';
  let op2 = '';
  if (adjustedAttributes.countIncludePad) {
    op2 += `value /= float(${kernelSize});`;
  } else {
    op2 += `value /= float(${kernelSize} - pad);`;
  }
  const poolingCode = generatePoolingCode(inputs[0].dims, adjustedAttributes, op1, op2, '0.0');
  const shaderSource = `
        ${poolingCode}
      `;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const globalAveragePool = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const metadata = {
    name: 'GlobalAveragePool',
    inputNames: ['X'],
    inputTypes: [types_1.TextureType.unpacked],
    cacheHint: `${attributes.countIncludePad}`,
  };
  const output = inferenceHandler.run(
    { ...metadata, get: () => createAveragePoolProgramInfo(inputs, metadata, true, attributes) },
    inputs,
  );
  return [output];
};
exports.globalAveragePool = globalAveragePool;
const parseGlobalAveragePoolAttributes = (node) => {
  const countIncludePad = node.attributes.getInt('count_include_pad', 0) === 0 ? false : true;
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({
    autoPad: '',
    ceilMode: 0,
    countIncludePad,
    kernelShape: [],
    strides: [],
    pads: [],
  });
};
exports.parseGlobalAveragePoolAttributes = parseGlobalAveragePoolAttributes;
const maxPool = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const metadata = {
    name: 'MaxPool',
    inputNames: ['X'],
    inputTypes: [types_1.TextureType.unpacked],
    cacheHint: attributes.cacheKey,
  };
  const output = inferenceHandler.run(
    { ...metadata, get: () => createMaxPoolProgramInfo(inputs, metadata, false, attributes) },
    inputs,
  );
  return [output];
};
exports.maxPool = maxPool;
const parseMaxPoolAttributes = (node) => {
  const autoPad = node.attributes.getString('auto_pad', 'NOTSET');
  const ceilMode = node.attributes.getInt('ceil_mode', 0);
  const kernelShape = node.attributes.getInts('kernel_shape');
  const strides = node.attributes.getInts('strides', []);
  const pads = node.attributes.getInts('pads', []);
  const storageOrder = node.attributes.getInt('storage_order', 0);
  const dilations = node.attributes.getInts('dilations', []);
  // TODO: support attribute 'ceil_mode' and 'storage_order'
  if (storageOrder !== 0) {
    throw new Error('column major storage order is not yet supported for MaxPool');
  }
  if (ceilMode !== 0) {
    throw new Error('using ceil() in shape computation is not yet supported for MaxPool');
  }
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({
    autoPad,
    ceilMode,
    countIncludePad: false,
    kernelShape,
    strides,
    pads,
    storageOrder,
    dilations,
  });
};
exports.parseMaxPoolAttributes = parseMaxPoolAttributes;
const createMaxPoolProgramInfo = (inputs, metadata, isGlobalOperator, attributes) => {
  const [adjustedAttributes, outputShape] = getAdjustedPoolAttributesAndOutputShape(
    inputs,
    attributes,
    isGlobalOperator,
  );
  const op1 = `
      value = max(_X(x), value);
    `;
  const op2 = '';
  const poolingCode = generatePoolingCode(inputs[0].dims, adjustedAttributes, op1, op2, '-1e5');
  const shaderSource = `
      ${poolingCode}
    `;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const getAdjustedPoolAttributesAndOutputShape = (inputs, attributes, isGlobalOperator) => {
  const inputShape = inputs[0].dims.slice();
  const hasDilations = Object.hasOwnProperty.call(attributes, 'dilations');
  const kernelShape = attributes.kernelShape.slice();
  const strides = attributes.strides.slice();
  const dilations = hasDilations ? attributes.dilations.slice() : [];
  const pads = attributes.pads.slice();
  util_1.PoolConvUtil.adjustPoolAttributes(isGlobalOperator, inputShape, kernelShape, strides, dilations, pads);
  const outputShape = util_1.PoolConvUtil.computePoolOutputShape(
    isGlobalOperator,
    inputShape,
    strides,
    dilations,
    kernelShape,
    pads,
    attributes.autoPad,
  );
  const newAttributes = Object.assign({}, attributes);
  if (hasDilations) {
    Object.assign(newAttributes, { kernelShape, strides, pads, dilations, cacheKey: attributes.cacheKey });
  } else {
    Object.assign(newAttributes, { kernelShape, strides, pads, cacheKey: attributes.cacheKey });
  }
  return [newAttributes, outputShape];
};
const globalMaxPoolAttributes = {
  autoPad: '',
  ceilMode: 0,
  countIncludePad: false,
  kernelShape: [],
  strides: [],
  pads: [],
  storageOrder: 0,
  dilations: [],
  cacheKey: '',
};
const globalMaxPoolMetadata = {
  name: 'GlobalMaxPool',
  inputNames: ['X'],
  inputTypes: [types_1.TextureType.unpacked],
};
const globalMaxPool = (inferenceHandler, inputs) => {
  validateInputs(inputs);
  const output = inferenceHandler.run(
    {
      ...globalMaxPoolMetadata,
      get: () => createMaxPoolProgramInfo(inputs, globalMaxPoolMetadata, true, globalMaxPoolAttributes),
    },
    inputs,
  );
  return [output];
};
exports.globalMaxPool = globalMaxPool;
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Pool ops requires 1 input.');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
};
const generatePoolingCode = (inputDims, attributes, op1, op2, start) => {
  const rank = inputDims.length;
  if (attributes.kernelShape.length <= 2) {
    const kw = attributes.kernelShape[attributes.kernelShape.length - 1];
    const sw = attributes.strides[attributes.strides.length - 1];
    const pwStart = attributes.pads[attributes.pads.length / 2 - 1];
    const pwEnd = attributes.pads[attributes.pads.length - 1];
    const dimW = inputDims[rank - 1];
    let codeW = '';
    let codeH = '';
    let codeHEnd = '';
    if (pwStart + pwEnd !== 0) {
      codeW = `
          for (int i = 0; i < ${kw}; i++) {
            x[${rank} - 1] = indices[${rank} - 1] * ${sw} - ${pwStart} + i;
            if (x[${rank} - 1] < 0 || x[${rank} - 1] >= ${dimW}) {
              pad++;
              continue;
            }
            ${op1}
          }`;
    } else {
      codeW = `
          for (int i = 0; i < ${kw}; i++) {
            x[${rank} - 1] = indices[${rank} - 1] * ${sw} - ${pwStart} + i;
            ${op1}
          }`;
    }
    if (attributes.kernelShape.length === 2) {
      const kh = attributes.kernelShape[attributes.kernelShape.length - 2];
      const sh = attributes.strides[attributes.strides.length - 2];
      const phStart = attributes.pads[attributes.pads.length / 2 - 2];
      const phEnd = attributes.pads[attributes.pads.length - 2];
      const dimH = inputDims[rank - 2];
      if (phStart + phEnd !== 0) {
        codeH = `
            for (int j = 0; j < ${kh}; j++) {
              x[${rank} - 2] = indices[${rank} - 2] * ${sh} - ${phStart} + j;
              if (x[${rank} - 2] < 0 || x[${rank} - 2] >= ${dimH}) {
                pad+= ${kw};
                continue;
              }
          `;
      } else {
        codeH = `
            for (int j = 0; j < ${kh}; j++) {
              x[${rank} - 2] = indices[${rank} - 2] * ${sh} - ${phStart} + j;
            `;
      }
      codeHEnd = `
          }
        `;
    }
    const poolingCode = `
        float process(int indices[${rank}]) {
          int x[${rank}];
          copyVec(indices, x);

          float value = ${start};
          int pad = 0;
          ${codeH}
          ${codeW}
          ${codeHEnd}
          ${op2}
          return value;
        }
      `;
    return poolingCode;
  } else {
    const kernelSize = util_1.ShapeUtil.size(attributes.kernelShape);
    const kernelStrides = util_1.ShapeUtil.computeStrides(attributes.kernelShape);
    const stridesRank = kernelStrides.length;
    const padsRank = attributes.pads.length;
    const offsetToIndicesFunction = offsetToIndices(stridesRank);
    const copyInputDims = copyArray(inputDims, 'inputDims');
    const copyPads = copyArray(attributes.pads, 'pads');
    const copyKernelStrides = copyArray(kernelStrides, 'kernelStrides');
    const copyStrides = copyArray(attributes.strides, 'strides');
    const hasPads = attributes.pads.reduce((sum, cur) => sum + cur);
    let padCode = '';
    if (hasPads) {
      padCode = `
            if (x[j] >= inputDims[j] || x[j] < 0) {
              pad++;
              isPad = true;
              break;
            }
          }
          if (!isPad) {
            ${op1}
          }`;
    } else {
      padCode = `
          }
          ${op1}
        `;
    }
    const poolingCode = `
        ${offsetToIndicesFunction}
        float process(int indices[${rank}]) {
          int x[${rank}];
          copyVec(indices, x);
          int offset[${stridesRank}];
          int pads[${padsRank}];
          int inputDims[${rank}];
          int kernelStrides[${stridesRank}];
          int strides[${stridesRank}];
          ${copyPads}
          ${copyInputDims}
          ${copyStrides}
          ${copyKernelStrides}

          float value = ${start};
          int pad = 0;
          bool isPad = false;
          for (int i = 0; i < ${kernelSize}; i++) {
            offsetToIndices(i, kernelStrides, offset);
            isPad = false;
            for (int j = ${rank} - ${stridesRank}; j < ${rank}; j++) {
              x[j] = indices[j] * strides[j - ${rank} + ${stridesRank}]
                + offset[j - ${rank} + ${stridesRank}] - pads[j - 2];
              ${padCode}
          }
          ${op2}

          return value;
        }
      `;
    return poolingCode;
  }
};
const copyArray = (array, arrayName) => {
  let block = '';
  for (let i = 0; i < array.length; i++) {
    block += `
      ${arrayName}[${i}] = ${array[i]};
    `;
  }
  return block;
};
const offsetToIndices = (rank) => `
  void offsetToIndices(int offset, int[${rank}] strides, out int[${rank}] indices) {
    if (${rank} == 0) {
      return;
    }
    for (int i = 0; i < ${rank} - 1; ++i) {
      indices[i] = offset / strides[i];
      offset -= indices[i] * strides[i];
    }
    indices[${rank} - 1] = offset;
  }`;
//# sourceMappingURL=pool.js.map
