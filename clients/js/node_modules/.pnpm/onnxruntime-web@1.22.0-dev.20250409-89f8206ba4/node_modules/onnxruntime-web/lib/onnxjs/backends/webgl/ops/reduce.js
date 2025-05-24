'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.reduceLogSumSquare =
  exports.reduceLogSum =
  exports.reduceProd =
  exports.reduceMin =
  exports.reduceMax =
  exports.reduceMean =
  exports.reduceSum =
  exports.parseReduceAttributes =
    void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const operators_1 = require('../../../operators');
const util_1 = require('../../../util');
const types_1 = require('../types');
const reduce = (inferenceHandler, inputs, attributes, name, reduceOp) => {
  validateInputs(inputs);
  const reduceProgramMetadata = {
    name,
    inputNames: ['A'],
    inputTypes: [types_1.TextureType.unpacked],
  };
  const output = inferenceHandler.run(
    {
      ...reduceProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createReduceProgramInfo(inferenceHandler, inputs, attributes, name, reduceOp, reduceProgramMetadata),
    },
    inputs,
  );
  return [output];
};
const parseReduceAttributes = (node) => {
  const axes = node.attributes.getInts('axes', []);
  const keepDims = node.attributes.getInt('keepdims', 1) === 1;
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ axes, keepDims });
};
exports.parseReduceAttributes = parseReduceAttributes;
const createReduceProgramInfo = (_handler, inputs, attributes, _name, reduceOp, reduceProgramMetadata) => {
  const outputShape = [];
  const iRank = inputs[0].dims.length || 1;
  const idxCopy = []; // copy output indexes to input indexes
  const axes = util_1.ShapeUtil.normalizeAxes(attributes.axes, inputs[0].dims.length);
  const ops = reduceOp(inputs, axes);
  let reduceOps = ops[1];
  for (let k = 0; k < inputs[0].dims.length; k++) {
    // if this axis is reduced
    if (axes.indexOf(k) >= 0 || axes.length === 0) {
      if (attributes.keepDims) {
        outputShape.push(1);
      } // else { remove the axis from outputShape; }
      // loop over the d-th axis
      reduceOps = `
          for(int j${k} = 0; j${k} < ${inputs[0].dims[k]}; j${k}++) {
            inputIdx[${k}] = j${k};
            ${reduceOps}
          }`;
    } else {
      idxCopy.push(`inputIdx[${k}] = outputIdx[${outputShape.length}];`);
      outputShape.push(inputs[0].dims[k]);
    }
  }
  const oRank = outputShape.length || 1;
  const shaderSource = `
      float process(int outputIdx[${oRank}]) {
        float value;                 // final result
        int inputIdx[${iRank}];      // addressing input data
        ${idxCopy.join('\n')}
        ${ops[0]}       // init ops for reduce max/min
        ${reduceOps}
        ${ops[2]}       // final computation for reduce mean
        return value;
      }`;
  return {
    ...reduceProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  // TODO: support Reduce* operators with 2 inputs.
  if (!inputs || inputs.length !== 1) {
    throw new Error('Reduce op requires 1 input.');
  }
  if (operators_1.NUMBER_TYPES.indexOf(inputs[0].type) === -1) {
    throw new Error('Invalid input type.');
  }
};
const reduceSum = (inferenceHandler, inputs, attributes) => {
  const reduceOp = () => ['value = 0.0;', 'value += _A(inputIdx);', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceSum', reduceOp);
};
exports.reduceSum = reduceSum;
const reduceMean = (inferenceHandler, inputs, attributes) => {
  const reduceOp = (inputs, axes) => {
    let size = 1.0;
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        size *= inputs[0].dims[k];
      }
    }
    return ['value = 0.0;', 'value += _A(inputIdx);', `value /= ${size}.;`]; // ensure real number with `.`
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMean', reduceOp);
};
exports.reduceMean = reduceMean;
const reduceMax = (inferenceHandler, inputs, attributes) => {
  const reduceOp = (inputs, axes) => {
    const idxZero = [];
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`inputIdx[${k}] = 0;`); // first element
      }
    }
    return [`${idxZero.join('\n')}\nvalue = _A(inputIdx);`, 'value = max(value, _A(inputIdx));', ''];
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMax', reduceOp);
};
exports.reduceMax = reduceMax;
const reduceMin = (inferenceHandler, inputs, attributes) => {
  const reduceOp = (inputs, axes) => {
    const idxZero = [];
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`inputIdx[${k}] = 0;`); // first element
      }
    }
    return [`${idxZero.join('\n')}\nvalue = _A(inputIdx);`, 'value = min(value, _A(inputIdx));', ''];
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMin', reduceOp);
};
exports.reduceMin = reduceMin;
const reduceProd = (inferenceHandler, inputs, attributes) => {
  const reduceOp = () => ['value = 1.0;', 'value *= _A(inputIdx);', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceProd', reduceOp);
};
exports.reduceProd = reduceProd;
const reduceLogSum = (inferenceHandler, inputs, attributes) => {
  const reduceOp = () => ['value = 0.0;', 'value += _A(inputIdx);', 'value = log(value);'];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceLogSum', reduceOp);
};
exports.reduceLogSum = reduceLogSum;
const reduceLogSumSquare = (inferenceHandler, inputs, attributes) => {
  const reduceOp = () => ['float t; value = 0.0;', 't = _A(inputIdx); value += t * t;', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceLogSumSquare', reduceOp);
};
exports.reduceLogSumSquare = reduceLogSumSquare;
//# sourceMappingURL=reduce.js.map
