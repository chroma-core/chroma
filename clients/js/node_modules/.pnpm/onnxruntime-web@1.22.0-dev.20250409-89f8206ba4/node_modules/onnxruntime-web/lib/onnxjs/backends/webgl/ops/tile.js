'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.tile = void 0;
const operators_1 = require('../../../operators');
const types_1 = require('../types');
const tile = (inferenceHandler, inputs) => {
  validateInputs(inputs);
  const tileProgramMetadata = {
    name: 'Tile',
    inputNames: ['A'],
    inputTypes: [types_1.TextureType.unpacked],
  };
  const output = inferenceHandler.run(
    { ...tileProgramMetadata, get: () => createTileProgramInfo(inferenceHandler, inputs, tileProgramMetadata) },
    inputs,
  );
  return [output];
};
exports.tile = tile;
const createTileProgramInfo = (_handler, inputs, tileProgramMetadata) => {
  const inputShape = inputs[0].dims.slice();
  const outputShape = new Array(inputShape.length);
  const tileOps = [];
  for (let i = 0; i < inputShape.length; i++) {
    outputShape[i] = inputShape[i] * inputs[1].numberData[i];
    tileOps.push(`inputIdx[${i}] = int(mod(float(outputIdx[${i}]), ${inputShape[i]}.));`);
  }
  const rank = outputShape.length;
  const shaderSource = `
      float process(int outputIdx[${rank}]) {
        int inputIdx[${rank}];
        ${tileOps.join('\n')}
        return _A(inputIdx);
      }
    `;
  return {
    ...tileProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: types_1.TextureType.unpacked },
    shaderSource,
  };
};
const validateInputs = (inputs) => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Tile requires 2 input.');
  }
  if (inputs[1].dims.length !== 1) {
    throw new Error('The second input shape must 1 dimension.');
  }
  if (inputs[1].dims[0] !== inputs[0].dims.length) {
    throw new Error('Invalid input shape.');
  }
  if (operators_1.NUMBER_TYPES.indexOf(inputs[0].type) === -1) {
    throw new Error('Invalid input type.');
  }
  if (inputs[1].type !== 'int32' && inputs[1].type !== 'int16') {
    throw new Error('Invalid repeat type.');
  }
};
//# sourceMappingURL=tile.js.map
