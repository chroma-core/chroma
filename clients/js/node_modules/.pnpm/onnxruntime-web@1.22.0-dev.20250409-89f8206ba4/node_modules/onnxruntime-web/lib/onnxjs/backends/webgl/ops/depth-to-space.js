'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.parseDepthToSpaceAttributes = exports.depthToSpace = void 0;
const transpose_1 = require('./transpose');
const depthToSpace = (inferenceHandler, inputs, attributes) => {
  validateInputs(inputs);
  const blocksize = attributes.blocksize;
  const blocksizeSqr = blocksize * blocksize;
  const transposePerm = attributes.mode === 'DCR' ? [0, 3, 4, 1, 5, 2] : [0, 1, 4, 2, 5, 3];
  const firstReshapeShape =
    attributes.mode === 'DCR'
      ? [
          inputs[0].dims[0],
          blocksize,
          blocksize,
          inputs[0].dims[1] / blocksizeSqr,
          inputs[0].dims[2],
          inputs[0].dims[3],
        ]
      : [
          inputs[0].dims[0],
          inputs[0].dims[1] / blocksizeSqr,
          blocksize,
          blocksize,
          inputs[0].dims[2],
          inputs[0].dims[3],
        ];
  // const transpose = new WebGLTranspose();
  // const attributes = new Attribute(undefined);
  // attributes.set('perm', 'ints', transposePerm);
  // transpose.initialize(attributes);
  // First reshape
  const firstReshapedTensor = inferenceHandler.reshapeUnpacked(inputs[0], firstReshapeShape);
  // transpose
  const transposeAttributes = { perm: transposePerm, cacheKey: `${transposePerm}` };
  const [transposeOutput] = (0, transpose_1.transpose)(inferenceHandler, [firstReshapedTensor], transposeAttributes);
  // Second reshape
  const secondReshapeShape = [
    inputs[0].dims[0],
    inputs[0].dims[1] / blocksizeSqr,
    inputs[0].dims[2] * blocksize,
    inputs[0].dims[3] * blocksize,
  ];
  const result = inferenceHandler.reshapeUnpacked(transposeOutput, secondReshapeShape);
  return [result];
};
exports.depthToSpace = depthToSpace;
const parseDepthToSpaceAttributes = (node) => {
  // processing node attributes
  const blocksize = node.attributes.getInt('blocksize');
  if (blocksize < 1) {
    throw new Error(`blocksize must be >= 1, but got : ${blocksize} for DepthToSpace`);
  }
  const mode = node.attributes.getString('mode', 'DCR');
  if (mode !== 'DCR' && mode !== 'CRD') {
    throw new Error(`unrecognized mode: ${mode} for DepthToSpace`);
  }
  return { mode, blocksize };
};
exports.parseDepthToSpaceAttributes = parseDepthToSpaceAttributes;
const validateInputs = (inputs) => {
  if (inputs.length !== 1) {
    throw new Error(`DepthToSpace expect 1 inputs, but got ${inputs.length}`);
  }
  // Input has to be a 4-D tensor
  // TODO: Support string depth-to-space.
  if (inputs[0].type === 'string' || inputs[0].dims.length !== 4) {
    throw new TypeError('DepthToSpace input should be a 4-D numeric tensor');
  }
};
//# sourceMappingURL=depth-to-space.js.map
