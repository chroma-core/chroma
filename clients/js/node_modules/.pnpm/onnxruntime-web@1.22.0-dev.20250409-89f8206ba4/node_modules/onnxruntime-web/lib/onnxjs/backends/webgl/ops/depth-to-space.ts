// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';

import { transpose, TransposeAttributes } from './transpose';

export interface DepthToSpaceAttributes {
  mode: 'DCR' | 'CRD';
  blocksize: number;
}

export const depthToSpace: OperatorImplementation<DepthToSpaceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: DepthToSpaceAttributes,
): Tensor[] => {
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
  const transposeAttributes: TransposeAttributes = { perm: transposePerm, cacheKey: `${transposePerm}` };
  const [transposeOutput] = transpose(inferenceHandler, [firstReshapedTensor], transposeAttributes);

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

export const parseDepthToSpaceAttributes: OperatorInitialization<DepthToSpaceAttributes> = (
  node: Graph.Node,
): DepthToSpaceAttributes => {
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

const validateInputs = (inputs: Tensor[]): void => {
  if (inputs.length !== 1) {
    throw new Error(`DepthToSpace expect 1 inputs, but got ${inputs.length}`);
  }

  // Input has to be a 4-D tensor
  // TODO: Support string depth-to-space.
  if (inputs[0].type === 'string' || inputs[0].dims.length !== 4) {
    throw new TypeError('DepthToSpace input should be a 4-D numeric tensor');
  }
};
