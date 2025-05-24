// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';

export const flatten: OperatorImplementation<number> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  axis: number,
): Tensor[] => {
  validateInputs(inputs, axis);

  const outputDims = ShapeUtil.flattenShape(inputs[0].dims, axis);
  return [inferenceHandler.reshapeUnpacked(inputs[0], outputDims)];
};

export const parseFlattenAttributes: OperatorInitialization<number> = (node: Graph.Node): number =>
  node.attributes.getInt('axis', 1); // default axis is 1

const validateInputs = (inputs: Tensor[], axis: number): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Flatten requires 1 input.');
  }

  const r = inputs[0].dims.length;
  if (r === 0) {
    throw new Error('scalar tensor is not supported.');
  }

  if (axis < -r || axis > r) {
    throw new Error('Invalid axis');
  }

  // TODO: Support string type
  if (inputs[0].type === 'string') {
    throw new Error('string tensor is not supported.');
  }
};
