// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';

export const shape = (_inferenceHandler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => {
  validateInputs(inputs);
  return [new Tensor([inputs[0].dims.length], 'int32', undefined, undefined, new Int32Array(inputs[0].dims))];
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Shape requires 1 input.');
  }
};
