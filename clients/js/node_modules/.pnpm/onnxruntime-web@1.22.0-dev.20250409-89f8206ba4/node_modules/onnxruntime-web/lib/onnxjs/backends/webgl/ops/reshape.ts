// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';

export const reshape = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => {
  const reshapedDims = ShapeUtil.calculateReshapedDims(inputs[0].dims, inputs[1].integerData);
  if (handler.session.pack) {
    return [handler.reshapePacked(inputs[0], reshapedDims)];
  } else {
    return [handler.reshapeUnpacked(inputs[0], reshapedDims)];
  }
};
