// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, TextureType } from '../types';

export interface LrnAttributes extends AttributeWithCacheKey {
  alpha: number;
  beta: number;
  bias: number;
  size: number;
}

export const lrn: OperatorImplementation<LrnAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: LrnAttributes,
): Tensor[] => {
  validateInputs(inputs);

  // if (inferenceHandler.session.pack) {
  //   return [inferenceHandler.run(createPackedLrnProgramInfoLoader(inferenceHandler, inputs, attributes),
  //   inputs)];
  // } else {
  return [inferenceHandler.run(createLrnProgramInfoLoader(inputs, attributes), inputs)];
  //}
};

export const parseLrnAttributes: OperatorInitialization<LrnAttributes> = (node: Graph.Node): LrnAttributes => {
  const alpha = node.attributes.getFloat('alpha', 0.0001);
  const beta = node.attributes.getFloat('beta', 0.75);
  const bias = node.attributes.getFloat('bias', 1.0);
  const size = node.attributes.getInt('size');

  return createAttributeWithCacheKey({ alpha, beta, bias, size });
};

const lrnProgramMetadata = {
  name: 'LRN',
  inputNames: ['X'],
  inputTypes: [TextureType.unpacked],
};

function createLrnProgramInfo(inputs: Tensor[], attributes: LrnAttributes): ProgramInfo {
  const C = inputs[0].dims[1];
  const rank = inputs[0].dims.length;
  const from = -Math.floor((attributes.size - 1) / 2);
  const to = Math.ceil((attributes.size - 1) / 2);
  const alpha = `float(${attributes.alpha}) / float(${attributes.size})`;
  const bias = `float(${attributes.bias})`;
  const beta = `float(${attributes.beta})`;

  const shaderSource = `
    float process(int indices[${rank}]) {
        int c = indices[1];
        float x = _X(indices);
        float square_sum = 0.0;

        for (int i = ${from}; i <= ${to}; i++) {
          int idx = c + i;
          if (c >= 0 && c < ${C}) {
            indices[1] = idx;
            float j = _X(indices);
            square_sum += j * j;
          }
        }
        return x / pow(${bias} + ${alpha} * square_sum, ${beta});
    }`;
  return {
    ...lrnProgramMetadata,
    cacheHint: attributes.cacheKey,
    output: { dims: inputs[0].dims, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
  };
}

export function createLrnProgramInfoLoader(inputs: Tensor[], attributes: LrnAttributes): ProgramInfoLoader {
  return { ...lrnProgramMetadata, cacheHint: attributes.cacheKey, get: () => createLrnProgramInfo(inputs, attributes) };
}

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('LRN requires 1 input.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('currently only support LRN for input with "NCHW" format');
  }
  if (inputs[0].type !== 'float32') {
    throw new Error('input should be float type');
  }
};
