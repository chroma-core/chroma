// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

export interface ImageScalerAttributes extends AttributeWithCacheKey {
  scale: number;
  bias: number[];
}

export const imageScaler: OperatorImplementation<ImageScalerAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ImageScalerAttributes,
): Tensor[] => {
  validateInputs(inputs);
  const output = inferenceHandler.run(createImageScalerProgramInfoLoader(inferenceHandler, inputs, attributes), inputs);
  return [output];
};

export const parseImageScalerAttributes: OperatorInitialization<ImageScalerAttributes> = (
  node: Graph.Node,
): ImageScalerAttributes => {
  const scale = node.attributes.getFloat('scale');
  const bias = node.attributes.getFloats('bias');
  return createAttributeWithCacheKey({ scale, bias });
};

const imageScalerProgramMetadata = {
  name: 'ImageScaler',
  inputNames: ['X'],
  inputTypes: [TextureType.unpacked],
};

const createImageScalerProgramInfo = (
  _handler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  inputs: Tensor[],
  attributes: ImageScalerAttributes,
): ProgramInfo => {
  const outputShape = inputs[0].dims.slice();
  const rank = outputShape.length;
  const getBiasMethod = createGetBiasMethod(attributes.bias.length);
  const shaderSource = `
      ${getBiasMethod}
      float process(int indices[${rank}]) {
        return _X(indices) * scale + getBias(bias, indices[1]);
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    variables: [
      { name: 'bias', type: 'float', arrayLength: attributes.bias.length, data: attributes.bias },
      { name: 'scale', type: 'float', data: attributes.scale },
    ],
    shaderSource,
  };
};

const createImageScalerProgramInfoLoader = (
  handler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ImageScalerAttributes,
): ProgramInfoLoader => {
  const metadata = { ...imageScalerProgramMetadata, cacheHint: attributes.cacheKey };
  return { ...metadata, get: () => createImageScalerProgramInfo(handler, metadata, inputs, attributes) };
};

const createGetBiasMethod = (numChannels: number): string => {
  const codeLines: string[] = [`float getBias(float bias[${numChannels}], int channel) {`];
  for (let i = 0; i < numChannels; ++i) {
    if (i === 0) {
      codeLines.push('\t' + `if (channel == ${i}) { return bias[${i}]; }`);
    } else if (i === numChannels - 1) {
      codeLines.push('\t' + `else { return bias[${i}]; }`);
    } else {
      codeLines.push('\t' + `else if (channel == ${i}) { return bias[${i}]; }`);
    }
  }
  codeLines.push('\t' + '}');
  return codeLines.join('\n');
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('ImageScaler requires 1 input.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('Invalid input shape.');
  }
  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
};
