// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramMetadata, TextureType } from '../types';

export const sum = (inferenceHandler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => {
  validateInputs(inputs);

  const sumProgramMetadata = {
    name: 'Sum',
    inputNames: inputs.map((_v, i) => `X${i}`),
    inputTypes: new Array(inputs.length).fill(TextureType.unpacked),
  };

  const output = inferenceHandler.run(
    { ...sumProgramMetadata, get: () => createSumProgramInfo(inferenceHandler, inputs, sumProgramMetadata) },
    inputs,
  );
  return [output];
};

const createSumProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  sumProgramMetadata: ProgramMetadata,
): ProgramInfo => {
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const outputShape = inputs[0].dims.slice();
  const sumLine = inputs.map((_v, i) => `${glsl.texture2D}(X${i},TexCoords)`).join(' + ');
  const shaderSource = `
      void main() {
        vec4 result = ${sumLine};
        ${glsl.output} = result;
      }
    `;
  return {
    ...sumProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    hasMain: true,
    shaderSource,
  };
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length === 0) {
    throw new Error('Sum requires inputs.');
  }

  const length = inputs[0].dims.length;
  for (let i = 1; i < inputs.length; i++) {
    if (length !== inputs[i].dims.length) {
      throw new Error('Input shapes are mismatched.');
    }

    for (let j = 0; j < length; j++) {
      if (inputs[0].dims[j] !== inputs[i].dims[j]) {
        throw new Error('Input shapes are not matched.');
      }
    }
  }

  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('Invalid input type.');
  }
  for (let i = 1; i < inputs.length; i++) {
    if (inputs[0].type !== inputs[i].type) {
      throw new Error('Input types are not matched.');
    }
  }
};
