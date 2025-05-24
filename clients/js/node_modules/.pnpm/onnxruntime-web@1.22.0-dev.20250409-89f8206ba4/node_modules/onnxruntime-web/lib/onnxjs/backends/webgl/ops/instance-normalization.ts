// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

export const instanceNormalization: OperatorImplementation<number> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  epsilon: number,
): Tensor[] => {
  validateInputs(inputs);

  const meanAndVariance = inferenceHandler.run(createMeanAndVarianceProgramInfoLoader(inputs[0]), inputs);
  const output = inferenceHandler.run(
    createComputeOutputProgramInfoLoader(inferenceHandler, inputs[0], epsilon, meanAndVariance.dims),
    [inputs[0], meanAndVariance, inputs[1], inputs[2]],
  );
  return [output];
};

export const parseInstanceNormalizationAttributes: OperatorInitialization<number> = (node: Graph.Node): number =>
  node.attributes.getFloat('epsilon', 1e-5);

const meanAndVarianceProgramMetadata = {
  name: 'InstanceNormalization_MeanAndVariance',
  inputNames: ['X'],
  inputTypes: [TextureType.unpacked],
};

const createMeanAndVarianceProgramInfo = (metadata: ProgramMetadata, input: Tensor): ProgramInfo => {
  const xDims = input.dims.slice();
  const channel = xDims[1];
  const channelSize = xDims[2] * xDims[3];
  const outputShape = [xDims[0], channel];

  const shaderSource = `
      vec4 process(int[2] indices) {
        vec4 v = vec4(0.0);
        int a[4];
        a[0] = indices[0];
        a[1] = indices[1];
        float temp = 0.0;
        for(int a2=0; a2<${xDims[2]}; a2++) {
          a[2] = a2;
          for(int a3=0; a3<${xDims[3]}; a3++) {
            a[3] = a3;
            float x = _X(a);
            temp += x;
          }
        }
        float mean = temp / float(${channelSize});
        temp = 0.0;
        for(int a2=0; a2<${xDims[2]}; a2++) {
          a[2] = a2;
          for(int a3=0; a3<${xDims[3]}; a3++) {
            a[3] = a3;
            float x = _X(a);
            temp += (x - mean) * (x - mean);
          }
        }
        v.r = mean;
        v.g = temp / float(${channelSize});

        return v;
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: input.type, textureType: TextureType.packedLastDimension },
    shaderSource,
  };
};

const createMeanAndVarianceProgramInfoLoader = (input: Tensor): ProgramInfoLoader => ({
  ...meanAndVarianceProgramMetadata,
  get: () => createMeanAndVarianceProgramInfo(meanAndVarianceProgramMetadata, input),
});

const computeOutputProgramMetadata = {
  name: 'InstanceNormalization_ComputeOutput',
  inputNames: ['X', 'MeanAndVariance', 'Scale', 'B'],
  inputTypes: [TextureType.unpacked, TextureType.packedLastDimension, TextureType.unpacked, TextureType.unpacked],
};

const createComputeOutputProgramInfo = (
  inferenceHandler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  input: Tensor,
  epsilon: number,
  meanAndVarianceShape: readonly number[],
): ProgramInfo => {
  const glsl = getGlsl(inferenceHandler.session.backend.glContext.version);
  const [textureWidth, textureHeight] = inferenceHandler.calculateTextureWidthAndHeight(
    meanAndVarianceShape,
    TextureType.packedLastDimension,
  );
  const [meanAndVarianceWidth, meanAndVarianceHeight] = [textureWidth / 4, textureHeight];
  const shaderSource = `
      vec4 get_MeanAndVariance(int[2] mv) {
        int offset = indicesToOffset_MeanAndVariance(mv);
        vec2 coords = offsetToCoords(offset, ${meanAndVarianceWidth}, ${meanAndVarianceHeight});
        return ${glsl.texture2D}(MeanAndVariance, coords);
      }

      float process(int[4] indices) {
        int mv[2];
        mv[0] = indices[0];
        mv[1] = indices[1];
        vec4 mean_and_variance = get_MeanAndVariance(mv);
        float mean = mean_and_variance.r;
        float variance = mean_and_variance.g;

        int sb[1];
        sb[0] = indices[1];
        float scale = _Scale(sb);
        float b = _B(sb);

        return scale * (_X(indices) - mean) / sqrt(variance + epsilon) + b;
      }`;
  return {
    ...metadata,
    output: { dims: input.dims, type: input.type, textureType: TextureType.unpacked },
    variables: [{ name: 'epsilon', type: 'float', data: epsilon }],
    shaderSource,
  };
};

const createComputeOutputProgramInfoLoader = (
  inferenceHandler: WebGLInferenceHandler,
  input: Tensor,
  epsilon: number,
  meanAndVarianceShape: readonly number[],
): ProgramInfoLoader => {
  const metadata = { ...computeOutputProgramMetadata, cacheHint: `${epsilon}` };
  return {
    ...metadata,
    get: () => createComputeOutputProgramInfo(inferenceHandler, metadata, input, epsilon, meanAndVarianceShape),
  };
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 3) {
    throw new Error('InstanceNormalization requires 3 inputs.');
  }

  const X = inputs[0];
  const scale = inputs[1];
  const B = inputs[2];

  // input should at least have three dimensions - N,C,dim1,...,dimn
  // other inputs can have only one dimensions
  if (X.dims.length < 3 || scale.dims.length !== 1 || B.dims.length !== 1) {
    throw new Error('Invalid input shape.');
  }
  if (scale.dims[0] !== X.dims[1] || B.dims[0] !== X.dims[1]) {
    throw new Error('Input shapes are mismatched.');
  }
  if (
    (X.type !== 'float32' && X.type !== 'float64') ||
    (scale.type !== 'float32' && scale.type !== 'float64') ||
    (B.type !== 'float32' && B.type !== 'float64')
  ) {
    throw new Error('Invalid input type.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('Only support 4-D input shape.');
  }
};
