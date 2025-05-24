// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from 'onnxruntime-common';

import { DataType } from '../../../wasm-common';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  outputVariable,
  ShaderHelper,
  UniformDataElementType,
  UniformsArrayType,
} from './common';

const validateInputsContent = (start: number, limit: number, delta: number): void => {
  const sameStartLimit = start === limit;
  const increasingRangeNegativeStep = start < limit && delta < 0;
  const decreasingRangePositiveStep = start > limit && delta > 0;

  if (sameStartLimit || increasingRangeNegativeStep || decreasingRangePositiveStep) {
    throw new Error("Range these inputs' contents are invalid.");
  }
};

const createRangeProgramInfo = (start: number, limit: number, delta: number, dataType: DataType): ProgramInfo => {
  const numElements = Math.abs(Math.ceil((limit - start) / delta));
  const outputShape: number[] = [numElements];
  const outputSize = numElements;
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: dataType, data: start },
    { type: dataType, data: delta },
    ...createTensorShapeVariables(outputShape),
  ];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const output = outputVariable('output', dataType, outputShape.length);
    const wgslType = output.type.value;
    const uniforms: UniformsArrayType = [
      { name: 'outputSize', type: 'u32' },
      { name: 'start', type: wgslType as UniformDataElementType },
      { name: 'delta', type: wgslType as UniformDataElementType },
    ];
    return `
        ${shaderHelper.registerUniforms(uniforms).declareVariables(output)}
        ${shaderHelper.mainStart()}
        ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
        output[global_idx] = uniforms.start + ${wgslType}(global_idx) * uniforms.delta;
      }`;
  };

  return {
    name: 'Range',
    shaderCache: { hint: `${dataType}` },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
  };
};

export const range = (context: ComputeContext): void => {
  let start = 0;
  let limit = 0;
  let delta = 0;
  if (context.inputs[0].dataType === DataType.int32) {
    start = context.inputs[0].getInt32Array()[0];
    limit = context.inputs[1].getInt32Array()[0];
    delta = context.inputs[2].getInt32Array()[0];
  } else if (context.inputs[0].dataType === DataType.float) {
    start = context.inputs[0].getFloat32Array()[0];
    limit = context.inputs[1].getFloat32Array()[0];
    delta = context.inputs[2].getFloat32Array()[0];
  }
  if (env.webgpu.validateInputContent) {
    validateInputsContent(start, limit, delta);
  }

  context.compute(createRangeProgramInfo(start, limit, delta, context.inputs[0].dataType), { inputs: [] });
};
