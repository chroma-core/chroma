// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

const getRepeats = (repeatsTensorView: TensorView): readonly number[] =>
  Array.from(repeatsTensorView.getBigInt64Array(), Number);

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Tile requires 2 inputs.');
  }

  if (
    inputs[0].dataType !== DataType.float &&
    inputs[0].dataType !== DataType.float16 &&
    inputs[0].dataType !== DataType.int32 &&
    inputs[0].dataType !== DataType.uint32
  ) {
    throw new Error('Tile only support float, float16, int32, and uint32 data types');
  }

  if (inputs[1].dataType !== DataType.int64) {
    throw new Error('Tile `repeats` input should be of int64 data type');
  }

  if (inputs[1].dims.length !== 1) {
    throw new Error('Tile `repeats` input should be 1-D');
  }

  const repeats: readonly number[] = getRepeats(inputs[1]);

  if (repeats.length !== inputs[0].dims.length) {
    throw new Error('Tile `repeats` input should have same number of elements as rank of input data tensor');
  }
};

const getOutputShape = (inputShape: readonly number[], repeats: readonly number[]): readonly number[] => {
  const outputShape: number[] = [];

  for (let i = 0; i < inputShape.length; ++i) {
    outputShape.push(inputShape[i] * repeats[i]);
  }

  return outputShape;
};

export const createTileProgramInfo = (inputs: readonly TensorView[], shape?: number[]): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const repeats: readonly number[] = shape == null ? getRepeats(inputs[1]) : shape;
  const outputShape = getOutputShape(inputShape, repeats);
  const outputSize = ShapeUtil.size(outputShape);

  const dataType = inputs[0].dataType;
  const input = inputVariable('input', dataType, inputShape.length);
  const output = outputVariable('output', dataType, outputShape.length);

  const getShaderSource = (shaderHelper: ShaderHelper) => `
      const inputShape = ${input.indices(...inputShape)};
      ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(input, output)}
      ${shaderHelper.mainStart()}
      ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
      let output_indices = ${output.offsetToIndices('global_idx')};
      var input_indices: ${input.type.indices};
      for (var i = 0; i < ${inputShape.length}; i++) {
        let input_dim_i = ${input.indicesGet('uniforms.input_shape', 'i')};
        let input_dim_value = ${output.indicesGet('output_indices', 'i')}  % input_dim_i;

        ${input.indicesSet('input_indices', 'i', 'input_dim_value')}
      }
      ${output.setByOffset('global_idx', input.getByIndices('input_indices'))}
    }`;

  return {
    name: 'Tile',
    shaderCache: { hint: `${repeats}`, inputDependencies: ['rank'] },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms: [
        { type: DataType.uint32, data: outputSize },
        ...createTensorShapeVariables(inputs[0].dims, outputShape),
      ],
    }),
    getShaderSource,
  };
};

export const tile = (context: ComputeContext): void => {
  validateInputs(context.inputs);
  context.compute(createTileProgramInfo(context.inputs), { inputs: [0] });
};
