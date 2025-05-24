// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Expand requires 2 input.');
  }
  const inputShape = inputs[0].dims;
  const shape = Array.from(inputs[1].getBigInt64Array(), Number);

  let shapeIndex = shape.length < inputShape.length ? 0 : shape.length - inputShape.length;
  let inputShapeIndex = inputShape.length < shape.length ? 0 : inputShape.length - shape.length;
  for (; shapeIndex < shape.length && inputShapeIndex < inputShape.length; ++shapeIndex, ++inputShapeIndex) {
    if (
      shape[shapeIndex] !== inputShape[inputShapeIndex] &&
      shape[shapeIndex] !== 1 &&
      inputShape[inputShapeIndex] !== 1
    ) {
      throw new Error('Expand requires shape to be broadcastable to input');
    }
  }
};

const getAdjustedShape = (shape1: readonly number[], shape2: readonly number[]): number[] => {
  const diff = shape1.length - shape2.length;
  const shape: number[] = [];
  for (let i = 0; i < diff; ++i) {
    shape.push(shape1[i]);
  }
  for (let i = 0; i < shape2.length; ++i) {
    shape.push(shape2[i] === 1 ? shape1[i + diff] : shape2[i]);
  }
  return shape;
};

const calculateOutputShape = (inputShape: readonly number[], shape: readonly number[]): number[] =>
  inputShape.length > shape.length ? getAdjustedShape(inputShape, shape) : getAdjustedShape(shape, inputShape);

const createExpandProgramInfo = (inputs: readonly TensorView[]): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const shape = Array.from(inputs[1].getBigInt64Array(), Number);
  const outputShape: number[] = calculateOutputShape(inputShape, shape);
  const dataType = inputs[0].dataType;
  const isBoolOrScalar = dataType === DataType.bool || ShapeUtil.size(inputShape) === 1;
  const iComponents =
    dataType === DataType.bool ? 4 : inputShape.length > 0 && inputShape[inputShape.length - 1] % 4 === 0 ? 4 : 1;
  const components = isBoolOrScalar
    ? 4
    : outputShape.length > 0 && outputShape[outputShape.length - 1] % 4 === 0
      ? 4
      : 1;
  const outputSize = Math.ceil(ShapeUtil.size(outputShape) / components);

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const input = inputVariable('input', dataType, inputShape.length, iComponents);
    const output = outputVariable('output', dataType, outputShape.length, components);
    let assignment: string;
    if (dataType === DataType.bool) {
      const singleAssignment = (resStr: string, x: number, typeCast = '') => `
          let outputIndices${x} = ${output.offsetToIndices(`outputOffset + ${x}u`)};
          let offset${x} = ${input.broadcastedIndicesToOffset(`outputIndices${x}`, output)};
          let index${x} = offset${x} / 4u;
          let component${x} = offset${x} % 4u;
          ${resStr}[${x}] = ${typeCast}(${input.getByOffset(`index${x}`)}[component${x}]);
        `;
      assignment = `
        let outputOffset = global_idx * ${components};
        var data = vec4<u32>(0);
        ${singleAssignment('data', 0, 'u32')}
        ${singleAssignment('data', 1, 'u32')}
        ${singleAssignment('data', 2, 'u32')}
        ${singleAssignment('data', 3, 'u32')}
        ${output.setByOffset('global_idx', 'data')}
      }`;
    } else {
      assignment = `
        let outputIndices = ${output.offsetToIndices(`global_idx * ${components}`)};
        let inputOffset = ${input.broadcastedIndicesToOffset('outputIndices', output)};
        let data = ${output.type.value}(${input.getByOffset(`inputOffset / ${iComponents}`)});
        ${output.setByOffset('global_idx', 'data')}
      }`;
    }
    return `
    ${shaderHelper.registerUniform('vec_size', 'u32').declareVariables(input, output)}
    ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.vec_size')}
    ${assignment}`;
  };

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    ...createTensorShapeVariables(inputShape, outputShape),
  ];
  return {
    name: 'Expand',
    shaderCache: { hint: `${outputShape.length};${iComponents}${components}`, inputDependencies: ['rank'] },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
  };
};

export const expand = (context: ComputeContext): void => {
  validateInputs(context.inputs);
  context.compute(createExpandProgramInfo(context.inputs), { inputs: [0] });
};
