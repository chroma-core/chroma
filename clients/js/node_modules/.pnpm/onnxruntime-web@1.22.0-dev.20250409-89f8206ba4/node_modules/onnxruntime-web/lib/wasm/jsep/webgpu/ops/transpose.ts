// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, IndicesHelper, inputVariable, outputVariable, ShaderHelper } from './common';

export interface TransposeAttributes extends AttributeWithCacheKey {
  readonly perm: number[];
}

const validateInputs = (inputs: readonly TensorView[], perm: readonly number[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Transpose requires 1 input.');
  }

  if (perm.length !== 0 && perm.length !== inputs[0].dims.length) {
    throw new Error(`perm size ${perm.length} does not match input rank ${inputs[0].dims.length}`);
  }
};

const getAdjustedPerm = (inputRank: number, perm: number[]): number[] =>
  perm.length !== 0 ? perm : [...new Array(inputRank).keys()].reverse();

const getOutputShape = (inputShape: readonly number[], perm: number[]): readonly number[] =>
  ShapeUtil.sortBasedOnPerm(inputShape, getAdjustedPerm(inputShape.length, perm));

const permFunctionBody = (perm: number[], rank: number, input: IndicesHelper, output: IndicesHelper): string => {
  let reverseFunc = `fn perm(i: ${output.type.indices}) -> ${input.type.indices} {
    var a: ${input.type.indices};`;
  for (let i = 0; i < rank; ++i) {
    // input indices and output indices should always be larger or equal to 2,
    // so indexer is always valid to be used on `a` and `i`.
    reverseFunc += `a[${perm[i]}]=i[${i}];`;
  }
  return (reverseFunc += 'return a;}');
};

const squeezeShape = (shape: readonly number[], adjustedPerm: number[]): { newShape: number[]; newPerm: number[] } => {
  const newShape: number[] = [];
  const newPerm: number[] = [];
  for (let i = 0; i < shape.length; ++i) {
    if (shape[i] !== 1) {
      newShape.push(shape[i]);
    }
    if (shape[adjustedPerm[i]] !== 1) {
      newPerm.push(adjustedPerm[i]);
    }
  }
  return { newShape, newPerm };
};

const isTransposeReshape = (perm: number[], shape: readonly number[]) => {
  // As long as the dims with values > 1 stay in the same order, it's a reshape.
  // Example: Shape=(1,1,1024,4096) -> perm=(2,0,3,1).
  let lastPermutedAxis = 0;
  for (let i = 0; i < perm.length; ++i) {
    if (shape[perm[i]] === 1) {
      continue;
    }
    if (perm[i] < lastPermutedAxis) {
      return false;
    }
    lastPermutedAxis = perm[i];
  }
  return true;
};

export const createTransposeProgramInfo = (inputTensor: TensorView, permAttr: number[]): ProgramInfo => {
  const inputDataType = inputTensor.dataType;
  const inputRank = inputTensor.dims.length;
  const perm = getAdjustedPerm(inputRank, permAttr);
  const outputShape = getOutputShape(inputTensor.dims, perm);
  let newInputShape = inputTensor.dims;
  let newOutputShape = outputShape;
  const transposeAsReshape = inputRank < 2 || isTransposeReshape(perm, inputTensor.dims);
  let getShaderSource;
  if (transposeAsReshape) {
    getShaderSource = (shaderHelper: ShaderHelper) => {
      const input = inputVariable('input', inputDataType, newInputShape, 4);
      const output = outputVariable('output', inputDataType, newOutputShape, 4);
      return `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(input, output)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
    output[global_idx] = input[global_idx];
  }`;
    };

    return {
      name: 'TransposeCopy',
      shaderCache: { inputDependencies: ['type'] },
      getRunData: () => {
        const outputSize = ShapeUtil.size(outputShape);
        return {
          outputs: [{ dims: outputShape, dataType: inputTensor.dataType }],
          dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */ / 4 /* components */) },
          programUniforms: [{ type: DataType.uint32, data: Math.ceil(outputSize / 4) }],
        };
      },
      getShaderSource,
    };
  }
  const { newShape, newPerm } = squeezeShape(inputTensor.dims, perm);
  const channelsLast = ShapeUtil.areEqual(newPerm, [2, 3, 1]);
  const channelsFirst = ShapeUtil.areEqual(newPerm, [3, 1, 2]);
  const useShared = newShape.length === 2 || channelsLast || channelsFirst;
  if (useShared) {
    newInputShape = channelsLast
      ? [newShape[0], newShape[1] * newShape[2]]
      : channelsFirst
        ? [newShape[0] * newShape[1], newShape[2]]
        : newShape;
    newOutputShape = [newInputShape[1], newInputShape[0]];
    const tileSize = 16;
    getShaderSource = (shaderHelper: ShaderHelper) => {
      const input = inputVariable('a', inputDataType, newInputShape.length);
      const output = outputVariable('output', inputDataType, newOutputShape.length);
      return `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(input, output)}
  var<workgroup> tile : array<array<${output.type.value}, ${tileSize + 1}>, ${tileSize}>;
  ${shaderHelper.mainStart([tileSize, tileSize, 1])}
    let stride = (uniforms.output_shape[1] - 1) / ${tileSize} + 1;
    let workgroup_id_x = workgroup_index % stride;
    let workgroup_id_y = workgroup_index / stride;
    let input_col = workgroup_id_y * ${tileSize}u + local_id.x;
    let input_row = workgroup_id_x * ${tileSize}u + local_id.y;
    if (input_row < uniforms.a_shape[0] && input_col < uniforms.a_shape[1]) {
      tile[local_id.y][local_id.x] = ${input.getByIndices(`${input.type.indices}(input_row, input_col)`)};
    }
    workgroupBarrier();

    let output_col = workgroup_id_x * ${tileSize}u + local_id.x;
    let output_row = workgroup_id_y * ${tileSize}u + local_id.y;
    if (output_row < uniforms.output_shape[0] && output_col < uniforms.output_shape[1]) {
      ${output.setByIndices(`${output.type.indices}(output_row, output_col)`, 'tile[local_id.x][local_id.y]')}
    }
  }`;
    };
    return {
      name: 'TransposeShared',
      shaderCache: { inputDependencies: ['type'] },
      getRunData: () => {
        const outputSize = ShapeUtil.size(outputShape);
        return {
          outputs: [{ dims: outputShape, dataType: inputTensor.dataType }],
          dispatchGroup: { x: Math.ceil(newOutputShape[1] / tileSize), y: Math.ceil(newOutputShape[0] / tileSize) },
          programUniforms: [
            { type: DataType.uint32, data: outputSize },
            ...createTensorShapeVariables(newInputShape, newOutputShape),
          ],
        };
      },
      getShaderSource,
    };
  }

  getShaderSource = (shaderHelper: ShaderHelper) => {
    const input = inputVariable('a', inputDataType, newInputShape.length);
    const output = outputVariable('output', inputDataType, newOutputShape.length);
    return `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(input, output)}

  ${permFunctionBody(perm, inputRank, input, output)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}

    let indices = ${output.offsetToIndices('global_idx')};
    let aIndices = perm(indices);

    ${output.setByOffset('global_idx', input.getByIndices('aIndices'))}
  }`;
  };
  return {
    name: 'Transpose',
    shaderCache: { hint: `${permAttr}`, inputDependencies: ['rank'] },
    getRunData: () => {
      const outputSize = ShapeUtil.size(outputShape);
      return {
        outputs: [{ dims: outputShape, dataType: inputTensor.dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms: [
          { type: DataType.uint32, data: outputSize },
          ...createTensorShapeVariables(newInputShape, newOutputShape),
        ],
      };
    },
    getShaderSource,
  };
};

export const transpose = (context: ComputeContext, attributes: TransposeAttributes): void => {
  validateInputs(context.inputs, attributes.perm);
  context.compute(createTransposeProgramInfo(context.inputs[0], attributes.perm));
};

export const parseTransposeAttributes = (attributes: Record<string, unknown>): TransposeAttributes =>
  createAttributeWithCacheKey({ perm: attributes.perm as number[] });
