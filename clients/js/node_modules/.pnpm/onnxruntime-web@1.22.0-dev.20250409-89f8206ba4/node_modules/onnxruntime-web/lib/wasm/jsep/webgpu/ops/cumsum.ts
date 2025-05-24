// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, getElementAt, inputVariable, outputVariable, ShaderHelper } from './common';

export interface CumSumAttributes extends AttributeWithCacheKey {
  readonly exclusive: boolean;
  readonly reverse: boolean;
}
const createCumsumProgramInfo = (
  inputType: number,
  inputShape: readonly number[],
  axisInput: TensorView,
  attributes: CumSumAttributes,
): ProgramInfo => {
  const outputSize = ShapeUtil.size(inputShape); // outputShape is same as inputShape.
  const rank = inputShape.length; // input/output rank
  const input = inputVariable('input', inputType, rank);
  const output = outputVariable('output', inputType, rank);
  const axisValue =
    axisInput.dataType === DataType.int32 ? axisInput.getInt32Array()[0] : Number(axisInput.getBigInt64Array()[0]);
  const axis = ShapeUtil.normalizeAxis(axisValue, rank);
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const index = ` i32(${input.indicesGet('inputIndices', 'uniforms.axis')}) `;
    const max = getElementAt('uniforms.input_shape', 'uniforms.axis', rank);
    const lowerLimit = attributes.reverse ? index + (attributes.exclusive ? ' + 1' : '') : '0';
    const upperLimit = attributes.reverse ? max : index + (attributes.exclusive ? '' : ' + 1');
    return `
                ${shaderHelper
                  .registerUniform('outputSize', 'u32')
                  .registerUniform('axis', 'u32')
                  .declareVariables(input, output)}
                ${shaderHelper.mainStart()}
                  ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
                  var inputIndices = ${output.offsetToIndices('global_idx')};
                  var sum = ${output.type.value}(0);
                  let first : i32 = ${lowerLimit};
                  let last : i32 = ${upperLimit};
                  for (var i : i32 = first; i < last; i++) {
                    ${input.indicesSet('inputIndices', 'uniforms.axis', 'u32(i)')};
                    sum = sum + ${input.getByIndices('inputIndices')};
                  }
                  ${output.setByOffset('global_idx', 'sum')};
                }`;
  };
  return {
    name: 'CumSum',
    shaderCache: { hint: attributes.cacheKey, inputDependencies: ['rank'] },
    getRunData: () => ({
      outputs: [{ dims: inputShape, dataType: inputType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms: [
        { type: DataType.uint32, data: outputSize },
        { type: DataType.uint32, data: axis },
        ...createTensorShapeVariables(inputShape, inputShape),
      ],
    }),
    getShaderSource,
  };
};

export const cumsum = (context: ComputeContext, attributes: CumSumAttributes): void => {
  const inputShape = context.inputs[0].dims;
  const inputType = context.inputs[0].dataType;
  const axis = context.inputs[1];
  context.compute(createCumsumProgramInfo(inputType, inputShape, axis, attributes), { inputs: [0] });
};

export const parseCumSumAttributes = (attributes: Record<string, unknown>): CumSumAttributes => {
  const exclusive = (attributes.exclusive as number) === 1;
  const reverse = (attributes.reverse as number) === 1;
  return createAttributeWithCacheKey({ exclusive, reverse });
};
