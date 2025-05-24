// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { BroadcastUtil, ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

const createWhereOpProgramShader = (
  shaderHelper: ShaderHelper,
  inputs: readonly TensorView[],
  dimsOutput: readonly number[],
  isBroadcast: boolean,
  typeOutput: number,
) => {
  const output = outputVariable('output_data', typeOutput, dimsOutput.length, 4);
  const a = inputVariable('a_data', inputs[1].dataType, inputs[1].dims.length, 4);
  const b = inputVariable('b_data', inputs[2].dataType, inputs[2].dims.length, 4);
  const c = inputVariable('c_data', inputs[0].dataType, inputs[0].dims.length, 4);

  let assignment: string;
  const expression = (a: string, b: string, c: string) => `select(${b}, ${a}, ${c})`;
  if (!isBroadcast) {
    assignment = output.setByOffset(
      'global_idx',
      expression(a.getByOffset('global_idx'), b.getByOffset('global_idx'), c.getByOffset('global_idx')),
    );
  } else {
    const singleAssignment = (resStr: string, x: number, typeCast = '') => {
      const expressionA = `a_data[index_a${x}][component_a${x}]`;
      const expressionB = `b_data[index_b${x}][component_b${x}]`;
      // eslint-disable-next-line no-bitwise
      const expressionC = `bool(c_data[index_c${x}] & (0xffu << (component_c${x} * 8)))`;
      return `
            let output_indices${x} = ${output.offsetToIndices(`global_idx * 4u + ${x}u`)};
            let offset_a${x} = ${a.broadcastedIndicesToOffset(`output_indices${x}`, output)};
            let offset_b${x} = ${b.broadcastedIndicesToOffset(`output_indices${x}`, output)};
            let offset_c${x} = ${c.broadcastedIndicesToOffset(`output_indices${x}`, output)};
            let index_a${x} = offset_a${x} / 4u;
            let index_b${x} = offset_b${x} / 4u;
            let index_c${x} = offset_c${x} / 4u;
            let component_a${x} = offset_a${x} % 4u;
            let component_b${x} = offset_b${x} % 4u;
            let component_c${x} = offset_c${x} % 4u;
            ${resStr}[${x}] = ${typeCast}(${expression(expressionA, expressionB, expressionC)});
          `;
    };
    if (typeOutput === DataType.bool) {
      assignment = `
            var data = vec4<u32>(0);
            ${singleAssignment('data', 0, 'u32')}
            ${singleAssignment('data', 1, 'u32')}
            ${singleAssignment('data', 2, 'u32')}
            ${singleAssignment('data', 3, 'u32')}
            output_data[global_idx] = dot(vec4<u32>(0x1, 0x100, 0x10000, 0x1000000), vec4<u32>(data));`;
    } else {
      assignment = `
            ${singleAssignment('output_data[global_idx]', 0)}
            ${singleAssignment('output_data[global_idx]', 1)}
            ${singleAssignment('output_data[global_idx]', 2)}
            ${singleAssignment('output_data[global_idx]', 3)}
          `;
    }
  }

  return `
        ${shaderHelper.registerUniform('vec_size', 'u32').declareVariables(c, a, b, output)}
        ${shaderHelper.mainStart()}
        ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.vec_size')}
        ${assignment}
      }`;
};

const createWhereOpProgramInfo = (inputs: readonly TensorView[]): ProgramInfo => {
  const dimsA = inputs[1].dims;
  const dimsB = inputs[2].dims;
  const dimsC = inputs[0].dims;
  const outputDataType = inputs[1].dataType;

  const isBroadcast = !(ShapeUtil.areEqual(dimsA, dimsB) && ShapeUtil.areEqual(dimsB, dimsC));
  let outputShape = dimsA;
  let outputSize = ShapeUtil.size(dimsA);
  // TODO: deal with zero-sized tensors (eg. dims=[1,0])

  if (isBroadcast) {
    const calculatedShape = BroadcastUtil.calcShape(BroadcastUtil.calcShape(dimsA, dimsB, false)!, dimsC, false);
    if (!calculatedShape) {
      throw new Error("Can't perform where op on the given tensors");
    }
    outputShape = calculatedShape;
    outputSize = ShapeUtil.size(outputShape);
  }

  const vecSize = Math.ceil(outputSize / 4);

  return {
    name: 'Where',
    shaderCache: { inputDependencies: ['rank', 'rank', 'rank'] },
    getShaderSource: (shaderHelper) =>
      createWhereOpProgramShader(shaderHelper, inputs, outputShape, isBroadcast, outputDataType),
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: outputDataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */ / 4 /* vec size */) },
      programUniforms: [
        { type: DataType.uint32, data: vecSize },
        ...createTensorShapeVariables(dimsC, dimsA, dimsB, outputShape),
      ],
    }),
  };
};

export const where = (context: ComputeContext): void => {
  context.compute(createWhereOpProgramInfo(context.inputs));
};
