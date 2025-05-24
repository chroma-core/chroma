// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

export interface GatherAttributes extends AttributeWithCacheKey {
  axis: number;
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 2) {
    throw new Error('Gather requires 2 inputs.');
  }
};

const createGatherProgramInfo = (inputs: readonly TensorView[], attributes: GatherAttributes): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const indicesShape = inputs[1].dims;

  const inputRank = inputShape.length;
  const axis = ShapeUtil.normalizeAxis(attributes.axis, inputRank);

  const outputShape = inputShape.slice(0);
  outputShape.splice(axis, 1, ...indicesShape);

  const axisDimLimit = inputShape[axis];
  const components = inputs[0].dataType === DataType.bool ? 4 : 1;
  const outputSize = Math.ceil(ShapeUtil.size(outputShape) / components);

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.int32, data: axisDimLimit },
    { type: DataType.uint32, data: axis },
    ...createTensorShapeVariables(inputs[0].dims, inputs[1].dims, outputShape),
  ];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const data = inputVariable('data', inputs[0].dataType, inputs[0].dims.length, components);
    const indices = inputVariable('inputIndices', inputs[1].dataType, inputs[1].dims.length);
    const output = outputVariable('output', inputs[0].dataType, outputShape.length, components);

    const calcDataIndices = (x: number | string): string => {
      const indicesRank = indicesShape.length;
      let calcStr = `var indicesIndices${x}  = ${indices.type.indices}(0);`;
      for (let i = 0; i < indicesRank; i++) {
        calcStr += `${indicesRank > 1 ? `indicesIndices${x}[${i}]` : `indicesIndices${x}`} = ${
          outputShape.length > 1 ? `outputIndices${x}[uniforms.axis + ${i}]` : `outputIndices${x}`
        };`;
      }
      calcStr += `
          var idx${x} = ${indices.getByIndices(`indicesIndices${x}`)};
          if (idx${x} < 0) {
            idx${x} = idx${x} + uniforms.axisDimLimit;
          }
          var dataIndices${x} : ${data.type.indices};
        `;
      for (let i = 0, j = 0; i < inputRank; i++) {
        if (i === axis) {
          calcStr += `${inputRank > 1 ? `dataIndices${x}[${i}]` : `dataIndices${x}`} = u32(idx${x});`;
          j += indicesRank;
        } else {
          calcStr += `${inputRank > 1 ? `dataIndices${x}[${i}]` : `dataIndices${x}`} = ${
            outputShape.length > 1 ? `outputIndices${x}[${j}]` : `outputIndices${x}`
          };`;
          j++;
        }
      }
      return calcStr;
    };
    let assignment: string;
    if (inputs[0].dataType === DataType.bool) {
      const singleAssignment = (resStr: string, x: number, typeCast = '') => `
          let outputIndices${x} = ${output.offsetToIndices(`outputOffset + ${x}u`)};
          ${calcDataIndices(x)};
          let offset${x} = ${data.indicesToOffset(`dataIndices${x}`)};
          let index${x} = offset${x} / 4u;
          let component${x} = offset${x} % 4u;
          ${resStr}[${x}] = ${typeCast}(${data.getByOffset(`index${x}`)}[component${x}]);
        `;
      assignment = `
        let outputOffset = global_idx * ${components};
        var value = vec4<u32>(0);
        ${singleAssignment('value', 0, 'u32')}
        ${singleAssignment('value', 1, 'u32')}
        ${singleAssignment('value', 2, 'u32')}
        ${singleAssignment('value', 3, 'u32')}
        ${output.setByOffset('global_idx', 'value')}
      `;
    } else {
      assignment = `
      let outputIndices = ${output.offsetToIndices('global_idx')};
      ${calcDataIndices('')};
      let value = ${data.getByIndices('dataIndices')};
      ${output.setByOffset('global_idx', 'value')};
      `;
    }
    return `
      ${shaderHelper
        .registerUniform('outputSize', 'u32')
        .registerUniform('axisDimLimit', 'i32')
        .registerUniform('axis', 'u32')
        .declareVariables(data, indices, output)}
      ${shaderHelper.mainStart()}
        ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
        ${assignment}
      }`;
  };
  return {
    name: 'Gather',
    shaderCache: { hint: attributes.cacheKey, inputDependencies: ['rank', 'rank'] },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const parseGatherAttributes = (attributes: Record<string, unknown>): GatherAttributes =>
  createAttributeWithCacheKey({ axis: attributes.axis as number });

export const gather = (context: ComputeContext, attributes: GatherAttributes): void => {
  const inputs = context.inputs;
  validateInputs(inputs);
  context.compute(createGatherProgramInfo(context.inputs, attributes));
};
