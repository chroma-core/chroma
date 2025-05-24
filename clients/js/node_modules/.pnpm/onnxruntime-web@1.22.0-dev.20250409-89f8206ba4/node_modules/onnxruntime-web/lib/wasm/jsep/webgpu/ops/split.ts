// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform, TensorInfo } from '../types';

import {
  createTensorShapeVariables,
  getElementAt,
  IndicesHelper,
  inputVariable,
  outputVariable,
  ShaderHelper,
} from './common';

export interface SplitAttributes extends AttributeWithCacheKey {
  readonly axis: number;
  readonly numOutputs: number;
  readonly splitSizes: number[];
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length < 1) {
    throw new Error('too few inputs');
  }
};

const createSplitAttributesFromInputs = (
  inputs: readonly TensorView[],
  attributes: SplitAttributes,
): SplitAttributes => {
  const splitSizes: number[] = [];
  let numOutputs: number = attributes.numOutputs;
  if (inputs[1].dims[0] > 0) {
    inputs[1].getBigInt64Array().forEach((v) => splitSizes.push(Number(v)));
    numOutputs = splitSizes.length;
  }
  return createAttributeWithCacheKey({ numOutputs, axis: attributes.axis, splitSizes });
};

const calculateOutputIndexImpl = (numberOfTensors: number): string => `
fn calculateOutputIndex(index: u32) -> u32 {
    for (var i: u32 = 0u; i < ${numberOfTensors}u; i += 1u ) {
    if (index < ${getElementAt('uniforms.size_in_split_axis', 'i', numberOfTensors)}) {
        return i;
    }
    }
    return ${numberOfTensors}u;
}`;
const writeBufferDataImpl = (outputs: readonly IndicesHelper[]) => {
  const numberOfTensors = outputs.length;
  const codeLines: string[] = [];
  for (let i = 0; i < numberOfTensors; ++i) {
    const returnSnippet = outputs[i].setByIndices('indices', 'input[global_idx]');
    if (numberOfTensors === 1) {
      codeLines.push(returnSnippet);
    } else if (i === 0) {
      codeLines.push(`if (output_number == ${i}u) { ${returnSnippet} }`);
    } else if (i === numberOfTensors - 1) {
      codeLines.push(`else { ${returnSnippet} }`);
    } else {
      codeLines.push(`else if (output_number == ${i}) { ${returnSnippet} }`);
    }
  }
  return `
      fn writeBufferData(output_number: u32, indices: ${outputs[0].type.indices}, global_idx: u32) {
        ${codeLines.join('\n')}
      }`;
};

export const createSplitProgramInfo = (inputs: readonly TensorView[], attributes: SplitAttributes): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const inputSize = ShapeUtil.size(inputShape);
  const dataType = inputs[0].dataType;
  const axis = ShapeUtil.normalizeAxis(attributes.axis, inputShape.length);
  const outputs = new Array<IndicesHelper>(attributes.numOutputs);
  const input = inputVariable('input', dataType, inputShape.length);
  const sizeInSplitAxis = new Array<number>(attributes.numOutputs);
  const outputsTensorInfo: TensorInfo[] = [];
  const outputShapes: number[][] = [];
  let previousSum = 0;
  const programUniforms: ProgramUniform[] = [{ type: DataType.uint32, data: inputSize }];
  for (let i = 0; i < attributes.numOutputs; i++) {
    previousSum += attributes.splitSizes[i];
    sizeInSplitAxis[i] = previousSum;
    const outputShape = inputShape.slice();
    outputShape[axis] = attributes.splitSizes[i];
    outputShapes.push(outputShape);
    outputs[i] = outputVariable(`output${i}`, dataType, outputShape.length);
    outputsTensorInfo.push({ dims: outputShapes[i], dataType: inputs[0].dataType });
  }
  programUniforms.push(
    { type: DataType.uint32, data: sizeInSplitAxis },
    ...createTensorShapeVariables(inputShape, ...outputShapes),
  );
  const getShaderSource = (shaderHelper: ShaderHelper) => `
  ${shaderHelper
    .registerUniform('input_size', 'u32')
    .registerUniform('size_in_split_axis', 'u32', sizeInSplitAxis.length)
    .declareVariables(input, ...outputs)}
  ${calculateOutputIndexImpl(sizeInSplitAxis.length)}
  ${writeBufferDataImpl(outputs)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.input_size')}

    var indices = ${input.offsetToIndices('global_idx')};
    var index = ${input.indicesGet('indices', axis)};
    let output_number = calculateOutputIndex(index);
    if (output_number != 0) {
      index -= ${getElementAt('uniforms.size_in_split_axis', 'output_number - 1u', sizeInSplitAxis.length)};
      ${input.indicesSet('indices', axis, 'index')};
    }
    writeBufferData(output_number, indices, global_idx);
  }`;
  return {
    name: 'Split',
    shaderCache: { hint: attributes.cacheKey, inputDependencies: ['rank'] },
    getShaderSource,
    getRunData: () => ({
      outputs: outputsTensorInfo,
      dispatchGroup: { x: Math.ceil(inputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
  };
};

export const split = (context: ComputeContext, attributes: SplitAttributes): void => {
  validateInputs(context.inputs);
  const updatedAttributes =
    context.inputs.length === 1 ? attributes : createSplitAttributesFromInputs(context.inputs, attributes);
  context.compute(createSplitProgramInfo(context.inputs, updatedAttributes), { inputs: [0] });
};

export const parseSplitAttributes = (attributes: Record<string, unknown>): SplitAttributes => {
  const axis = attributes.axis as number;
  const splitSizes: number[] = attributes.splitSizes as number[];
  const numOutputs = (attributes.numOutputs as number) < 0 ? splitSizes.length : (attributes.numOutputs as number);
  if (numOutputs !== splitSizes.length) {
    throw new Error('numOutputs and splitSizes lengh must be equal');
  }
  return createAttributeWithCacheKey({ axis, numOutputs, splitSizes });
};
