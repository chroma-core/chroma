// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import { createTensorShapeVariables, IndicesHelper, inputVariable, outputVariable, ShaderHelper } from './common';

export interface ConcatAttributes extends AttributeWithCacheKey {
  readonly axis: number;
}

const validateInputs = (inputs: readonly TensorView[], axis: number): void => {
  if (!inputs || inputs.length < 1) {
    throw new Error('too few inputs');
  }
  const referenceIndex = 0;
  const referenceInput = inputs[referenceIndex];
  const inputType = referenceInput.dataType;
  const inputRank = referenceInput.dims.length;
  inputs.forEach((input, i) => {
    if (i === referenceIndex) {
      return;
    }
    // make sure types of all inputs match
    if (input.dataType !== inputType) {
      throw new Error('input tensors should be one type');
    }
    // make sure the dimensionality of all inputs are the same
    if (input.dims.length !== inputRank) {
      throw new Error('input tensors should have the same shape');
    }
    input.dims.forEach((dim, i) => {
      if (i !== axis && dim !== referenceInput.dims[i]) {
        throw new Error('non concat dimensions must match');
      }
    });
  });
};

const calculateInputIndexImpl = (numberOfTensors: number, sizeInConcatAxisStr: string): string => `
  fn calculateInputIndex(index: u32) -> u32 {
    let sizeInConcatAxis = array<u32, ${numberOfTensors}u>(${sizeInConcatAxisStr});
    for (var i: u32 = 0u; i < ${numberOfTensors}; i += 1u ) {
      if (index < sizeInConcatAxis[i]) {
        return i;
      }
    }
    return ${numberOfTensors}u;
  }`;

const assignOutputData = (inputs: readonly IndicesHelper[], output: IndicesHelper) => {
  const numberOfTensors = inputs.length;

  const codeLines: string[] = [];
  for (let i = 0; i < numberOfTensors; ++i) {
    const returnSnippet = output.setByOffset('global_idx', inputs[i].getByIndices('indices'));
    if (numberOfTensors === 1) {
      codeLines.push(returnSnippet);
    } else if (i === 0) {
      codeLines.push(`if (inputIndex == ${i}u) { ${returnSnippet} }`);
    } else if (i === numberOfTensors - 1) {
      codeLines.push(`else { ${returnSnippet} }`);
    } else {
      codeLines.push(`else if (inputIndex == ${i}) { ${returnSnippet} }`);
    }
  }
  return codeLines.join('\n');
};

const createConcatProgramInfo = (
  inputs: readonly TensorView[],
  adjustedAxis: number,
  outputShape: number[],
  dataType: DataType,
): ProgramInfo => {
  const outputSize = ShapeUtil.size(outputShape);

  const sizeInConcatAxis = new Array<number>(inputs.length);
  const inputVars = new Array<IndicesHelper>(inputs.length);

  let previousSum = 0;
  const inputDependencies: ProgramInputTensorInfoDependency[] = [];
  const inputRanks = [];
  const programUniforms: ProgramUniform[] = [{ type: DataType.uint32, data: outputSize }];
  for (let i = 0; i < inputs.length; ++i) {
    previousSum += inputs[i].dims[adjustedAxis];
    sizeInConcatAxis[i] = previousSum;
    inputRanks.push(inputs[i].dims.length);
    inputVars[i] = inputVariable(`input${i}`, dataType, inputRanks[i]);
    inputDependencies.push('rank');
    programUniforms.push({ type: DataType.uint32, data: sizeInConcatAxis[i] });
  }
  for (let i = 0; i < inputs.length; ++i) {
    programUniforms.push(...createTensorShapeVariables(inputs[i].dims));
  }
  programUniforms.push(...createTensorShapeVariables(outputShape));

  const output = outputVariable('output', dataType, outputShape.length);
  const indicesAxis = output.indicesGet('indices', adjustedAxis);
  const sizeInConcatAxisStr = Array.from(Array(sizeInConcatAxis.length).keys())
    .map((i) => `uniforms.sizeInConcatAxis${i}`)
    .join(',');
  const getShaderSource = (shaderHelper: ShaderHelper) => `

  ${(() => {
    shaderHelper.registerUniform('outputSize', 'u32');
    for (let i = 0; i < inputs.length; i++) {
      shaderHelper.registerUniform(`sizeInConcatAxis${i}`, 'u32');
    }
    return shaderHelper.declareVariables(...inputVars, output);
  })()}

  ${calculateInputIndexImpl(sizeInConcatAxis.length, sizeInConcatAxisStr)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}

    var indices = ${output.offsetToIndices('global_idx')};

    let inputIndex = calculateInputIndex(${indicesAxis});
    if (inputIndex != 0u) {
      let sizeInConcatAxis = array<u32, ${sizeInConcatAxis.length}u>(${sizeInConcatAxisStr});
      ${indicesAxis} -= sizeInConcatAxis[inputIndex - 1u];
    }

    ${assignOutputData(inputVars, output)}
  }`;

  return {
    name: 'Concat',
    shaderCache: { hint: `${adjustedAxis}`, inputDependencies },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const concat = (context: ComputeContext, attributes: ConcatAttributes): void => {
  const inputs = context.inputs;
  const inputShape = inputs[0].dims;
  const adjustedAxis = ShapeUtil.normalizeAxis(attributes.axis, inputShape.length);
  validateInputs(inputs, adjustedAxis);
  const outputShape = inputShape.slice();
  outputShape[adjustedAxis] = inputs.reduce(
    (sum, input) => sum + (input.dims.length > adjustedAxis ? input.dims[adjustedAxis] : 0),
    0,
  );
  // 0 length tensors are valid for concat, remove them
  const nonEmptyInputs = inputs.filter((input) => ShapeUtil.size(input.dims) > 0);
  context.compute(createConcatProgramInfo(nonEmptyInputs, adjustedAxis, outputShape, inputs[0].dataType), {
    inputs: nonEmptyInputs,
  });
};

export const parseConcatAttributes = (attributes: Record<string, unknown>): ConcatAttributes =>
  createAttributeWithCacheKey({ axis: attributes.axis as number });
