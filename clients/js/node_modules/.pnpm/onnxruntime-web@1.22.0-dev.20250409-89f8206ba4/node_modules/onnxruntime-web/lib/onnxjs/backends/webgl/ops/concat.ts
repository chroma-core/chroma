// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

import { createPackedConcatProgramInfoLoader } from './concat-packed';

export interface ConcatAttributes extends AttributeWithCacheKey {
  readonly axis: number;
}

export const concat: OperatorImplementation<ConcatAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ConcatAttributes,
): Tensor[] => {
  validateInputs(inputs);
  if (inferenceHandler.session.pack && inputs[0].dims.length > 1) {
    const output = inferenceHandler.run(
      createPackedConcatProgramInfoLoader(inferenceHandler, inputs, attributes),
      inputs,
    );
    return [output];
  } else {
    const output = inferenceHandler.run(
      createUnpackedConcatProgramInfoLoader(inferenceHandler, inputs, attributes),
      inputs,
    );
    return [output];
  }
};

const createUnpackedConcatProgramMetadata = (inputCount: number, cacheHint: string) => ({
  name: 'Concat',
  inputNames: Array.from({ length: inputCount }, (_v, i) => `X${i}`),
  inputTypes: Array(inputCount).fill(TextureType.unpacked),
  cacheHint,
});

const createUnpackedConcatProgramInfo = (
  _handler: WebGLInferenceHandler,
  metadata: ProgramMetadata,
  inputs: Tensor[],
  axis: number,
): ProgramInfo => {
  const inputShape = inputs[0].dims.slice();
  if (axis >= inputShape.length || axis < -1 * inputShape.length) {
    throw new Error("axis specified for concat doesn't match input dimensionality");
  }
  if (axis < 0) {
    axis = inputShape.length + axis;
  }
  // ensure all of the non-concatenated axes match each other
  // calculate the shape of the output tensor while we do that
  const outputShape = inputShape.slice(0);
  for (let i = 1; i < inputs.length; i++) {
    const dataNShape = inputs[i].dims.slice();
    for (let axisIndex = 0; axisIndex < inputShape.length; axisIndex++) {
      // add to the placeholder for computing output shape
      if (axisIndex === axis) {
        outputShape[axis] += dataNShape[axisIndex];
      }
      // ensure all non-cancatenated axes match each other
      else if (inputShape[axisIndex] !== dataNShape[axisIndex]) {
        throw new Error('non concat dimensions must match');
      }
    }
  }

  const rank = outputShape.length;

  const sizeInConcatAxis = new Array<number>(inputs.length);
  let previousSum = 0;
  for (let i = 0; i < sizeInConcatAxis.length; ++i) {
    previousSum += inputs[i].dims[axis];
    sizeInConcatAxis[i] = previousSum;
  }

  let getTextureIndexWhereDataResidesMethod = '';
  // in most cases linear search is sufficient, as in most scenarios, only 2 tensors are concatenated
  if (inputs.length < 5) {
    getTextureIndexWhereDataResidesMethod = getTextureIndexWhereDataResidesLinearSearch(sizeInConcatAxis);
  } else {
    getTextureIndexWhereDataResidesMethod = getTextureIndexWhereDataResidesBinarySearch(sizeInConcatAxis);
  }

  const fetchDataFromCorrectTextureMethod = getFetchDataFromCorrectTextureMethod(inputs.length, rank);
  const getSizeInConcatAxisValueFromIndexMethod = getGetSizeInConcatAxisValueFromIndexMethod(sizeInConcatAxis);
  const shaderSource = `
        ${fetchDataFromCorrectTextureMethod}
        ${getSizeInConcatAxisValueFromIndexMethod}
        ${getTextureIndexWhereDataResidesMethod}
        float process(int indices[${rank}]) {
          int textureIndex = getTextureWhereDataResides (indices[${axis}]);

          if(textureIndex != 0) {
            indices[${axis}] = indices[${axis}] - int(getSizeInConcatAxisValueFromIndex(textureIndex-int(1)));
          }

          return fetchDataFromCorrectTexture(textureIndex, indices);
        }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

const createUnpackedConcatProgramInfoLoader = (
  handler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ConcatAttributes,
): ProgramInfoLoader => {
  const metadata = createUnpackedConcatProgramMetadata(inputs.length, attributes.cacheKey);
  return { ...metadata, get: () => createUnpackedConcatProgramInfo(handler, metadata, inputs, attributes.axis) };
};

const getTextureIndexWhereDataResidesLinearSearch = (sizeInConcatAxis: number[]): string => {
  const searchAxis = sizeInConcatAxis.map(
    (size, i) => `if(index<${size}) {return ${i};}
`,
  );
  return `int getTextureWhereDataResides(int index) {
      ${searchAxis.join('')}
    }`;
};

// TODO: Implement BinarySearch in GLSL
const getTextureIndexWhereDataResidesBinarySearch = (sizeInConcatAxis: number[]): string =>
  getTextureIndexWhereDataResidesLinearSearch(sizeInConcatAxis);

const getFetchDataFromCorrectTextureMethod = (numberOfTensors: number, tensorRank: number) => {
  const codeLines: string[] = [`float fetchDataFromCorrectTexture(int textureIndex, int indices[${tensorRank}]) {`];
  for (let i = 0; i < numberOfTensors; ++i) {
    if (i === 0) {
      codeLines.push('\t' + `if (textureIndex == ${i}) { return _X${i}(indices); }`);
    } else if (i === numberOfTensors - 1) {
      codeLines.push('\t' + `else { return _X${i}(indices); }`);
    } else {
      codeLines.push('\t' + `else if (textureIndex == ${i}) { return _X${i}(indices); }`);
    }
  }
  codeLines.push('\t' + '}');
  return codeLines.join('\n');
};

const getGetSizeInConcatAxisValueFromIndexMethod = (sizeInConcatAxis: number[]): string => {
  const codeLines: string[] = ['int getSizeInConcatAxisValueFromIndex(int index) {'];
  for (let i = 0; i < sizeInConcatAxis.length; ++i) {
    if (i === 0) {
      codeLines.push('\t' + `if (index == ${i}) { return ${sizeInConcatAxis[i]}; }`);
    } else if (i === sizeInConcatAxis.length - 1) {
      codeLines.push('\t' + `else { return ${sizeInConcatAxis[i]}; }`);
    } else {
      codeLines.push('\t' + `else if (index == ${i}) { return ${sizeInConcatAxis[i]}; }`);
    }
  }
  codeLines.push('\t' + '}');

  return codeLines.join('\n');
};

export const parseConcatAttributes: OperatorInitialization<ConcatAttributes> = (node: Graph.Node): ConcatAttributes =>
  createAttributeWithCacheKey({ axis: node.attributes.getInt('axis') });

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length < 1) {
    throw new Error('too few inputs');
  }

  const inputType = inputs[0].type;
  const inputDimensionality = inputs[0].dims.length;

  // TODO: Support string concat
  if (inputType === 'string') {
    throw new Error('string tensor is not supported yet');
  }

  for (const input of inputs) {
    // make sure types of all inputs match
    if (input.type !== inputType) {
      throw new Error('input tensors should be one type');
    }

    // make sure the dimensionality of all inputs are the same
    if (input.dims.length !== inputDimensionality) {
      throw new Error('input tensors should have the same shape');
    }
  }
};
