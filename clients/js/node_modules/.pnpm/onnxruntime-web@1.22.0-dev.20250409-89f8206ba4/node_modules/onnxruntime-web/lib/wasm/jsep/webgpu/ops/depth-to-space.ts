// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, IndicesHelper, inputVariable, outputVariable, ShaderHelper } from './common';

export interface FormatAttributes {
  readonly format: 'NHWC' | 'NCHW';
}

export interface DepthToSpaceAttributes extends FormatAttributes, AttributeWithCacheKey {
  readonly blocksize: number;
  readonly mode: string;
}

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('DepthToSpace requires 1 input.');
  }
  if (inputs[0].dims.length !== 4) {
    throw new Error('DepthToSpace requires 4D input.');
  }
};

const permFunctionBody = (perm: number[], rank: number, input: IndicesHelper, output: IndicesHelper): string => {
  const reverseFunc = [];
  reverseFunc.push(`fn perm(i: ${output.type.indices}) -> ${input.type.indices} {
    var a: ${input.type.indices};`);
  for (let i = 0; i < rank; ++i) {
    reverseFunc.push(input.indicesSet('a', perm[i], `i[${i}]`));
  }
  reverseFunc.push('return a;}');
  return reverseFunc.join('\n');
};

const createDepthToSpaceProgramInfo = (inputTensor: TensorView, attributes: DepthToSpaceAttributes): ProgramInfo => {
  let n: number, h: number, w: number, c: number;
  let shape: number[];
  let perm: number[];
  const isChannelLast = attributes.format === 'NHWC';
  const blocksize = attributes.blocksize;
  const isDCRmode = attributes.mode === 'DCR';
  if (isChannelLast) {
    [n, h, w, c] = inputTensor.dims;
    shape = isDCRmode
      ? [n, h, w, blocksize, blocksize, c / blocksize ** 2]
      : [n, h, w, c / blocksize ** 2, blocksize, blocksize];
    perm = isDCRmode ? [0, 1, 3, 2, 4, 5] : [0, 1, 4, 2, 5, 3];
  } else {
    [n, h, w, c] = [inputTensor.dims[0], inputTensor.dims[2], inputTensor.dims[3], inputTensor.dims[1]];
    shape = isDCRmode
      ? [n, blocksize, blocksize, c / blocksize ** 2, h, w]
      : [n, c / blocksize ** 2, blocksize, blocksize, h, w];
    perm = isDCRmode ? [0, 3, 4, 1, 5, 2] : [0, 1, 4, 2, 5, 3];
  }
  const reshapedInputTensor = inputTensor.reshape(shape);
  const reshapedInputRank = reshapedInputTensor.dims.length;
  const inputDataType = inputTensor.dataType;

  const reshapedInput = inputVariable('a', inputDataType, reshapedInputRank);
  const permedOutput = outputVariable('output', inputDataType, reshapedInputRank);

  const getShaderSource = (shaderHelper: ShaderHelper) => `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(reshapedInput, permedOutput)}

  ${permFunctionBody(perm, reshapedInputRank, reshapedInput, permedOutput)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}

    let indices = ${permedOutput.offsetToIndices('global_idx')};
    let aIndices = perm(indices);

    ${permedOutput.setByOffset('global_idx', reshapedInput.getByIndices('aIndices'))}
  }`;

  return {
    name: 'DepthToSpace',
    shaderCache: {
      hint: `${inputTensor.dims};${attributes.blocksize};${attributes.mode}`,
      inputDependencies: ['rank'],
    },
    getRunData: (inputs) => {
      const outputShape = isChannelLast
        ? [n, h * blocksize, w * blocksize, c / blocksize ** 2]
        : [n, c / blocksize ** 2, h * blocksize, w * blocksize];
      const outputSize = ShapeUtil.size(outputShape);
      const shapeBeforePerm = reshapedInputTensor.dims;
      const shapeAfterPerm = ShapeUtil.sortBasedOnPerm(shapeBeforePerm, perm);
      return {
        outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms: [
          { type: DataType.uint32, data: outputSize },
          ...createTensorShapeVariables(shapeBeforePerm, shapeAfterPerm),
        ],
      };
    },
    getShaderSource,
  };
};

export const depthToSpace = (context: ComputeContext, attributes: DepthToSpaceAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createDepthToSpaceProgramInfo(context.inputs[0], attributes));
};

export const parseDepthToSpaceAttributes = (attributes: Record<string, unknown>): DepthToSpaceAttributes =>
  createAttributeWithCacheKey({
    blocksize: attributes.blocksize as number,
    mode: attributes.mode as string,
    format: attributes.format as 'NHWC' | 'NCHW',
  });
