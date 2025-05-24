// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, TextureType } from '../types';

export interface TransposeAttributes extends AttributeWithCacheKey {
  readonly perm: number[];
}

const transposeProgramMetadata = {
  name: 'Transpose',
  inputNames: ['A'],
  inputTypes: [TextureType.unpacked],
};

export const transpose: OperatorImplementation<TransposeAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: TransposeAttributes,
): Tensor[] => {
  validateInputs(inputs);
  const output = inferenceHandler.run(
    {
      ...transposeProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createTransposeProgramInfo(inferenceHandler, inputs[0], attributes.perm),
    },
    inputs,
  );
  return [output];
};

export const parseTransposeAttributes: OperatorInitialization<TransposeAttributes> = (
  node: Graph.Node,
): TransposeAttributes => createAttributeWithCacheKey({ perm: node.attributes.getInts('perm', []) });

const createTransposeProgramInfo = (
  _inferenceHandler: WebGLInferenceHandler,
  input: Tensor,
  perm: number[],
): ProgramInfo => {
  const inputShape = input.dims;
  perm = getAdjustedPerm(inputShape, perm);
  const unpackedOutputShape = getOutputShape(inputShape, perm);
  const rank = inputShape.length;
  // A dims=[${inputs[0].dims.toString()}]
  // out Dims=[${unpackedOutputShape.toString()}]
  // based on perm=[${perm.toString()}]
  const shaderSource = `
      ${getPermFunctionBody('perm', perm, rank)}
      float process(int indices[${rank}]) {
        int a[${rank}];
        perm(a, indices);
        return _A(a);
      }`;
  return {
    ...transposeProgramMetadata,
    output: { dims: unpackedOutputShape, type: input.type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

const getAdjustedPerm = (inputShape: readonly number[], perm: number[]): number[] => {
  if (perm && perm.length !== inputShape.length) {
    perm = [...inputShape.keys()].reverse();
  }
  return perm;
};

const getOutputShape = (inputShape: readonly number[], perm: number[]): readonly number[] => {
  perm = getAdjustedPerm(inputShape, perm);
  return ShapeUtil.sortBasedOnPerm(inputShape, perm);
};

const getPermFunctionBody = (name: string, perm: number[], rank: number): string => {
  const reverseFunc = [];
  reverseFunc.push(`void ${name}(out int a[${rank}], int src[${rank}]) {`);
  for (let i = 0; i < rank; ++i) {
    reverseFunc.push(`\ta[${perm[i]}]=src[${i}];`);
  }
  reverseFunc.push('\t}');
  return reverseFunc.join('\n');
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Transpose requires 1 input.');
  }

  if (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') {
    throw new Error('input should be float tensor');
  }
};
