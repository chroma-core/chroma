// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { ShapeUtil, SplitUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, TextureType } from '../types';

export interface SplitAttributes extends AttributeWithCacheKey {
  readonly axis: number;
  readonly split: number[];
  readonly numOutputs: number;
}

const splitProgramMetadata = {
  name: 'Split',
  inputNames: ['A'],
  inputTypes: [TextureType.unpacked],
};

export const split: OperatorImplementation<SplitAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: SplitAttributes,
): Tensor[] => {
  validateInputs(inputs);

  const axis = ShapeUtil.normalizeAxis(attributes.axis, inputs[0].dims.length);
  const count = getProgramCount(inferenceHandler, inputs, axis, attributes);
  const output: Tensor[] = [];
  for (let i = 0; i < count; ++i) {
    output.push(
      inferenceHandler.run(
        {
          ...splitProgramMetadata,
          cacheHint: `${attributes.cacheKey};${i}`,
          get: () => createSplitProgramInfo(inferenceHandler, inputs[0], attributes, axis, i),
        },
        inputs,
      ),
    );
  }

  return output;
};

export const parseSplitAttributes: OperatorInitialization<SplitAttributes> = (node: Graph.Node): SplitAttributes => {
  const axis = node.attributes.getInt('axis', 0);
  const split = node.attributes.getInts('split', []);
  const numOutputs = node.outputs.length;
  return createAttributeWithCacheKey({ axis, split, numOutputs });
};

const getProgramCount = (
  _inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  axis: number,
  attributes: SplitAttributes,
): number => {
  const [, offsets] = SplitUtil.splitShape(inputs[0].dims, axis, attributes.split, attributes.numOutputs);
  return offsets.length;
};

const createSplitProgramInfo = (
  _inferenceHandler: WebGLInferenceHandler,
  input: Tensor,
  attributes: SplitAttributes,
  axis: number,
  index: number,
): ProgramInfo => {
  const [shapes, offsets] = SplitUtil.splitShape(input.dims, axis, attributes.split, attributes.numOutputs);
  const offset = offsets[index];
  const outputShape = shapes[index];
  const rank = outputShape.length;
  const shaderSource = `
      float process(int indices[${rank}]) {
        indices[${axis}] += ${offset};
        return _A(indices);
      }
    `;
  return {
    ...splitProgramMetadata,
    cacheHint: `${attributes.cacheKey}:${index}`,
    output: { dims: outputShape, type: input.type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

const validateInputs = (inputs: Tensor[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Split requires one input.');
  }

  if (
    inputs[0].type !== 'int8' &&
    inputs[0].type !== 'uint8' &&
    inputs[0].type !== 'int16' &&
    inputs[0].type !== 'uint16' &&
    inputs[0].type !== 'int32' &&
    inputs[0].type !== 'uint32' &&
    inputs[0].type !== 'float32' &&
    inputs[0].type !== 'float64' &&
    inputs[0].type !== 'bool'
  ) {
    throw new Error('Invalid input type.');
  }
};
