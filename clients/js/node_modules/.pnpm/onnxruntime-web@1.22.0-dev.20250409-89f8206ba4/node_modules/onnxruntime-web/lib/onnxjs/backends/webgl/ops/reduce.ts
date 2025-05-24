// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { NUMBER_TYPES, OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { ShapeUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramMetadata, TextureType } from '../types';

export interface ReduceAttributes extends AttributeWithCacheKey {
  readonly axes: number[];
  readonly keepDims: boolean;
}

// return [init ops, reduce ops, final ops]
type ReduceOp = (inputs: Tensor[], axes: number[]) => string[];

const reduce = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
  name: string,
  reduceOp: ReduceOp,
): Tensor[] => {
  validateInputs(inputs);

  const reduceProgramMetadata = {
    name,
    inputNames: ['A'],
    inputTypes: [TextureType.unpacked],
  };

  const output = inferenceHandler.run(
    {
      ...reduceProgramMetadata,
      cacheHint: attributes.cacheKey,
      get: () => createReduceProgramInfo(inferenceHandler, inputs, attributes, name, reduceOp, reduceProgramMetadata),
    },
    inputs,
  );
  return [output];
};

export const parseReduceAttributes: OperatorInitialization<ReduceAttributes> = (node: Graph.Node): ReduceAttributes => {
  const axes = node.attributes.getInts('axes', []);
  const keepDims = node.attributes.getInt('keepdims', 1) === 1;
  return createAttributeWithCacheKey({ axes, keepDims });
};

const createReduceProgramInfo = (
  _handler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
  _name: string,
  reduceOp: ReduceOp,
  reduceProgramMetadata: ProgramMetadata,
): ProgramInfo => {
  const outputShape: number[] = [];
  const iRank = inputs[0].dims.length || 1;

  const idxCopy = []; // copy output indexes to input indexes

  const axes = ShapeUtil.normalizeAxes(attributes.axes, inputs[0].dims.length);
  const ops = reduceOp(inputs, axes);
  let reduceOps = ops[1];

  for (let k = 0; k < inputs[0].dims.length; k++) {
    // if this axis is reduced
    if (axes.indexOf(k) >= 0 || axes.length === 0) {
      if (attributes.keepDims) {
        outputShape.push(1);
      } // else { remove the axis from outputShape; }

      // loop over the d-th axis
      reduceOps = `
          for(int j${k} = 0; j${k} < ${inputs[0].dims[k]}; j${k}++) {
            inputIdx[${k}] = j${k};
            ${reduceOps}
          }`;
    } else {
      idxCopy.push(`inputIdx[${k}] = outputIdx[${outputShape.length}];`);

      outputShape.push(inputs[0].dims[k]);
    }
  }

  const oRank = outputShape.length || 1;

  const shaderSource = `
      float process(int outputIdx[${oRank}]) {
        float value;                 // final result
        int inputIdx[${iRank}];      // addressing input data
        ${idxCopy.join('\n')}
        ${ops[0]}       // init ops for reduce max/min
        ${reduceOps}
        ${ops[2]}       // final computation for reduce mean
        return value;
      }`;

  return {
    ...reduceProgramMetadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    shaderSource,
  };
};

const validateInputs = (inputs: Tensor[]): void => {
  // TODO: support Reduce* operators with 2 inputs.
  if (!inputs || inputs.length !== 1) {
    throw new Error('Reduce op requires 1 input.');
  }

  if (NUMBER_TYPES.indexOf(inputs[0].type) === -1) {
    throw new Error('Invalid input type.');
  }
};

export const reduceSum: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (): string[] => ['value = 0.0;', 'value += _A(inputIdx);', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceSum', reduceOp);
};

export const reduceMean: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (inputs: Tensor[], axes: number[]): string[] => {
    let size = 1.0;
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        size *= inputs[0].dims[k];
      }
    }

    return ['value = 0.0;', 'value += _A(inputIdx);', `value /= ${size}.;`]; // ensure real number with `.`
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMean', reduceOp);
};

export const reduceMax: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (inputs: Tensor[], axes: number[]): string[] => {
    const idxZero = [];
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`inputIdx[${k}] = 0;`); // first element
      }
    }

    return [`${idxZero.join('\n')}\nvalue = _A(inputIdx);`, 'value = max(value, _A(inputIdx));', ''];
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMax', reduceOp);
};

export const reduceMin: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (inputs: Tensor[], axes: number[]): string[] => {
    const idxZero = [];
    for (let k = 0; k < inputs[0].dims.length; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`inputIdx[${k}] = 0;`); // first element
      }
    }

    return [`${idxZero.join('\n')}\nvalue = _A(inputIdx);`, 'value = min(value, _A(inputIdx));', ''];
  };
  return reduce(inferenceHandler, inputs, attributes, 'ReduceMin', reduceOp);
};

export const reduceProd: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (): string[] => ['value = 1.0;', 'value *= _A(inputIdx);', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceProd', reduceOp);
};

export const reduceLogSum: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (): string[] => ['value = 0.0;', 'value += _A(inputIdx);', 'value = log(value);'];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceLogSum', reduceOp);
};

export const reduceLogSumSquare: OperatorImplementation<ReduceAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: ReduceAttributes,
): Tensor[] => {
  const reduceOp: ReduceOp = (): string[] => ['float t; value = 0.0;', 't = _A(inputIdx); value += t * t;', ''];
  return reduce(inferenceHandler, inputs, attributes, 'ReduceLogSumSquare', reduceOp);
};
