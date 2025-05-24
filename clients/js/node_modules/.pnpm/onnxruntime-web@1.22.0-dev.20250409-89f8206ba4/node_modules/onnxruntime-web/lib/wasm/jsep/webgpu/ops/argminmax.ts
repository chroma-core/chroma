// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// TODO: this is the same naive implementation we use for reduce that has
// performance limitations when the reduced axis is long. Need to add
// a optimized codepath for this.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext } from '../types';

import { createReduceProgramInfo, ReduceOp } from './reduce';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length === 0 || inputs.length > 2) {
    throw new Error('ArgMinMaxOp op requires 1 or 2 inputs.');
  }
  if (inputs[0].dataType !== DataType.float) {
    throw new Error('Invalid input type.');
  }
};

export interface ArgMinMaxAttributes extends AttributeWithCacheKey {
  keepDims: boolean;
  axis: number;
  selectLastIndex: number;
}

export const argMin = (context: ComputeContext, attributes: ArgMinMaxAttributes): void => {
  validateInputs(context.inputs);
  const argMinMaxOp: ReduceOp = (input, output, axes) => {
    const idxZero = [];
    for (let k = 0; k < input.rank; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`input_indices[${k}] = 0;`); // first element
      }
    }
    return [
      `${idxZero.join('\n')}`,
      `var value = ${input.getByIndices('input_indices')};\nvar best_index : i32 = 0;`,
      `if (${input.getByIndices('input_indices')} ${attributes.selectLastIndex > 0 ? '<=' : '<'} value) {
         value = ${input.getByIndices('input_indices')};
         best_index = i32(last_index);
       }`,
      '',
      output.setByOffset('global_idx', 'best_index'),
    ];
  };

  context.compute(
    createReduceProgramInfo(
      'ArgMin',
      { hint: attributes.cacheKey, inputDependencies: ['rank'] },
      [context.inputs[0]],
      argMinMaxOp,
      [attributes.axis],
      DataType.int64,
      attributes.keepDims,
    ),
    { inputs: [0] },
  );
};

export const argMax = (context: ComputeContext, attributes: ArgMinMaxAttributes): void => {
  validateInputs(context.inputs);
  const argMinMaxOp: ReduceOp = (input, output, axes) => {
    const idxZero = [];
    for (let k = 0; k < input.rank; k++) {
      if (axes.indexOf(k) >= 0 || axes.length === 0) {
        idxZero.push(`input_indices[${k}] = 0;`); // first element
      }
    }
    return [
      `${idxZero.join('\n')}`,
      `var value = ${input.getByIndices('input_indices')};\nvar best_index : i32 = 0;`,
      `if (${input.getByIndices('input_indices')} ${attributes.selectLastIndex > 0 ? '>=' : '>'} value) {
         value = ${input.getByIndices('input_indices')};
         best_index = i32(last_index);
       }`,
      '',
      output.setByOffset('global_idx', 'best_index'),
    ];
  };

  context.compute(
    createReduceProgramInfo(
      'argMax',
      { hint: attributes.cacheKey, inputDependencies: ['rank'] },
      [context.inputs[0]],
      argMinMaxOp,
      [attributes.axis],
      DataType.int64,
      attributes.keepDims,
    ),
    { inputs: [0] },
  );
};

export const parseArgMinMaxAttributes = (attributes: Record<string, unknown>): ArgMinMaxAttributes =>
  createAttributeWithCacheKey(attributes as Omit<ArgMinMaxAttributes, keyof AttributeWithCacheKey>);
