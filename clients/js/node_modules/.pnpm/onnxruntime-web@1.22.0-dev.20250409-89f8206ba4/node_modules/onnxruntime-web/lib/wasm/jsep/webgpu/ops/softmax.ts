// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// TODO: this is the same naive implementation we use for reduce that has
// performance limitations when the reduced axis is long. Need to add
// a optimized codepath for this.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext } from '../types';
import { createTransposeProgramInfo } from './transpose';

import {
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  sumVector,
  tensorTypeToWsglStorageType,
} from './common';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs || inputs.length !== 1) {
    throw new Error('Softmax op requires 1 input.');
  }
};

export interface SoftmaxAttributes extends AttributeWithCacheKey {
  readonly axis: number;
}

const createSoftmaxProgramInfo = (context: ComputeContext, attributes: SoftmaxAttributes) => {
  const input = context.inputs[0];
  const inputShape = input.dims;
  const outputSize = ShapeUtil.size(inputShape);
  const inputRank = inputShape.length;
  const axis = ShapeUtil.normalizeAxis(attributes.axis, inputRank);
  const isTransposeRequired = axis < inputShape.length - 1;
  let transposedInput: TensorView;
  let perm: number[] = [];

  if (isTransposeRequired) {
    perm = Array.from({ length: inputRank }, (_, i) => i);
    perm[axis] = inputRank - 1;
    perm[inputRank - 1] = axis;

    transposedInput = context.compute(createTransposeProgramInfo(input, perm), {
      inputs: [input],
      outputs: [-1],
    })[0];
  } else {
    transposedInput = input;
  }

  const transposedInputShape = transposedInput.dims;
  const cols = transposedInputShape[inputRank - 1];
  const rows = outputSize / cols;
  const components = getMaxComponents(cols);
  const packedCols = cols / components;
  let WG = 64;
  // If only one workgroup is dispatched, increase workgroupSize to improve parallelism.
  if (rows === 1) {
    WG = 256;
  }
  const maxVector = (name: string, components: number) => {
    if (components === 4) {
      return `max(max(${name}.x, ${name}.y), max(${name}.z, ${name}.w))`;
    } else if (components === 2) {
      return `max(${name}.x, ${name}.y)`;
    } else if (components === 3) {
      return `max(max(${name}.x, ${name}.y), ${name}.z)`;
    }

    return name;
  };
  const x = inputVariable('x', transposedInput.dataType, transposedInput.dims, components);
  const output = outputVariable('result', transposedInput.dataType, transposedInput.dims, components);
  const valueType = x.type.value;
  // 6.2.4 in wgsl spec
  const threadMaxDecl =
    tensorTypeToWsglStorageType(transposedInput.dataType) === 'f32'
      ? `var threadMax = ${valueType}(-3.402823e+38f);`
      : `var threadMax = ${valueType}(-65504.0h);`;
  const getShaderSource = (shaderHelper: ShaderHelper) => `
      var<workgroup> rowMaxShared : ${valueType};
      var<workgroup> rowSumShared : ${valueType};
      var<workgroup> threadShared : array<${valueType}, ${WG}>;

      fn getValue(row: i32, col: i32, row_stride: i32) -> ${valueType} {
        let index = row * row_stride + col;
        return x[index];
      }

      fn setValue(row: i32, col: i32, row_stride: i32, value: ${valueType}) {
        let index = row * row_stride + col;
        result[index] = value;
      }
      ${shaderHelper.registerUniform('packedCols', 'i32').declareVariables(x, output)}
      ${shaderHelper.mainStart(WG)}
        let gindex = i32(global_idx);
        let lindex = i32(local_idx);
        const wg = ${WG};
        let row = gindex / wg;
        let cols = uniforms.packedCols;
        let row_stride : i32 = uniforms.packedCols;

        // find the rows max
        ${threadMaxDecl}
        for (var col = lindex; col < cols; col += wg) {
          let value = getValue(row, col, row_stride);
          threadMax = max(threadMax, value);
        }
        if (lindex < cols) {
          threadShared[lindex] = threadMax;
        }
        workgroupBarrier();

        var reduceSize = min(cols, wg);
        for (var currSize = reduceSize >> 1;  currSize > 0; currSize = reduceSize >> 1) {
          reduceSize = currSize + (reduceSize & 1);
          if (lindex < currSize) {
            threadShared[lindex] = max(threadShared[lindex], threadShared[lindex + reduceSize]);
          }
          workgroupBarrier();
        }
        if (lindex == 0) {
          rowMaxShared = ${valueType}(${maxVector('threadShared[0]', components)});
        }
        workgroupBarrier();

        // find the rows sum
        var threadSum = ${valueType}(0.0);
        for (var col = lindex; col < cols; col += wg) {
          let subExp = exp(getValue(row, col, row_stride) - rowMaxShared);
          threadSum += subExp;
        }
        threadShared[lindex] = threadSum;
        workgroupBarrier();

        for (var currSize = wg >> 1;  currSize > 0; currSize = currSize >> 1) {
          if (lindex < currSize) {
            threadShared[lindex] = threadShared[lindex] + threadShared[lindex + currSize];
          }
          workgroupBarrier();
        }
        if (lindex == 0) {
          rowSumShared = ${valueType}(${sumVector('threadShared[0]', components)});
        }
        workgroupBarrier();

        // calculate final value for each element in the row
        for (var col = lindex; col < cols; col += wg) {
          let value = exp(getValue(row, col, row_stride) - rowMaxShared) / rowSumShared;
          setValue(row, col, row_stride, value);
        }
      }`;
  const result = context.compute(
    {
      name: 'Softmax',
      // Note that in JSEP, WG size is not included in cache by default, but WebGPU EP it is.
      shaderCache: { hint: `${components};${WG}`, inputDependencies: ['type'] },
      getRunData: () => ({
        outputs: [{ dims: transposedInputShape, dataType: transposedInput.dataType }],
        dispatchGroup: { x: rows },
        programUniforms: [{ type: DataType.int32, data: packedCols }],
      }),
      getShaderSource,
    },
    {
      inputs: [transposedInput],
      outputs: [isTransposeRequired ? -1 : 0],
    },
  )[0];

  if (isTransposeRequired) {
    context.compute(createTransposeProgramInfo(result, perm), {
      inputs: [result],
    });
  }
};

export const softmax = (context: ComputeContext, attributes: SoftmaxAttributes): void => {
  validateInputs(context.inputs);
  createSoftmaxProgramInfo(context, attributes);
};

export const parseSoftmaxAttributes = (attributes: Record<string, unknown>): SoftmaxAttributes =>
  createAttributeWithCacheKey({ axis: attributes.axis as number });
