// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { inputVariable, outputVariable, ShaderHelper } from './common';
import { createReduceAttributesFromInputs, ReduceAttributes } from './reduce';
import { createTransposeProgramInfo } from './transpose';

const reduceOps: { [key: string]: string } = {
  max: 'select(bestValue, candidate, candidate > bestValue)',
  min: 'select(bestValue, candidate, candidate < bestValue)',
  mean: 'bestValue + candidate',
  sum: 'bestValue + candidate',
  prod: 'bestValue * candidate',
  sumSquare: 'bestValue + candidate * candidate',
  logSumExp: 'bestValue + exp(candidate)',
  l1: 'bestValue + abs(candidate)',
  l2: 'bestValue + candidate * candidate',
  logSum: 'bestValue + candidate',
};

const reduceSharedOps: { [key: string]: string } = {
  max: 'select(bestValue, candidate, candidate > bestValue)',
  min: 'select(bestValue, candidate, candidate < bestValue)',
  mean: 'bestValue + candidate',
  sum: 'bestValue + candidate',
  prod: 'bestValue * candidate',
  sumSquare: 'bestValue + candidate',
  logSumExp: 'bestValue + candidate',
  l1: 'bestValue + candidate',
  l2: 'bestValue + candidate',
  logSum: 'bestValue + candidate',
};

const reduceInitValues: { [key: string]: string } = {
  max: '_A[offset]',
  min: '_A[offset]',
  mean: '0',
  sum: '0',
  prod: '1',
  sumSquare: '0',
  logSumExp: '0',
  l1: '0',
  l2: '0',
  logSum: '0',
};

const reduceOutputValues: { [key: string]: string } = {
  max: 'bestValue',
  min: 'bestValue',
  sum: 'bestValue',
  prod: 'bestValue',
  sumSquare: 'bestValue',
  logSumExp: 'log(bestValue)',
  l1: 'bestValue',
  l2: 'sqrt(bestValue)',
  logSum: 'log(bestValue)',
};

const getInnerMostAxes = (numInnerAxes: number, rank: number): number[] => {
  const res = [];
  for (let i = rank - numInnerAxes; i < rank; ++i) {
    res.push(i);
  }
  return res;
};

const computeOutAndReduceShapes = (shape: readonly number[], axes: readonly number[]): [number[], number[]] => {
  const outputShape = [];
  const rank = shape.length;
  for (let dim = 0; dim < rank; dim++) {
    if (axes.indexOf(dim) === -1) {
      outputShape.push(shape[dim]);
    }
  }
  const reduceShape = axes.map((dim) => shape[dim]);
  return [outputShape, reduceShape];
};

const expandShapeToKeepDim = (shape: number[], axes: number[]): number[] => {
  const rank = shape.length + axes.length;
  const expandShape = [];
  let shapeIdx = 0;
  for (let dim = 0; dim < rank; dim++) {
    if (axes.indexOf(dim) === -1) {
      expandShape.push(shape[shapeIdx++]);
    } else {
      expandShape.push(1);
    }
  }
  return expandShape;
};

const areAxesInnerMostDims = (axes: number[], rank: number): boolean => {
  for (let i = 0; i < axes.length; ++i) {
    if (axes[axes.length - i - 1] !== rank - 1 - i) {
      return false;
    }
  }
  return true;
};

const getAxesPermutation = (axes: number[], rank: number): number[] => {
  const res = [];
  if (!areAxesInnerMostDims(axes, rank)) {
    for (let i = 0; i < rank; ++i) {
      if (axes.indexOf(i) === -1) {
        res.push(i);
      }
    }
    axes.forEach((axis) => res.push(axis));
  }
  return res;
};

export const createReduceSharedProgramInfo = (
  name: string,
  cacheKey: string,
  inputs: readonly TensorView[],
  reduceType: string,
  outputDataType: DataType,
  outputShape: number[],
  reduceShape: number[],
): ProgramInfo => {
  const inputShape = inputs[0].dims;

  const outputSize = ShapeUtil.size(outputShape);
  const reduceSize = ShapeUtil.size(reduceShape);

  const input = inputVariable('_A', inputs[0].dataType, inputShape);
  const output = outputVariable('output', outputDataType, outputShape);

  let workgroupSize = 64;
  // If only one workgroup is dispatched, increase workgroupSize to improve parallelism.
  if (outputSize === 1) {
    workgroupSize = 256;
  }

  const sharedMemorySnippet = `
          var<workgroup> aBestValues : array<f32, ${workgroupSize}>;
       `;

  const getShaderSource = (shaderHelper: ShaderHelper) => `
        ${shaderHelper.registerUniform('reduceSize', 'u32').declareVariables(input, output)}
        ${sharedMemorySnippet}
        fn DIV_CEIL(a : u32, b : u32) -> u32 {
          return ((a - 1u) / b + 1u);
         }
         ${shaderHelper.mainStart(workgroupSize)}

          let outputIndex = global_idx / ${workgroupSize};
          let offset = outputIndex * uniforms.reduceSize;

          var bestValue = f32(${reduceInitValues[reduceType]});
          let Length = uniforms.reduceSize;
          for (var k = local_idx; k < Length; k = k + ${workgroupSize}) {
           let candidate = f32(${input.getByOffset('offset + k')});
           bestValue = ${reduceOps[reduceType]};
          }
          aBestValues[local_idx] = bestValue;
          workgroupBarrier();

         var reduceSize = min(Length, ${workgroupSize}u);
         for (var currentSize = reduceSize / 2u; reduceSize > 1u;
             currentSize = reduceSize / 2u) {
           let interval = DIV_CEIL(reduceSize, 2u);
           if (local_idx < currentSize) {
            let candidate = aBestValues[local_idx + interval];
            bestValue = ${reduceSharedOps[reduceType]};
            aBestValues[local_idx] = bestValue;
           }
           reduceSize = interval;
           workgroupBarrier();
         }

         if (local_idx == 0u) {
          ${output.setByOffset(
            'outputIndex',
            `${
              reduceType === 'mean'
                ? `${output.type.storage}(bestValue / f32(uniforms.reduceSize))`
                : `${output.type.storage}(${reduceOutputValues[reduceType]})`
            }`,
          )};
         }
        }`;

  // One work group is responsible for only one element of output.
  return {
    name,
    // Note that in JSEP, WG size is not included in cache by default, but WebGPU EP it is.
    shaderCache: { hint: `${cacheKey};${workgroupSize}`, inputDependencies: ['type'] },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: outputDataType }],
      dispatchGroup: { x: outputSize },
      programUniforms: [{ type: DataType.uint32, data: reduceSize }],
    }),
  };
};

const reduceCommon = (
  context: ComputeContext,
  name: string,
  attributes: ReduceAttributes,
  reduceType: 'sum' | 'sumSquare' | 'prod' | 'min' | 'max' | 'mean' | 'logSumExp' | 'l1' | 'l2' | 'logSum',
): void => {
  const updatedAttributes: ReduceAttributes =
    context.inputs.length === 1 ? attributes : createReduceAttributesFromInputs(context.inputs, attributes);

  let updatedAxes = updatedAttributes.axes;
  if (updatedAxes.length === 0 && !updatedAttributes.noopWithEmptyAxes) {
    updatedAxes = context.inputs[0].dims.map((_dim, i) => i);
  }
  const normalizeAxes = ShapeUtil.normalizeAxes(updatedAxes, context.inputs[0].dims.length);

  let axes = normalizeAxes;
  let input = context.inputs[0];
  const permutedAxes = getAxesPermutation(axes, context.inputs[0].dims.length);
  if (permutedAxes.length > 0) {
    input = context.compute(createTransposeProgramInfo(context.inputs[0], permutedAxes), {
      inputs: [0],
      outputs: [-1],
    })[0];
    axes = getInnerMostAxes(axes.length, input.dims.length);
  }

  const [outputShape, reduceShape] = computeOutAndReduceShapes(input.dims, axes);
  let finalOutputShape = outputShape;
  if (updatedAttributes.keepDims) {
    finalOutputShape = expandShapeToKeepDim(outputShape, normalizeAxes);
  }

  context.compute(
    createReduceSharedProgramInfo(
      name,
      updatedAttributes.cacheKey,
      [input],
      reduceType,
      context.inputs[0].dataType,
      finalOutputShape,
      reduceShape,
    ),
    { inputs: [input] },
  );
};

export const reduceMeanShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceMeanShared', attributes, 'mean');
};

export const reduceL1Shared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceL1Shared', attributes, 'l1');
};

export const reduceL2Shared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceL2Shared', attributes, 'l2');
};

export const reduceLogSumExpShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceLogSumExpShared', attributes, 'logSumExp');
};

export const reduceMaxShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceMaxShared', attributes, 'max');
};

export const reduceMinShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceMinShared', attributes, 'min');
};

export const reduceProdShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceProdShared', attributes, 'prod');
};

export const reduceSumShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceSumShared', attributes, 'sum');
};

export const reduceSumSquareShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceSumSquareShared', attributes, 'sumSquare');
};

export const reduceLogSumShared = (context: ComputeContext, attributes: ReduceAttributes): void => {
  reduceCommon(context, 'ReduceLogSumShared', attributes, 'logSum');
};
