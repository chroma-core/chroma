// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { BroadcastUtil, ShapeUtil } from '../../util';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper } from './common';

type BuiltinFunctionName = string;
type BinaryCustomExpression = (expressionA: string, expressionB: string) => string;
type BinaryFunctionCall =
  | BuiltinFunctionName
  | BinaryCustomExpression
  | {
      scalar: BinaryCustomExpression;
      vector: BinaryCustomExpression;
    };

const createBinaryOpProgramShader = (
  shaderHelper: ShaderHelper,
  dimsA: readonly number[],
  dimsB: readonly number[],
  dimsOutput: readonly number[],
  vectorize: boolean,
  doBroadcast: boolean,
  sharedDimensionDivisibleBy4: boolean,
  funcCall: BinaryFunctionCall,
  typeA: number,
  typeB: number,
  typeOutput: number,
  additionalImplementation?: string,
) => {
  let expressionScalar: BinaryCustomExpression;
  let expressionVector: BinaryCustomExpression;
  if (typeof funcCall === 'string') {
    expressionScalar = expressionVector = (a, b) => `${funcCall}((${a}),(${b}))`;
  } else if (typeof funcCall === 'function') {
    expressionScalar = expressionVector = funcCall;
  } else {
    expressionScalar = funcCall.scalar;
    expressionVector = funcCall.vector;
  }

  const output = outputVariable('outputData', typeOutput, dimsOutput.length, 4);
  const a = inputVariable('aData', typeA, dimsA.length, 4);
  const b = inputVariable('bData', typeB, dimsB.length, 4);

  let assignment: string;
  if (vectorize) {
    if (doBroadcast) {
      const isAOneElement = ShapeUtil.size(dimsA) === 1;
      const isBOneElement = ShapeUtil.size(dimsB) === 1;
      const aLastDimDivisibleBy4 = dimsA.length > 0 && dimsA[dimsA.length - 1] % 4 === 0;
      const bLastDimDivisibleBy4 = dimsB.length > 0 && dimsB[dimsB.length - 1] % 4 === 0;
      if (isAOneElement || isBOneElement) {
        assignment = output.setByOffset(
          'global_idx',
          expressionVector(
            isAOneElement ? `${a.type.value}(${a.getByOffset('0')}.x)` : a.getByOffset('global_idx'),
            isBOneElement ? `${b.type.value}(${b.getByOffset('0')}.x)` : b.getByOffset('global_idx'),
          ),
        );
      } else {
        assignment = `
            let outputIndices = ${output.offsetToIndices('global_idx * 4u')};
            let offsetA = ${a.broadcastedIndicesToOffset('outputIndices', output)};
            let offsetB = ${b.broadcastedIndicesToOffset('outputIndices', output)};
            ${output.setByOffset(
              'global_idx',
              expressionVector(
                sharedDimensionDivisibleBy4 || aLastDimDivisibleBy4
                  ? a.getByOffset('offsetA / 4u')
                  : `${a.type.value}(${a.getByOffset('offsetA / 4u')}[offsetA % 4u])`,
                sharedDimensionDivisibleBy4 || bLastDimDivisibleBy4
                  ? b.getByOffset('offsetB / 4u')
                  : `${b.type.value}(${b.getByOffset('offsetB / 4u')}[offsetB % 4u])`,
              ),
            )}
          `;
      }
    } else {
      assignment = output.setByOffset(
        'global_idx',
        expressionVector(a.getByOffset('global_idx'), b.getByOffset('global_idx')),
      );
    }
  } else {
    if (!doBroadcast) {
      throw new Error('no necessary to use scalar implementation for element-wise binary op implementation.');
    }

    const singleAssignment = (resStr: string, x: number, typeCast = '') => {
      const expressionA = `aData[indexA${x}][componentA${x}]`;
      const expressionB = `bData[indexB${x}][componentB${x}]`;
      return `
            let outputIndices${x} = ${output.offsetToIndices(`global_idx * 4u + ${x}u`)};
            let offsetA${x} = ${a.broadcastedIndicesToOffset(`outputIndices${x}`, output)};
            let offsetB${x} = ${b.broadcastedIndicesToOffset(`outputIndices${x}`, output)};
            let indexA${x} = offsetA${x} / 4u;
            let indexB${x} = offsetB${x} / 4u;
            let componentA${x} = offsetA${x} % 4u;
            let componentB${x} = offsetB${x} % 4u;
            ${resStr}[${x}] = ${typeCast}(${expressionScalar(expressionA, expressionB)});
          `;
    };
    if (typeOutput === DataType.bool) {
      assignment = `
            var data = vec4<u32>(0);
            ${singleAssignment('data', 0, 'u32')}
            ${singleAssignment('data', 1, 'u32')}
            ${singleAssignment('data', 2, 'u32')}
            ${singleAssignment('data', 3, 'u32')}
            outputData[global_idx] = dot(vec4<u32>(0x1, 0x100, 0x10000, 0x1000000), vec4<u32>(data));`;
    } else {
      assignment = `
            ${singleAssignment('outputData[global_idx]', 0)}
            ${singleAssignment('outputData[global_idx]', 1)}
            ${singleAssignment('outputData[global_idx]', 2)}
            ${singleAssignment('outputData[global_idx]', 3)}
          `;
    }
  }

  return `
        ${shaderHelper.registerUniform('vec_size', 'u32').declareVariables(a, b, output)}

        ${additionalImplementation ?? ''}

        ${shaderHelper.mainStart()}
        ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.vec_size')}
        ${assignment}
      }`;
};

const createBinaryOpProgramInfo = (
  name: string,
  cacheKey: string,
  a: TensorView,
  b: TensorView,
  funcCall: BinaryFunctionCall,
  additionalImplementation?: string,
  outputDataType: number = a.dataType,
): ProgramInfo => {
  const aDims = a.dims.map((x) => Number(x) ?? 1);
  const bDims = b.dims.map((x) => Number(x) ?? 1);
  const isBroadcast = !ShapeUtil.areEqual(aDims, bDims);
  let outputShape = aDims;
  let outputSize = ShapeUtil.size(aDims);

  let vectorize = false;
  let sharedDimensionDivisibleBy4 = false;

  // TODO: deal with zero-sized tensors (eg. dims=[1,0])
  const cacheKeyAux = [isBroadcast];
  if (isBroadcast) {
    const calculatedShape = BroadcastUtil.calcShape(aDims, bDims, false);
    if (!calculatedShape) {
      throw new Error("Can't perform binary op on the given tensors");
    }
    outputShape = calculatedShape.slice();
    outputSize = ShapeUtil.size(outputShape);
    const isAOneElement = ShapeUtil.size(aDims) === 1;
    const isBOneElement = ShapeUtil.size(bDims) === 1;
    const aLastDimDivisibleBy4 = aDims.length > 0 && aDims[aDims.length - 1] % 4 === 0;
    const bLastDimDivisibleBy4 = bDims.length > 0 && bDims[bDims.length - 1] % 4 === 0;
    cacheKeyAux.push(isAOneElement);
    cacheKeyAux.push(isBOneElement);
    cacheKeyAux.push(aLastDimDivisibleBy4);
    cacheKeyAux.push(bLastDimDivisibleBy4);
    // check whether vectorize can be enabled
    let sharedDimension = 1;
    for (let i = 1; i < outputShape.length; i++) {
      const dimA = aDims[aDims.length - i];
      const dimB = bDims[bDims.length - i];
      if (dimA === dimB) {
        sharedDimension *= dimA;
      } else {
        break;
      }
    }
    if (sharedDimension % 4 === 0) {
      sharedDimensionDivisibleBy4 = true;
      vectorize = true;
    } else if (isAOneElement || isBOneElement || aLastDimDivisibleBy4 || bLastDimDivisibleBy4) {
      vectorize = true;
    }
  } else {
    // element-wise
    vectorize = true;
  }
  cacheKeyAux.push(vectorize);

  return {
    name,
    shaderCache: {
      hint: cacheKey + cacheKeyAux.map((x) => x.toString()).join('_'),
      inputDependencies: ['rank', 'rank'],
    },
    getShaderSource: (shaderHelper) =>
      createBinaryOpProgramShader(
        shaderHelper,
        aDims,
        bDims,
        outputShape,
        vectorize,
        isBroadcast,
        sharedDimensionDivisibleBy4,
        funcCall,
        a.dataType,
        b.dataType,
        outputDataType,
        additionalImplementation,
      ),
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: outputDataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */ / 4 /* component size */) },
      programUniforms: [
        { type: DataType.uint32, data: Math.ceil(ShapeUtil.size(outputShape) / 4) },
        ...createTensorShapeVariables(aDims, bDims, outputShape),
      ],
    }),
  };
};

const runBinaryOp = (
  context: ComputeContext,
  name: string,
  funcCall: BinaryFunctionCall,
  additionalImplementation?: string,
  cacheKey?: string,
  outputDataType?: number,
): void => {
  context.compute(
    createBinaryOpProgramInfo(
      name,
      cacheKey ?? '',
      context.inputs[0],
      context.inputs[1],
      funcCall,
      additionalImplementation,
      outputDataType,
    ),
  );
};

export const add = (context: ComputeContext): void => {
  runBinaryOp(context, 'Add', (a, b) => `${a}+${b}`);
};

export const div = (context: ComputeContext): void => {
  runBinaryOp(context, 'Div', (a, b) => `${a}/${b}`);
};

export const equal = (context: ComputeContext): void => {
  runBinaryOp(
    context,
    'Equal',
    { scalar: (a, b) => `u32(${a}==${b})`, vector: (a, b) => `vec4<u32>(${a}==${b})` },
    undefined,
    undefined,
    DataType.bool,
  );
};

export const mul = (context: ComputeContext): void => {
  runBinaryOp(context, 'Mul', (a, b) => `${a}*${b}`);
};

export const pow = (context: ComputeContext): void => {
  const type = inputVariable('input', context.inputs[0].dataType, context.inputs[0].dims).type.value;
  const roundStr = type === 'i32' ? 'round' : '';
  runBinaryOp(
    context,
    'Pow',
    { scalar: (a, b) => `pow_custom(${a},${b})`, vector: (a, b) => `pow_vector_custom(${a},${b})` },
    `
    fn pow_custom(a : ${type}, b : ${type}) -> ${type} {
      if (b == ${type}(0.0)) {
        return ${type}(1.0);
      } else if (a < ${type}(0.0) && f32(b) != floor(f32(b))) {
        return ${type}(pow(f32(a), f32(b))); // NaN
      }
      return select(sign(a), ${type}(1.0), round(f32(abs(b) % ${type}(2.0))) != 1.0) * ${type}(${roundStr}(pow(f32(abs(a)), f32(b))));
    }
    fn pow_vector_custom(a : vec4<${type}>, b : vec4<${type}>) -> vec4<${type}> {
      // TODO: implement vectorized pow
      return vec4<${type}>(pow_custom(a.x, b.x), pow_custom(a.y, b.y), pow_custom(a.z, b.z), pow_custom(a.w, b.w));
    }
      `,
  );
};

export const sub = (context: ComputeContext): void => {
  runBinaryOp(context, 'Sub', (a, b) => `${a}-${b}`);
};

export const greater = (context: ComputeContext): void => {
  runBinaryOp(
    context,
    'Greater',
    { scalar: (a, b) => `u32(${a}>${b})`, vector: (a, b) => `vec4<u32>(${a}>${b})` },
    undefined,
    undefined,
    DataType.bool,
  );
};

export const less = (context: ComputeContext): void => {
  runBinaryOp(
    context,
    'Less',
    { scalar: (a, b) => `u32(${a}<${b})`, vector: (a, b) => `vec4<u32>(${a}<${b})` },
    undefined,
    undefined,
    DataType.bool,
  );
};

export const greaterOrEqual = (context: ComputeContext): void => {
  runBinaryOp(
    context,
    'GreaterOrEqual',
    { scalar: (a, b) => `u32(${a}>=${b})`, vector: (a, b) => `vec4<u32>(${a}>=${b})` },
    undefined,
    undefined,
    DataType.bool,
  );
};

export const lessOrEqual = (context: ComputeContext): void => {
  runBinaryOp(
    context,
    'LessOrEqual',
    { scalar: (a, b) => `u32(${a}<=${b})`, vector: (a, b) => `vec4<u32>(${a}<=${b})` },
    undefined,
    undefined,
    DataType.bool,
  );
};
