// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ProgramInfo, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getElementAt,
  getMaxComponents,
  IndicesHelper,
  inputVariable,
  internalVariable,
  outputVariable,
  ShaderHelper,
  tensorTypeToWsglStorageType,
  UniformsArrayType,
} from './common';
import {
  appendActivationUniforms,
  appendActivationUniformsData,
  getActivationSnippet,
  InternalActivationAttributes,
} from './fuse-utils';

// Helper that convert output batch indices to input batch indices using only the rank and
// the shape information in uniform
export const convertOutputBatchIndicesToInputBatchIndices = (
  targetIndicesName: string,
  inputVariable: IndicesHelper,
  inputBatchRank: number,
  outputBatchRank: number,
  batchIndicesName: string,
) => {
  // Assume outputBatchRank >= inputBatchRank, the first outputBatchRank - inputBatchRank of
  // outputBatchRank should be ignored.
  const extendingInputRank = outputBatchRank - inputBatchRank;
  return `
      ${Array.from({ length: inputBatchRank })
        .map(
          (_, i) => `
      if (${getElementAt(inputVariable.shape, i, inputVariable.rank)} != 1) {
        ${inputVariable.indicesSet(targetIndicesName, i, getElementAt(batchIndicesName, i + extendingInputRank, outputBatchRank))}
      } else {
        ${inputVariable.indicesSet(targetIndicesName, i, 0)}
      }`,
        )
        .join('')}
`;
};

export const createNaiveMatmulProgramInfo = (
  inputs: readonly TensorView[],
  activationAttributes: InternalActivationAttributes,
  outputShape: readonly number[],
  reshapedOutputShape?: readonly number[],
  isChannelsLast = false /* only used for conv2dByMatMul*/,
  squeezeOutputShapeFunction?: (shape: readonly number[]) => number[],
): ProgramInfo => {
  const aShape = inputs[0].dims;
  const bShape = inputs[1].dims;

  const M = aShape[aShape.length - 2];
  const N = bShape[bShape.length - 1];
  const K = aShape[aShape.length - 1];
  const components = getMaxComponents(N);
  const aComponents = getMaxComponents(K);
  const outputNumber = getMaxComponents(M);
  const outputSize = ShapeUtil.size(outputShape) / components / outputNumber;
  const hasBias = inputs.length > 2;
  const outerDims = reshapedOutputShape ? reshapedOutputShape.slice(0, -2) : outputShape.slice(0, -2);
  const batchSize = ShapeUtil.size(outerDims);
  const outputShapeInShader = [batchSize, M, N];

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: M },
    { type: DataType.uint32, data: N },
    { type: DataType.uint32, data: K },
  ];
  appendActivationUniformsData(activationAttributes, programUniforms);
  programUniforms.push(...createTensorShapeVariables(outerDims, aShape, bShape));
  if (hasBias) {
    programUniforms.push(...createTensorShapeVariables(inputs[2].dims));
  }
  programUniforms.push(...createTensorShapeVariables(outputShapeInShader));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const batchDims = internalVariable('batch_dims', inputs[0].dataType, outerDims.length);
    const a = inputVariable('a', inputs[0].dataType, aShape.length, aComponents);
    const b = inputVariable('b', inputs[1].dataType, bShape.length, components);
    const output = outputVariable('output', inputs[0].dataType, outputShapeInShader.length, components);
    const baseType = tensorTypeToWsglStorageType(output.type.tensor);
    const applyActivation = getActivationSnippet(activationAttributes, output.type.value, baseType);
    const inputVariables = [a, b];
    let processBias = '';
    if (hasBias) {
      const biasComponents = isChannelsLast ? components : 1;
      inputVariables.push(inputVariable('bias', inputs[2].dataType, inputs[2].dims.length, biasComponents));
      processBias = `${
        isChannelsLast ? `value += bias[col / ${biasComponents}];` : `value += ${output.type.value}(bias[row + i]);`
      }`;
    }

    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'M', type: 'u32' },
      { name: 'N', type: 'u32' },
      { name: 'K', type: 'u32' },
    ];
    appendActivationUniforms(activationAttributes, uniforms);

    const calcResult = (): string => {
      let calcStr = `var a_data: ${a.type.value};`;
      for (let i = 0; i < aComponents; i++) {
        calcStr += `
              let b_data${i} = b[(b_offset + (k + ${i}) * uniforms.N + col) / ${components}];`;
      }
      for (let i = 0; i < outputNumber; i++) {
        calcStr += `a_data = a[(a_offset + (row + ${i}) * uniforms.K + k) / ${aComponents}];`;

        for (let j = 0; j < aComponents; j++) {
          calcStr += `
            values[${i}] = fma(${b.type.value}(a_data${aComponents === 1 ? '' : `[${j}]`}), b_data${j}, values[${i}]);\n`;
        }
      }
      return calcStr;
    };

    return `
  ${shaderHelper
    .registerUniforms(uniforms)
    .registerInternalVariables(batchDims)
    .declareVariables(...inputVariables, output)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
    let col = (global_idx % (uniforms.N / ${components})) * ${components};
    var index1 = global_idx / (uniforms.N / ${components});
    let stride1 = uniforms.M / ${outputNumber};
    let row = (index1 % stride1) * ${outputNumber};
    let batch = index1 / stride1;

    ${outputShape.length === 2 ? '' : `let batch_indices = ${batchDims.offsetToIndices('batch')};`}

    var a_indices: ${a.type.indices};
    ${convertOutputBatchIndicesToInputBatchIndices('a_indices', a, a.rank - 2, batchDims.rank, 'batch_indices')}
    ${a.indicesSet('a_indices', a.rank - 2, 0)}
    ${a.indicesSet('a_indices', a.rank - 1, 0)}
    let a_offset = ${a.indicesToOffset('a_indices')};

    var b_indices: ${b.type.indices};
    ${convertOutputBatchIndicesToInputBatchIndices('b_indices', b, b.rank - 2, batchDims.rank, 'batch_indices')}
    ${b.indicesSet('b_indices', b.rank - 2, 0)}
    ${b.indicesSet('b_indices', b.rank - 1, 0)}
    let b_offset = ${b.indicesToOffset('b_indices')};
    var values: array<${output.type.value}, ${outputNumber}>;
    for (var k: u32 = 0u; k < uniforms.K; k = k + ${aComponents}) {
      ${calcResult()}
    }
    for (var i = 0u; i < ${outputNumber}u; i++) {
      var value = values[i];
      ${processBias}
      ${applyActivation}
      let cur_indices = ${output.type.indices}(batch, row + i, col);
      let offset = ${output.indicesToOffset('cur_indices')};
      ${output.setByOffset(`offset / ${components}`, 'value')};
    }
  }
  `;
  };
  return {
    name: 'MatMulNaive',
    shaderCache: {
      hint: `${activationAttributes.activation};${components};${aComponents};${outputNumber};${isChannelsLast}`,
      inputDependencies: hasBias ? ['rank', 'rank', 'rank'] : ['rank', 'rank'],
    },
    getRunData: () => ({
      outputs: [
        {
          dims: squeezeOutputShapeFunction ? squeezeOutputShapeFunction(outputShape) : outputShape,
          dataType: inputs[0].dataType,
        },
      ],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};
