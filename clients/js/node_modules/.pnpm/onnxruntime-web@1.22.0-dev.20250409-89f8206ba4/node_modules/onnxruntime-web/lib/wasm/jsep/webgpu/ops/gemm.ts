// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { GemmUtil, ShapeUtil } from '../../util';
import { AttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  IndicesHelper,
  inputVariable,
  outputVariable,
  ShaderHelper,
  UniformsArrayType,
} from './common';

const validateInputs = (inputs: readonly TensorView[]): void => {
  if (!inputs) {
    throw new Error('Input is missing');
  }
  if (inputs.length < 2 || inputs.length > 3) {
    throw new Error('Invaid input number.');
  }

  // 'C' can be of dimensionality 0, 1 or 2 only
  if (inputs.length === 3 && inputs[2].dims.length > 2) {
    throw new Error('Invalid input shape of C');
  }

  if (inputs[0].dataType !== inputs[1].dataType || (inputs.length === 3 && inputs[0].dataType !== inputs[2].dataType)) {
    throw new Error('Input types are mismatched');
  }
};

export interface GemmAttributes extends AttributeWithCacheKey {
  transA: boolean;
  transB: boolean;
  alpha: number;
  beta: number;
}

const createGemmProgramInfo = (inputs: readonly TensorView[], attributes: GemmAttributes): ProgramInfo => {
  const aShape = inputs[0].dims.slice();
  const bShape = inputs[1].dims.slice();
  const [M, N, K] = GemmUtil.getShapeOfGemmResult(
    aShape,
    attributes.transA,
    bShape,
    attributes.transB,
    inputs.length === 3 ? inputs[2].dims : undefined,
  );
  const outputShape = [M, N];
  if (!outputShape) {
    throw new Error("Can't use gemm on the given tensors");
  }
  const tileSize = 16;
  const numTileN = Math.ceil(N / tileSize);
  const numTileM = Math.ceil(M / tileSize);
  // TODO: Find the condition when to use the naive one.
  const useShared = true;

  const outputSize = ShapeUtil.size(outputShape);
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: useShared ? numTileN : outputSize },
    { type: DataType.uint32, data: M },
    { type: DataType.uint32, data: N },
    { type: DataType.uint32, data: K },
    { type: DataType.float, data: attributes.alpha },
    { type: DataType.float, data: attributes.beta },
  ];
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['type', 'type'];
  if (inputs.length === 3) {
    programUniforms.push(...createTensorShapeVariables(inputs[2].dims));
    inputDependencies.push('rank');
  }
  programUniforms.push(...createTensorShapeVariables(outputShape));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    let line = '';
    if (attributes.transA && attributes.transB) {
      line = 'value += a[k * uniforms.M + m] * b[n * uniforms.K + k];';
    } else if (attributes.transA && !attributes.transB) {
      line = 'value += a[k * uniforms.M + m] * b[k * uniforms.N + n];';
    } else if (!attributes.transA && attributes.transB) {
      line = 'value += a[m * uniforms.K + k] * b[n * uniforms.K + k];';
    } else if (!attributes.transA && !attributes.transB) {
      line = 'value += a[m * uniforms.K + k] * b[k * uniforms.N + n];';
    }

    const calculateAlpha = attributes.alpha === 1 ? '' : 'value *= uniforms.alpha;';
    const a = inputVariable('a', inputs[0].dataType, inputs[0].dims);
    const b = inputVariable('b', inputs[1].dataType, inputs[1].dims);
    const dataType = a.type.value;
    let c: IndicesHelper | null = null;
    const variables = [a, b];
    if (inputs.length === 3) {
      c = inputVariable('c', inputs[2].dataType, inputs[2].dims.length);
      variables.push(c);
    }
    const output = outputVariable('output', inputs[0].dataType, outputShape.length);
    variables.push(output);
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'M', type: 'u32' },
      { name: 'N', type: 'u32' },
      { name: 'K', type: 'u32' },
      { name: 'alpha', type: 'f32' },
      { name: 'beta', type: 'f32' },
    ];
    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...variables)}

  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}

    let m = global_idx / uniforms.N;
    let n = global_idx % uniforms.N;

    var value = ${dataType}(0);
    for (var k: u32 = 0u; k < uniforms.K; k++) {
      ${line}
    }

    ${calculateAlpha}
    ${(() => {
      if (c != null) {
        return `let cOffset = ${c.broadcastedIndicesToOffset('vec2(m, n)', output)}; value += ${
          dataType
        }(uniforms.beta) * ${c.getByOffset('cOffset')};`;
      }
      return '';
    })()}
    output[global_idx] = value;
  }`;
  };

  const getShaderSourceShared = (shaderHelper: ShaderHelper) => {
    const a = inputVariable('a', inputs[0].dataType, inputs[0].dims);
    const b = inputVariable('b', inputs[1].dataType, inputs[1].dims);
    let c: IndicesHelper | null = null;
    const variables = [a, b];
    if (inputs.length === 3) {
      c = inputVariable('c', inputs[2].dataType, inputs[2].dims.length);
      variables.push(c);
    }
    const output = outputVariable('output', inputs[0].dataType, outputShape.length);
    variables.push(output);
    const uniforms: UniformsArrayType = [
      { name: 'num_tile_n', type: 'u32' },
      { name: 'M', type: 'u32' },
      { name: 'N', type: 'u32' },
      { name: 'K', type: 'u32' },
      { name: 'alpha', type: 'f32' },
      { name: 'beta', type: 'f32' },
    ];

    let calcResult = '';
    let fillWorkgroupMemory = '';
    if (attributes.transA && attributes.transB) {
      fillWorkgroupMemory = `
      var col = tile_row_start + local_id.x;
      var row = k_start + local_id.y;
      if (col < uniforms.M && row < uniforms.K) {
        tile_a[local_id.y][local_id.x] = a[row * uniforms.M + col];
      } else {
        tile_a[local_id.y][local_id.x] = ${a.type.value}(0);
      }

      col = k_start + local_id.x;
      row = tile_col_start + local_id.y;
      if (col < uniforms.K && row < uniforms.N) {
        tile_b[local_id.y][local_id.x] = b[row * uniforms.K + col];
      } else {
        tile_b[local_id.y][local_id.x] = ${b.type.value}(0);
      }
      `;
      calcResult = `value += tile_a[k][local_id.y] * tile_b[local_id.x][k];`;
    } else if (attributes.transA && !attributes.transB) {
      fillWorkgroupMemory = `
      var col = tile_row_start + local_id.x;
      var row = k_start + local_id.y;
      if (col < uniforms.M && row < uniforms.K) {
        tile_a[local_id.y][local_id.x] = a[row * uniforms.M + col];
      } else {
        tile_a[local_id.y][local_id.x] = ${a.type.value}(0);
      }

      col = tile_col_start + local_id.x;
      row = k_start + local_id.y;
      if (col < uniforms.N && row < uniforms.K) {
        tile_b[local_id.y][local_id.x] = b[row * uniforms.N + col];
      } else {
        tile_b[local_id.y][local_id.x] = ${b.type.value}(0);
      }
      `;
      calcResult = `value += tile_a[k][local_id.y] * tile_b[k][local_id.x];`;
    } else if (!attributes.transA && attributes.transB) {
      fillWorkgroupMemory = `
      var col = k_start + local_id.x;
      var row = tile_row_start + local_id.y;
      if (col < uniforms.K && row < uniforms.M) {
        tile_a[local_id.y][local_id.x] = a[row * uniforms.K + col];
      } else {
        tile_a[local_id.y][local_id.x] = ${a.type.value}(0);
      }

      col = k_start + local_id.x;
      row = tile_col_start + local_id.y;
      if (col < uniforms.K && row < uniforms.N) {
        tile_b[local_id.y][local_id.x] = b[row * uniforms.K + col];
      } else {
        tile_b[local_id.y][local_id.x] = ${b.type.value}(0);
      }
      `;
      calcResult = `value += tile_a[local_id.y][k] * tile_b[local_id.x][k];`;
    } else if (!attributes.transA && !attributes.transB) {
      fillWorkgroupMemory = `
      var col = k_start + local_id.x;
      var row = tile_row_start + local_id.y;
      if (col < uniforms.K && row < uniforms.M) {
        tile_a[local_id.y][local_id.x] = a[row * uniforms.K + col];
      } else {
        tile_a[local_id.y][local_id.x] = ${a.type.value}(0);
      }

      col = tile_col_start + local_id.x;
      row = k_start + local_id.y;
      if (col < uniforms.N && row < uniforms.K) {
        tile_b[local_id.y][local_id.x] = b[row * uniforms.N + col];
      } else {
        tile_b[local_id.y][local_id.x] = ${b.type.value}(0);
      }
      `;
      calcResult = `value += tile_a[local_id.y][k] * tile_b[k][local_id.x];`;
    }

    const calculateAlpha = attributes.alpha === 1 ? '' : 'value *= uniforms.alpha;';

    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...variables)}
  var<workgroup> tile_a: array<array<${a.type.storage}, ${tileSize}>, ${tileSize}>;
  var<workgroup> tile_b: array<array<${b.type.storage}, ${tileSize}>, ${tileSize}>;
  ${shaderHelper.mainStart([tileSize, tileSize, 1])}
    let tile_col_start = (workgroup_index % uniforms.num_tile_n) * ${tileSize};
    let tile_row_start = (workgroup_index / uniforms.num_tile_n) * ${tileSize};
    let num_tiles = (uniforms.K - 1) / ${tileSize} + 1;
    var k_start = 0u;
    var value = ${output.type.value}(0);
    for (var t: u32 = 0u; t < num_tiles; t++) {
      ${fillWorkgroupMemory}
      k_start = k_start + ${tileSize};
      workgroupBarrier();

      for (var k: u32 = 0u; k < ${tileSize}; k++) {
        ${calcResult}
      }
      workgroupBarrier();
    }

    ${calculateAlpha}
    let m = tile_row_start + local_id.y;
    let n = tile_col_start + local_id.x;
    ${(() => {
      if (c != null) {
        return `let cOffset = ${c.broadcastedIndicesToOffset('vec2(m, n)', output)}; value += ${
          output.type.value
        }(uniforms.beta) * ${c.getByOffset('cOffset')};`;
      }
      return '';
    })()}
    if (m < uniforms.M && n < uniforms.N) {
      output[m * uniforms.N + n] = value;
    }
  }`;
  };

  if (useShared) {
    return {
      name: 'GemmShared',
      shaderCache: { hint: `${attributes.cacheKey}`, inputDependencies },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
        dispatchGroup: { x: numTileN * numTileM },
        programUniforms,
      }),
      getShaderSource: getShaderSourceShared,
    };
  }

  return {
    name: 'Gemm',
    shaderCache: { hint: `${attributes.cacheKey}`, inputDependencies },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const parseGemmAttributes = (attributes: Record<string, unknown>): GemmAttributes => {
  const transA = attributes.transA as boolean;
  const transB = attributes.transB as boolean;
  const alpha = attributes.alpha as number;
  const beta = attributes.beta as number;
  return {
    transA,
    transB,
    alpha,
    beta,
    cacheKey: `${attributes.transA};${attributes.transB};${attributes.alpha === 1}`,
  };
};

export const gemm = (context: ComputeContext, attributes: GemmAttributes): void => {
  validateInputs(context.inputs);
  context.compute(createGemmProgramInfo(context.inputs, attributes));
};
