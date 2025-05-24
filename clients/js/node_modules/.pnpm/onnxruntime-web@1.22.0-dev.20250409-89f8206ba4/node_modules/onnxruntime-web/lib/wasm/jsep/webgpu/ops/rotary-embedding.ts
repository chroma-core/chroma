// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper, WORKGROUP_SIZE } from './common';

export interface RotaryEmbeddingAttributes {
  readonly interleaved: boolean;
  readonly numHeads: number;
  readonly rotaryEmbeddingDim: number;
  readonly scale: number;
}

const validateInputs = (inputs: readonly TensorView[], attributes: RotaryEmbeddingAttributes): void => {
  const [input, positionIds, cosCache, sinCache] = inputs;
  const { numHeads, rotaryEmbeddingDim } = attributes;

  if (input.dims.length !== 3 && input.dims.length !== 4) {
    throw new Error(`Input 'x' is expected to have 3 or 4 dimensions, got ${input.dims.length}`);
  }
  if (
    !ShapeUtil.areEqual(positionIds.dims, []) &&
    !ShapeUtil.areEqual(positionIds.dims, [1]) &&
    positionIds.dims.length !== 2
  ) {
    throw new Error(`Input 'position_ids' is expected to have 0, 1, or 2 dimensions, got ${positionIds.dims.length}`);
  }
  if (cosCache.dims.length !== 2) {
    throw new Error(`Input 'cos_cache' is expected to have 2 dimensions, got ${cosCache.dims.length}`);
  }
  if (sinCache.dims.length !== 2) {
    throw new Error(`Input 'sin_cache' is expected to have 2 dimensions, got ${sinCache.dims.length}`);
  }
  if (!ShapeUtil.areEqual(cosCache.dims, sinCache.dims)) {
    throw new Error("Inputs 'cos_cache' and 'sin_cache' are expected to have the same shape");
  }

  if (rotaryEmbeddingDim > 0 && numHeads === 0) {
    throw new Error('num_heads must be provided if rotary_embedding_dim is specified');
  }

  const batchSize = input.dims[0];
  const sequenceLength = input.dims[input.dims.length - 2];
  const maxSequenceLength = cosCache.dims[0];
  const hiddenSize = ShapeUtil.sizeFromDimension(input.dims, 1) / sequenceLength;
  const headSize = rotaryEmbeddingDim === 0 ? cosCache.dims[1] * 2 : hiddenSize / numHeads;
  if (rotaryEmbeddingDim > headSize) {
    throw new Error('rotary_embedding_dim must be less than or equal to head_size');
  }

  if (positionIds.dims.length === 2) {
    if (batchSize !== positionIds.dims[0]) {
      throw new Error(`Input 'position_ids' dimension 0 should be of size batch_size, got ${positionIds.dims[0]}`);
    }
    if (sequenceLength !== positionIds.dims[1]) {
      throw new Error(`Input 'position_ids' dimension 1 should be of size sequence_length, got ${positionIds.dims[1]}`);
    }
  }

  if (headSize / 2 !== cosCache.dims[1] && rotaryEmbeddingDim / 2 !== cosCache.dims[1]) {
    throw new Error(
      `Input 'cos_cache' dimension 1 should be same as head_size / 2 or rotary_embedding_dim / 2, got ${
        cosCache.dims[1]
      }`,
    );
  }

  if (sequenceLength > maxSequenceLength) {
    throw new Error('Updating cos_cache and sin_cache in RotaryEmbedding is not currently supported');
  }
};

export const createRotaryEmbeddingProgramInfo = (
  inputs: readonly TensorView[],
  attributes: RotaryEmbeddingAttributes,
): ProgramInfo => {
  const { interleaved, numHeads, rotaryEmbeddingDim, scale } = attributes;
  const batchSize = inputs[0].dims[0];
  const batchStride = ShapeUtil.sizeFromDimension(inputs[0].dims, 1);
  const sequenceLength = inputs[0].dims[inputs[0].dims.length - 2];
  const hiddenSize = batchStride / sequenceLength;
  const halfRotaryEmbeddingDim = inputs[2].dims[1];
  const headSize = rotaryEmbeddingDim === 0 ? halfRotaryEmbeddingDim * 2 : hiddenSize / numHeads;

  // Rotary embeddings will be calculated in a pair-wise fashion. In accordance, use the shape
  // [batch size, sequence length, num of heads, num of pairs to rotate + num of dims to copy]
  // to unfold the global index in shader.
  const globalShape = new Array<number>(
    batchSize,
    sequenceLength,
    hiddenSize / headSize,
    headSize - halfRotaryEmbeddingDim,
  );
  const globalStrides = ShapeUtil.computeStrides(globalShape);

  const programUniforms: ProgramUniform[] = [
    { type: DataType.float, data: scale },
    { type: DataType.uint32, data: globalShape },
    { type: DataType.uint32, data: globalStrides },

    // strides for addressing the input/output tensor, in permutated order to align with the unfolded global index,
    // i.e. BSNH
    ...(inputs[0].dims.length === 3
      ? new Array<ProgramUniform>({ type: DataType.uint32, data: [batchStride, hiddenSize, headSize, 1] })
      : []),
    ...(inputs[0].dims.length === 4
      ? new Array<ProgramUniform>({
          type: DataType.uint32,
          data: [batchStride, headSize, sequenceLength * headSize, 1],
        })
      : []),

    ...createTensorShapeVariables(inputs[0].dims, inputs[1].dims, inputs[2].dims, inputs[3].dims, inputs[0].dims),
  ];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const input = inputVariable('input', inputs[0].dataType, inputs[0].dims.length);
    const positionIds = inputVariable('position_ids', inputs[1].dataType, inputs[1].dims.length);
    const cosCache = inputVariable('cos_cache', inputs[2].dataType, inputs[2].dims.length);
    const sinCache = inputVariable('sin_cache', inputs[3].dataType, inputs[3].dims.length);
    const output = outputVariable('output', inputs[0].dataType, inputs[0].dims.length);

    shaderHelper.registerUniforms([
      { name: 'scale', type: 'f32' },
      { name: 'global_shape', type: 'u32', length: globalShape.length },
      { name: 'global_strides', type: 'u32', length: globalStrides.length },
      { name: 'input_output_strides', type: 'u32', length: globalStrides.length },
    ]);

    return `
        ${shaderHelper.declareVariables(input, positionIds, cosCache, sinCache, output)}

        ${shaderHelper.mainStart(WORKGROUP_SIZE)}
          let half_rotary_emb_dim = uniforms.${cosCache.name}_shape[1];
          let bsnh = global_idx / uniforms.global_strides % uniforms.global_shape;
          let size = uniforms.global_shape[0] * uniforms.global_strides[0];
          ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('size')}

          if (bsnh[3] < half_rotary_emb_dim) {
            let position_ids_idx =
                ${positionIds.broadcastedIndicesToOffset('bsnh.xy', outputVariable('', positionIds.type.tensor, 2))};
            let position_id =
                u32(${positionIds.getByOffset('position_ids_idx')}) + select(0, bsnh[1], position_ids_idx == 0);
            let i = dot(bsnh, uniforms.input_output_strides) + select(0, bsnh[3], ${interleaved});
            let j = i + select(half_rotary_emb_dim, 1, ${interleaved});
            let re = ${input.getByOffset('i')} * ${cosCache.get('position_id', 'bsnh[3]')} -
                ${input.getByOffset('j')} * ${sinCache.get('position_id', 'bsnh[3]')};
            ${output.setByOffset('i', 're')}
            let im = ${input.getByOffset('i')} * ${sinCache.get('position_id', 'bsnh[3]')} +
                ${input.getByOffset('j')} * ${cosCache.get('position_id', 'bsnh[3]')};
            ${output.setByOffset('j', 'im')}
          } else {
            let k = dot(bsnh, uniforms.input_output_strides) + half_rotary_emb_dim;
            ${output.setByOffset('k', input.getByOffset('k'))}
          }
        }`;
  };

  return {
    name: 'RotaryEmbedding',
    shaderCache: {
      hint: createAttributeWithCacheKey({
        interleaved,
      }).cacheKey,
      inputDependencies: ['rank', 'rank', 'rank', 'rank'],
    },
    getShaderSource,
    getRunData: () => ({
      outputs: [{ dims: inputs[0].dims, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(ShapeUtil.size(globalShape) / WORKGROUP_SIZE) },
      programUniforms,
    }),
  };
};

export const rotaryEmbedding = (context: ComputeContext, attributes: RotaryEmbeddingAttributes): void => {
  validateInputs(context.inputs, attributes);
  context.compute(createRotaryEmbeddingProgramInfo(context.inputs, attributes));
};
