// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';
import { DataType } from '../../../wasm-common';

import { applyAttention, AttentionMaskType, AttentionParameters, AttentionQkvFormat } from './attention';
import { maybeTransposeToBNSHAndAddBias } from './multihead-attention';
import { createSplitProgramInfo, SplitAttributes } from './split';
import { createTransposeProgramInfo, TransposeAttributes } from './transpose';
import { RotaryEmbeddingAttributes, createRotaryEmbeddingProgramInfo } from './rotary-embedding';
import { inputVariable, outputVariable, ShaderHelper, UniformsArrayType } from './common';
export interface GroupQueryAttentionAttributes {
  numHeads: number;
  kvNumHeads: number;
  scale: number;
  softcap: number;
  doRotary: number;
  rotaryInterleaved: number;
  smoothSoftmax: boolean;
  localWindowSize: number;
}

export const validateInputs = (
  inputs: readonly TensorView[],
  attributes: GroupQueryAttentionAttributes,
): AttentionParameters => {
  if (attributes.doRotary && inputs.length <= 7) {
    throw new Error('cos_cache and sin_cache inputs are required if do_rotary is specified');
  }
  const query = inputs[0];
  const key = inputs[1];
  const value = inputs[2];
  const pastKey = inputs[3];
  const pastValue = inputs[4];
  if (attributes.doRotary !== 0 && inputs.length <= 7) {
    throw new Error('cos_cast and sin_cache are expected if do_rotary attribute is non-zero');
  }
  if (attributes.localWindowSize !== -1) {
    throw new Error('Local attention is not supported');
  }
  if (attributes.softcap !== 0) {
    throw new Error('Softcap is not supported');
  }
  if (attributes.rotaryInterleaved !== 0) {
    throw new Error('Rotary interleaved is not supported');
  }
  if (attributes.smoothSoftmax) {
    throw new Error('Smooth softmax is not supported');
  }
  // Abbreviation and Meanings:
  //   B:    batch_size
  //   S:    sequence_length (input sequence length of query)
  //   P:    past_sequence_length (past sequence length of key or value)
  //   L:    kv_sequence_length (input sequence length of key or value)
  //   M:    max_sequence_length
  //   T:    total_sequence_length = past_sequence_length + kv_sequence_length
  //   N:    num_heads
  //   H:    head size for Q and K, aka q_head_size or k_head_size or qk_head_size
  //   H_v:  v_head_size
  //   D_i:  input hidden size
  //   D:    hidden size for Q and K (D = N * H), aka q_hidden_size or k_hidden_size or qk_hidden_size
  //   D_v:  v_hidden_size = num_heads * v_head_size

  //     past_key                   : (B, N, S*, H)
  //     past_value                 : (B, N, S*, H)
  // When no packing for q/k/v:
  //     query            (Q)       : (B, S, D)
  //     key              (K)       : (B, L, D) or (B, N, S*, H)
  //     value            (V)       : (B, L, D_v) or (B, N, S*, H)
  // When packed kv is used:
  //     query            (Q)       : (B, S, D)
  //     key              (K)       : (B, L, N, 2, H)
  //     value            (V)       : None
  // When packed qkv is used:
  //     query            (Q)       : (B, L, N, 3, H) or (B, S, 3*D)
  //     key              (K)       : None
  //     value            (V)       : None

  if (query.dims.length !== 3 && query.dims.length !== 5) {
    throw new Error('Input query is expected to have 3 or 5 dimensions');
  }

  const dmmhaPacking = false;
  const batchSize = query.dims[0];
  const sequenceLength = query.dims[1];
  let hiddenSize =
    query.dims.length === 3 ? (dmmhaPacking ? query.dims[2] / 3 : query.dims[2]) : attributes.numHeads * query.dims[4];
  let kvSequenceLength = sequenceLength;

  let pastSequenceLength = 0;
  const packedQKV = !key || key.dims.length === 0;
  const headSize = !packedQKV
    ? Math.floor(hiddenSize / attributes.numHeads)
    : Math.floor(hiddenSize / (attributes.numHeads + 2 * attributes.kvNumHeads));
  if (packedQKV) {
    hiddenSize = headSize * attributes.numHeads;
  }
  const hasPastKey = pastKey && pastKey.dims.length !== 0;
  const hasPastValue = pastValue && pastValue.dims.length !== 0;
  // Currenly the onnxruntime GQA specification only support key/value BNSH format.
  const isPastkvBSNH =
    hasPastKey &&
    pastKey.dims.length === 4 &&
    pastKey.dims[0] === batchSize &&
    pastKey.dims[1] !== attributes.kvNumHeads &&
    pastKey.dims[2] === attributes.kvNumHeads &&
    pastKey.dims[3] === headSize;

  if (isPastkvBSNH) {
    throw new Error('BSNH pastKey/pastValue is not supported');
  }
  if (hasPastKey && hasPastValue) {
    if (pastKey.dims.length !== 4) {
      throw new Error('Input "past_key" is expected to have 4 dimensions');
    }
    if (pastValue.dims.length !== 4) {
      throw new Error('Input "past_value" is expected to have 4 dimensions');
    }
    pastSequenceLength = pastKey.dims[2];
  } else if (hasPastKey || hasPastValue) {
    throw new Error('Input "past_key" and "past_value" shall be both present or both absent');
  }

  let qkvFormat: AttentionQkvFormat = AttentionQkvFormat.qkvBNSH;
  if (key && key.dims.length > 0) {
    if (query.dims.length !== 3) {
      throw new Error('Input "query" is expected to have 3 dimensions when key is given');
    }
    if (key.dims.length < 3 || key.dims.length > 5) {
      throw new Error('Input "key" is expected to have 3, 4, or 5 dimensions');
    }
    if (query.dims[0] !== key.dims[0]) {
      throw new Error('Input "query" and "key" shall have same dim 0 (batch size)');
    }

    if (key.dims.length === 3) {
      if (query.dims[2] % key.dims[2] !== 0) {
        throw new Error('Dimension 2 of "query" should be a multiple of "key"');
      }
      kvSequenceLength = key.dims[1];
    } else if (key.dims.length === 5) {
      if (key.dims[2] !== attributes.numHeads || key.dims[3] !== 2 || key.dims[4] !== headSize) {
        throw new Error('Expect "key" shape (batch_size, kv_sequence_length, num_heads, 2, head_size) for packed kv');
      }
      if (value) {
        throw new Error('Expect "value" be none when "key" has packed kv format.');
      }
      kvSequenceLength = key.dims[1];
    } else {
      // key_dims.size() == 4 (cross-attention with past_key)
      if (key.dims[1] !== attributes.numHeads || key.dims[3] !== headSize) {
        throw new Error('Expect "key" shape (batch_size, num_heads, kv_sequence_length, head_size) for past_key');
      }
      kvSequenceLength = key.dims[2];
    }
  } else {
    // packed QKV
    if (query.dims.length !== 3 && query.dims.length !== 5) {
      throw new Error('Input "query" is expected to have 3 or 5 dimensions when key is empty');
    }
    if (query.dims.length === 5 && (query.dims[2] !== attributes.numHeads || query.dims[3] !== 3)) {
      throw new Error('Expect "query" shape (batch_size, kv_sequence_length, num_heads, 3, head_size) for packed kv');
    }

    qkvFormat = AttentionQkvFormat.qkvBSN3H;
  }

  const maskType: AttentionMaskType = AttentionMaskType.none;
  let passPastInKv = false;
  let vHiddenSize = attributes.kvNumHeads ? headSize * attributes.kvNumHeads : hiddenSize;
  if (value && value.dims.length > 0) {
    if (value.dims.length !== 3 && value.dims.length !== 4) {
      throw new Error('Input "value" is expected to have 3 or 4 dimensions');
    }

    if (query.dims[0] !== value.dims[0]) {
      throw new Error('Input "query" and "value" shall have same dim 0 (batch_size)');
    }

    if (value.dims.length === 3) {
      if (kvSequenceLength !== value.dims[1]) {
        throw new Error('Input "key" and "value" shall have the same dim 1 (kv_sequence_length)');
      }
      vHiddenSize = value.dims[2];
    } else {
      if (kvSequenceLength !== value.dims[2]) {
        throw new Error('Input "past_key" and "past_value" shall have the same dim 2 (kv_sequence_length)');
      }
      vHiddenSize = value.dims[1] * value.dims[3];
      passPastInKv = true;
    }
  }
  const seqlLens = inputs.length > 4 ? inputs[5] : undefined;
  if (seqlLens && seqlLens.dims.length !== 1 && seqlLens.dims[0] !== batchSize) {
    throw new Error('Input "seqlens" is expected to have 1 dimension and the same dim 0 as batch_size');
  }
  const totalSequenceLength = -1;
  const maxSequenceLength = -1;
  const broadcastResPosBias = false;

  return {
    batchSize,
    sequenceLength,
    pastSequenceLength,
    kvSequenceLength,
    totalSequenceLength,
    maxSequenceLength,
    inputHiddenSize: 0,
    hiddenSize,
    vHiddenSize,
    headSize,
    vHeadSize: Math.floor(vHiddenSize / attributes.kvNumHeads),
    numHeads: attributes.numHeads,
    kvNumHeads: attributes.kvNumHeads,
    nReps: attributes.numHeads / attributes.kvNumHeads,
    pastPresentShareBuffer: false,
    maskType,
    scale: attributes.scale,
    broadcastResPosBias,
    passPastInKv,
    qkvFormat,
  };
};

const weightTransposeAttribute: TransposeAttributes = createAttributeWithCacheKey({ perm: [0, 2, 1, 3] });

const maybeTransposeToBNSH = (context: ComputeContext, input: TensorView, params: AttentionParameters) => {
  let reshapedInput = input;
  const numHeads = params.kvNumHeads!;
  if (input.dims.length === 3 && params.kvSequenceLength !== 0) {
    reshapedInput = input.reshape([params.batchSize, params.kvSequenceLength, numHeads, params.headSize]);
    reshapedInput = context.compute(createTransposeProgramInfo(reshapedInput, weightTransposeAttribute.perm), {
      inputs: [reshapedInput],
      outputs: [-1],
    })[0];
  }

  return reshapedInput;
};

const generatePositionIdsProgramInfo = (
  batchSize: number,
  sequenceLength: number,
  seqLens: TensorView,
  totalSeqLen: TensorView,
) => {
  const outputDataType = DataType.int64;
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['type', 'type'];
  const outputShape = [batchSize * sequenceLength];
  const outputSize = batchSize * sequenceLength;
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: sequenceLength },
    { type: DataType.uint32, data: batchSize },
  ];
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const seqLensInputHelper = inputVariable('seq_lens', seqLens.dataType, seqLens.dims);
    const totalSeqLenInputHelper = inputVariable('total_seq_lens', totalSeqLen.dataType, totalSeqLen.dims);
    const positionIdsHelper = outputVariable('pos_ids', outputDataType, outputShape);

    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'sequence_length', type: 'u32' },
      { name: 'batch_size', type: 'u32' },
    ];

    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(seqLensInputHelper, totalSeqLenInputHelper, positionIdsHelper)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
    let total_sequence_length = u32(${totalSeqLenInputHelper.getByOffset('0')});
    let is_subsequent_prompt = uniforms.sequence_length > 1 && uniforms.sequence_length != total_sequence_length;
    let is_first_prompt = !is_subsequent_prompt && uniforms.sequence_length == total_sequence_length;
    let batch_idx = global_idx / uniforms.sequence_length;
    let sequence_idx = i32(global_idx % uniforms.sequence_length);
    var pos_id: i32 = 0;
    let seqlen = ${seqLensInputHelper.getByOffset('batch_idx')};
    let total_seqlen = seqlen + 1;
    if (is_first_prompt) {
      if (sequence_idx < total_seqlen) {
        pos_id = sequence_idx;
      } else {
        pos_id = 1;
      }
      ${positionIdsHelper.setByOffset('global_idx', 'pos_id')}
    } else if (is_subsequent_prompt) {
      let past_seqlen = total_seqlen - i32(uniforms.sequence_length);
      if (past_seqlen + sequence_idx < total_seqlen) {
        pos_id = past_seqlen + sequence_idx;
      } else {
        pos_id = 1;
      }
      ${positionIdsHelper.setByOffset('global_idx', 'pos_id')}
    } else if (global_idx < uniforms.batch_size) {
      ${positionIdsHelper.setByOffset('global_idx', 'seqlen')}
    };
  }
  `;
  };
  return {
    name: 'GeneratePositionIds',
    shaderCache: { hint: `${batchSize};${sequenceLength}`, inputDependencies },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType: outputDataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const groupQueryAttention = (context: ComputeContext, attributes: GroupQueryAttentionAttributes): void => {
  const params = validateInputs(context.inputs, attributes);
  if (context.inputs[0].dims.length === 5) {
    throw new Error('Packed QKV is not implemented');
  }

  if (context.inputs[1]?.dims.length === 5) {
    throw new Error('Packed KV is not implemented');
  }

  const q = context.inputs[0];
  const k = context.inputs[1] && context.inputs[1].dims.length > 0 ? context.inputs[1] : undefined;
  const v = context.inputs[2] && context.inputs[2].dims.length > 0 ? context.inputs[2] : undefined;
  const pastKey = context.inputs[3] && context.inputs[3].dims.length !== 0 ? context.inputs[3] : undefined;
  const pastValue = context.inputs[4] && context.inputs[4].dims.length !== 0 ? context.inputs[4] : undefined;
  const seqLens = context.inputs.length > 4 ? context.inputs[5] : undefined;
  const totalSequenceLengthInput = context.inputs.length > 5 ? context.inputs[6] : undefined;
  const kvNumHeads = params.kvNumHeads ? params.kvNumHeads : params.numHeads;

  // TODO Remove explicit split operation and use indexing in Attention implementation to avoid overhead.

  const splitAttributes: SplitAttributes = createAttributeWithCacheKey({
    axis: 2,
    numOutputs: 3,
    splitSizes: [params.numHeads * params.headSize, kvNumHeads * params.headSize, kvNumHeads * params.headSize],
  });
  const [query, key, value] =
    !k && !v
      ? context.compute(createSplitProgramInfo([q], splitAttributes), { inputs: [q], outputs: [-1, -1, -1] })
      : [q, k!, v!];
  let qRotary: TensorView | undefined;
  let kRotary: TensorView | undefined;
  if (attributes.doRotary) {
    const posIds = context.compute(
      generatePositionIdsProgramInfo(params.batchSize, params.sequenceLength, seqLens!, totalSequenceLengthInput!),
      { inputs: [seqLens!, totalSequenceLengthInput!], outputs: [-1] },
    )[0];
    const cosCache = context.inputs[7];
    const sinCache = context.inputs[8];
    const qRotaryEmbeddingAttributes: RotaryEmbeddingAttributes = createAttributeWithCacheKey({
      interleaved: attributes.rotaryInterleaved !== 0,
      numHeads: params.numHeads,
      rotaryEmbeddingDim: 0,
      scale: attributes.scale,
    });
    const inputs = [query, posIds, cosCache, sinCache];
    const outputs = [-1];
    qRotary = context.compute(createRotaryEmbeddingProgramInfo(inputs, qRotaryEmbeddingAttributes), {
      inputs,
      outputs,
    })[0];
    inputs.splice(0, 1, key);
    const kRotaryEmbeddingAttributes: RotaryEmbeddingAttributes = createAttributeWithCacheKey({
      interleaved: attributes.rotaryInterleaved !== 0,
      numHeads: params.kvNumHeads!,
      rotaryEmbeddingDim: 0,
      scale: attributes.scale,
    });
    kRotary = context.compute(createRotaryEmbeddingProgramInfo(inputs, kRotaryEmbeddingAttributes), {
      inputs,
      outputs,
    })[0];
  }
  const Q = maybeTransposeToBNSHAndAddBias(
    context,
    params.batchSize,
    params.numHeads,
    params.sequenceLength,
    params.headSize,
    attributes.doRotary ? qRotary! : query,
    undefined,
    0,
  );
  const K = maybeTransposeToBNSH(context, attributes.doRotary ? kRotary! : key, params);
  const V = maybeTransposeToBNSH(context, value, params);

  applyAttention(
    context,
    Q,
    K,
    V,
    undefined,
    undefined,
    pastKey,
    pastValue,
    undefined,
    params,
    seqLens,
    totalSequenceLengthInput,
  );
};
