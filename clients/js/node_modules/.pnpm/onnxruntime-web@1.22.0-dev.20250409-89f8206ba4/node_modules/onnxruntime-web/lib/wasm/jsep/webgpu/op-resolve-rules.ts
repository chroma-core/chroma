// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { argMax, argMin, parseArgMinMaxAttributes } from './ops/argminmax';
import { attention } from './ops/attention';
import { batchNorm } from './ops/batch-norm';
import { biasAdd } from './ops/bias-add';
import { biasSplitGelu } from './ops/bias-split-gelu';
import * as binaryOps from './ops/binary-op';
import { concat, parseConcatAttributes } from './ops/concat';
import { conv, parseConvAttributes } from './ops/conv';
import { convTranspose, parseConvTransposeAttributes } from './ops/conv-transpose';
import { cumsum, parseCumSumAttributes } from './ops/cumsum';
import { depthToSpace, parseDepthToSpaceAttributes } from './ops/depth-to-space';
import { einsum, parseEinsumAttributes } from './ops/einsum';
import { expand } from './ops/expand';
import { fastGelu } from './ops/fast-gelu';
import { gather, parseGatherAttributes } from './ops/gather';
import { gatherND, parseGatherNDAttributes } from './ops/gather-nd';
import { gatherBlockQuantized, parseGatherBlockQuantizedAttributes } from './ops/gather-block-quantized';
import { gatherElements, parseGatherElementsAttributes } from './ops/gather-elements';
import { gemm, parseGemmAttributes } from './ops/gemm';
import { gridSample, parseGridSampleAttributes } from './ops/grid-sample';
import { groupQueryAttention } from './ops/group-query-attention';
import { instanceNorm } from './ops/instance-norm';
import { layerNorm } from './ops/layer-norm';
import { matMul } from './ops/matmul';
import { matMulNBits, parseMatMulNBitsAttributes } from './ops/matmulnbits';
import { multiHeadAttention, parseMultiHeadAttentionAttributes } from './ops/multihead-attention';
import { pad } from './ops/pad';
import * as pool from './ops/pool';
import { dequantizeLinear, parseDequantizeLinearAttributes } from './ops/quantize-linear';
import { range } from './ops/range';
import { scatterND, parseScatterNDAttributes } from './ops/scatter-nd';
import {
  reduceL1,
  reduceL2,
  reduceLogSum,
  reduceLogSumExp,
  reduceMax,
  reduceMean,
  reduceMin,
  reduceProd,
  reduceSum,
  reduceSumSquare,
} from './ops/reduce';
import { parseResizeAttributes, resize } from './ops/resize';
import { rotaryEmbedding } from './ops/rotary-embedding';
import { skipLayerNorm } from './ops/skip-layer-norm';
import { parseSliceAttributes, slice } from './ops/slice';
import { parseSoftmaxAttributes, softmax } from './ops/softmax';
import { parseSplitAttributes, split } from './ops/split';
import { tile } from './ops/tile';
import { parseTransposeAttributes, transpose } from './ops/transpose';
import * as unaryOps from './ops/unary-op';
import { where } from './ops/where';
import { ComputeContext } from './types';

export type RunFunction = (context: ComputeContext, attribute?: unknown) => void;
export type ParseAttributeFunction = (attributeRaw: unknown) => unknown;
export type OperatorImplementation = [RunFunction] | [RunFunction, ParseAttributeFunction];

export const WEBGPU_OP_RESOLVE_RULES: Map<string, OperatorImplementation> = new Map([
  ['Abs', [unaryOps.abs]],
  ['Acos', [unaryOps.acos]],
  ['Acosh', [unaryOps.acosh]],
  ['Add', [binaryOps.add]],
  ['ArgMax', [argMax, parseArgMinMaxAttributes]],
  ['ArgMin', [argMin, parseArgMinMaxAttributes]],
  ['Asin', [unaryOps.asin]],
  ['Asinh', [unaryOps.asinh]],
  ['Atan', [unaryOps.atan]],
  ['Atanh', [unaryOps.atanh]],
  ['Attention', [attention]],
  // TODO: support new attributes for AveragePool-10
  ['AveragePool', [pool.averagePool, pool.parseAveragePoolAttributes]],
  ['BatchNormalization', [batchNorm]],
  ['BiasAdd', [biasAdd]],
  ['BiasSplitGelu', [biasSplitGelu]],
  ['Cast', [unaryOps.cast, unaryOps.parseCastAttributes]],
  ['Ceil', [unaryOps.ceil]],
  ['Clip', [unaryOps.clip]],
  ['Concat', [concat, parseConcatAttributes]],
  ['Conv', [conv, parseConvAttributes]],
  ['ConvTranspose', [convTranspose, parseConvTransposeAttributes]],
  ['Cos', [unaryOps.cos]],
  ['Cosh', [unaryOps.cosh]],
  ['CumSum', [cumsum, parseCumSumAttributes]],
  ['DepthToSpace', [depthToSpace, parseDepthToSpaceAttributes]],
  ['DequantizeLinear', [dequantizeLinear, parseDequantizeLinearAttributes]],
  ['Div', [binaryOps.div]],
  ['Einsum', [einsum, parseEinsumAttributes]],
  ['Elu', [unaryOps.elu, unaryOps.parseAlphaAttributes]],
  ['Equal', [binaryOps.equal]],
  ['Erf', [unaryOps.erf]],
  ['Exp', [unaryOps.exp]],
  ['Expand', [expand]],
  ['FastGelu', [fastGelu]],
  ['Floor', [unaryOps.floor]],
  ['FusedConv', [conv, parseConvAttributes]],
  ['Gather', [gather, parseGatherAttributes]],
  ['GatherElements', [gatherElements, parseGatherElementsAttributes]],
  ['GatherBlockQuantized', [gatherBlockQuantized, parseGatherBlockQuantizedAttributes]],
  ['GatherND', [gatherND, parseGatherNDAttributes]],
  ['Gelu', [unaryOps.gelu]],
  ['Gemm', [gemm, parseGemmAttributes]],
  ['GlobalAveragePool', [pool.globalAveragePool, pool.parseGlobalAveragePoolAttributes]],
  ['GlobalMaxPool', [pool.globalMaxPool, pool.parseGlobalMaxPoolAttributes]],
  ['Greater', [binaryOps.greater]],
  ['GreaterOrEqual', [binaryOps.greaterOrEqual]],
  ['GridSample', [gridSample, parseGridSampleAttributes]],
  ['GroupQueryAttention', [groupQueryAttention]],
  ['HardSigmoid', [unaryOps.hardSigmoid, unaryOps.parseHardSigmoidAttributes]],
  ['InstanceNormalization', [instanceNorm]],
  ['LayerNormalization', [layerNorm]],
  ['LeakyRelu', [unaryOps.leakyRelu, unaryOps.parseAlphaAttributes]],
  ['Less', [binaryOps.less]],
  ['LessOrEqual', [binaryOps.lessOrEqual]],
  ['Log', [unaryOps.log]],
  ['MatMul', [matMul]],
  ['MatMulNBits', [matMulNBits, parseMatMulNBitsAttributes]],
  // TODO: support new attributes for MaxPool-8 and MaxPool-10
  ['MaxPool', [pool.maxPool, pool.parseMaxPoolAttributes]],
  ['Mul', [binaryOps.mul]],
  ['MultiHeadAttention', [multiHeadAttention, parseMultiHeadAttentionAttributes]],
  ['Neg', [unaryOps.neg]],
  ['Not', [unaryOps.not]],
  ['Pad', [pad]],
  ['Pow', [binaryOps.pow]],
  ['QuickGelu', [unaryOps.quickgelu, unaryOps.parseAlphaAttributes]],
  ['Range', [range]],
  ['Reciprocal', [unaryOps.reciprocal]],
  ['ReduceMin', [reduceMin]],
  ['ReduceMean', [reduceMean]],
  ['ReduceMax', [reduceMax]],
  ['ReduceSum', [reduceSum]],
  ['ReduceProd', [reduceProd]],
  ['ReduceL1', [reduceL1]],
  ['ReduceL2', [reduceL2]],
  ['ReduceLogSum', [reduceLogSum]],
  ['ReduceLogSumExp', [reduceLogSumExp]],
  ['ReduceSumSquare', [reduceSumSquare]],
  ['Relu', [unaryOps.relu]],
  ['Resize', [resize, parseResizeAttributes]],
  ['RotaryEmbedding', [rotaryEmbedding]],
  ['ScatterND', [scatterND, parseScatterNDAttributes]],
  ['Sigmoid', [unaryOps.sigmoid]],
  ['Sin', [unaryOps.sin]],
  ['Sinh', [unaryOps.sinh]],
  ['Slice', [slice, parseSliceAttributes]],
  ['SkipLayerNormalization', [skipLayerNorm]],
  ['Split', [split, parseSplitAttributes]],
  ['Sqrt', [unaryOps.sqrt]],
  ['Softmax', [softmax, parseSoftmaxAttributes]],
  ['Sub', [binaryOps.sub]],
  ['Tan', [unaryOps.tan]],
  ['Tanh', [unaryOps.tanh]],
  ['ThresholdedRelu', [unaryOps.thresholdedRelu, unaryOps.parseAlphaAttributes]],
  ['Tile', [tile]],
  ['Transpose', [transpose, parseTransposeAttributes]],
  ['Where', [where]],
]);
