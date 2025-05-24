// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { OpSet } from '../../opset';

import { batchNormalization, parseBatchNormalizationAttributes } from './ops/batch-normalization';
import * as binaryOps from './ops/binary-op';
import { cast, parseCastAttributes } from './ops/cast';
import { concat, parseConcatAttributes } from './ops/concat';
import { conv, parseConvAttributes } from './ops/conv';
import { convTranspose, parseConvTransposeAttributes } from './ops/conv-transpose';
import { depthToSpace, parseDepthToSpaceAttributes } from './ops/depth-to-space';
import { flatten, parseFlattenAttributes } from './ops/flatten';
import { gather, parseGatherAttributes } from './ops/gather';
import { gemm, parseGemmAttributesV11, parseGemmAttributesV7 } from './ops/gemm';
import { imageScaler, parseImageScalerAttributes } from './ops/image-scaler';
import { instanceNormalization, parseInstanceNormalizationAttributes } from './ops/instance-normalization';
import { lrn, parseLrnAttributes } from './ops/lrn';
import { matMul, parseMatMulAttributes } from './ops/matmul';
import { padV11, padV2, parsePadAttributesV11, parsePadAttributesV2 } from './ops/pad';
import {
  averagePool,
  globalAveragePool,
  globalMaxPool,
  maxPool,
  parseAveragePoolAttributes,
  parseGlobalAveragePoolAttributes,
  parseMaxPoolAttributes,
} from './ops/pool';
import {
  parseReduceAttributes,
  reduceLogSum,
  reduceLogSumSquare,
  reduceMax,
  reduceMean,
  reduceMin,
  reduceProd,
  reduceSum,
} from './ops/reduce';
import { reshape } from './ops/reshape';
import { parseResizeAttributesV10, parseResizeAttributesV11, resize } from './ops/resize-packed';
import { shape } from './ops/shape';
import { parseSliceAttributes, slice, sliceV10 } from './ops/slice';
import { parseSoftmaxAttributes, parseSoftmaxAttributesV13, softmax, softmaxV13 } from './ops/softmax';
import { parseSplitAttributes, split } from './ops/split';
import { parseSqueezeAttributes, squeeze, squeezeV13 } from './ops/squeeze';
import { sum } from './ops/sum';
import { tile } from './ops/tile';
import { parseTransposeAttributes, transpose } from './ops/transpose';
import * as unaryOps from './ops/unary-op';
import { parseUnsqueezeAttributes, unsqueeze, unsqueezeV13 } from './ops/unsqueeze';
import { parseUpsampleAttributesV7, parseUpsampleAttributesV9, upsample } from './ops/upsample';

export const WEBGL_OP_RESOLVE_RULES: readonly OpSet.ResolveRule[] = [
  ['Abs', '', '6+', unaryOps.abs],
  ['Acos', '', '7+', unaryOps.acos],
  ['Add', '', '7+', binaryOps.add],
  ['And', '', '7+', binaryOps.and],
  ['Asin', '', '7+', unaryOps.asin],
  ['Atan', '', '7+', unaryOps.atan],
  // TODO: support new attributes for AveragePool-10
  ['AveragePool', '', '7+', averagePool, parseAveragePoolAttributes],
  ['BatchNormalization', '', '7+', batchNormalization, parseBatchNormalizationAttributes],
  ['Cast', '', '6+', cast, parseCastAttributes],
  ['Ceil', '', '6+', unaryOps.ceil],
  ['Clip', '', '6-10', unaryOps.clip, unaryOps.parseClipAttributes],
  ['Clip', '', '11+', unaryOps.clipV11],
  ['Concat', '', '4+', concat, parseConcatAttributes],
  ['Conv', '', '1+', conv, parseConvAttributes],
  ['ConvTranspose', '', '1+', convTranspose, parseConvTransposeAttributes],
  ['Cos', '', '7+', unaryOps.cos],
  ['Div', '', '7+', binaryOps.div],
  ['Dropout', '', '7+', unaryOps.identity],
  ['DepthToSpace', '', '1+', depthToSpace, parseDepthToSpaceAttributes],
  ['Equal', '', '7+', binaryOps.equal],
  ['Elu', '', '6+', unaryOps.elu, unaryOps.parseEluAttributes],
  ['Exp', '', '6+', unaryOps.exp],
  ['Flatten', '', '1+', flatten, parseFlattenAttributes],
  ['Floor', '', '6+', unaryOps.floor],
  ['FusedConv', 'com.microsoft', '1+', conv, parseConvAttributes],
  ['Gather', '', '1+', gather, parseGatherAttributes],
  ['Gemm', '', '7-10', gemm, parseGemmAttributesV7],
  ['Gemm', '', '11+', gemm, parseGemmAttributesV11],
  ['GlobalAveragePool', '', '1+', globalAveragePool, parseGlobalAveragePoolAttributes],
  ['GlobalMaxPool', '', '1+', globalMaxPool],
  ['Greater', '', '7+', binaryOps.greater],
  ['Identity', '', '1+', unaryOps.identity],
  ['ImageScaler', '', '1+', imageScaler, parseImageScalerAttributes],
  ['InstanceNormalization', '', '6+', instanceNormalization, parseInstanceNormalizationAttributes],
  ['LeakyRelu', '', '6+', unaryOps.leakyRelu, unaryOps.parseLeakyReluAttributes],
  ['Less', '', '7+', binaryOps.less],
  ['LRN', '', '1+', lrn, parseLrnAttributes],
  ['Log', '', '6+', unaryOps.log],
  ['MatMul', '', '1+', matMul, parseMatMulAttributes],
  // TODO: support new attributes for MaxPool-8 and MaxPool-10
  ['MaxPool', '', '1+', maxPool, parseMaxPoolAttributes],
  ['Mul', '', '7+', binaryOps.mul],
  ['Neg', '', '6+', unaryOps.neg],
  ['Not', '', '1+', unaryOps.not],
  ['Or', '', '7+', binaryOps.or],
  ['Pad', '', '2-10', padV2, parsePadAttributesV2],
  ['Pad', '', '11+', padV11, parsePadAttributesV11],
  ['Pow', '', '7+', binaryOps.pow],
  ['PRelu', '', '7+', binaryOps.pRelu],
  ['ReduceLogSum', '', '1+', reduceLogSum, parseReduceAttributes],
  ['ReduceMax', '', '1+', reduceMax, parseReduceAttributes],
  ['ReduceMean', '', '1+', reduceMean, parseReduceAttributes],
  ['ReduceMin', '', '1+', reduceMin, parseReduceAttributes],
  ['ReduceProd', '', '1+', reduceProd, parseReduceAttributes],
  ['ReduceSum', '', '1-12', reduceSum, parseReduceAttributes],
  ['ReduceSumSquare', '', '1+', reduceLogSumSquare, parseReduceAttributes],
  ['Relu', '', '6+', unaryOps.relu],
  ['Reshape', '', '5+', reshape],
  ['Resize', '', '10', resize, parseResizeAttributesV10],
  ['Resize', '', '11+', resize, parseResizeAttributesV11],
  ['Shape', '', '1+', shape],
  ['Sigmoid', '', '6+', unaryOps.sigmoid],
  ['Sin', '', '7+', unaryOps.sin],
  ['Slice', '', '10+', sliceV10], // TODO: support 'steps' for Slice-10
  ['Slice', '', '1-9', slice, parseSliceAttributes],
  // The "semantic" meaning of axis has changed in opset-13.
  ['Softmax', '', '1-12', softmax, parseSoftmaxAttributes],
  ['Softmax', '', '13+', softmaxV13, parseSoftmaxAttributesV13],
  // 'Split' operator has an optional attribute 'split'
  // this attribute determines how the specified axis of input data is split.
  // When the attribute is missing, we need the count of number of outputs
  // so that we can determine the 'split' attribute from the runtime input to the Operator
  ['Split', '', '2-12', split, parseSplitAttributes],
  ['Sqrt', '', '6+', unaryOps.sqrt],
  ['Squeeze', '', '1-12', squeeze, parseSqueezeAttributes],
  ['Squeeze', '', '13+', squeezeV13],
  ['Sub', '', '7+', binaryOps.sub],
  ['Sum', '', '6+', sum],
  ['Tan', '', '7+', unaryOps.tan],
  ['Tanh', '', '6+', unaryOps.tanh],
  ['Tile', '', '6+', tile],
  ['Transpose', '', '1+', transpose, parseTransposeAttributes],
  ['Upsample', '', '7-8', upsample, parseUpsampleAttributesV7],
  ['Upsample', '', '9', upsample, parseUpsampleAttributesV9],
  ['Unsqueeze', '', '1-12', unsqueeze, parseUnsqueezeAttributes],
  ['Unsqueeze', '', '13+', unsqueezeV13],
  ['Xor', '', '7+', binaryOps.xor],
];
