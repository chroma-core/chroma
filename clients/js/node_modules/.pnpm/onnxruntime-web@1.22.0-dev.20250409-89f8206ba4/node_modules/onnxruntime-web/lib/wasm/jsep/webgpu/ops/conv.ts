// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { PoolConvUtil } from '../../util';
import { AttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext } from '../types';

import { createConv2DMatMulProgramInfo } from './3rd-party/conv2d_mm_webgpu';
import { computeConv3DInfo, createConv3DNaiveProgramInfo } from './3rd-party/conv3d_naive_webgpu';
import { createMatmulProgramInfo } from './3rd-party/matmul_packed_webgpu';
import { createGroupedConvProgramInfo, createGroupedConvVectorizeProgramInfo } from './conv-grouped';
import { InternalActivationAttributes, parseInternalActivationAttributes } from './fuse-utils';
import { createNaiveMatmulProgramInfo } from './matmul-shaders';
import { createTransposeProgramInfo } from './transpose';

export const calculateOutputShape = (
  inputShape: readonly number[],
  kernelShape: readonly number[],
  dilations: readonly number[],
  adjustPads: readonly number[],
  strides: readonly number[],
  isChannelLast: boolean,
): number[] => {
  const batchSize = inputShape[0];
  const inputSpatialShape = inputShape.slice(isChannelLast ? 1 : 2, isChannelLast ? 3 : 4);
  const spatialRank = inputSpatialShape.length;
  const outChannels = kernelShape[0];
  const kernelSpatialShape = kernelShape.slice(2);
  const dilatedKernelShape = kernelSpatialShape.map((v, i) => v + (v - 1) * (dilations[i] - 1));
  const inputSpatialShapeWithPad = inputSpatialShape.map((v, i) => v + adjustPads[i] + adjustPads[i + spatialRank]);
  const outputShape = inputSpatialShapeWithPad.map((v, i) =>
    Math.floor((v - dilatedKernelShape[i] + strides[i]) / strides[i]),
  );
  outputShape.splice(0, 0, batchSize);
  outputShape.splice(isChannelLast ? 3 : 1, 0, outChannels);
  return outputShape;
};

export interface ConvAttributes extends InternalActivationAttributes, AttributeWithCacheKey {
  readonly autoPad: string;
  readonly dilations: readonly number[];
  readonly format: 'NHWC' | 'NCHW';
  readonly group: number;
  readonly kernelShape: readonly number[];
  readonly pads: readonly number[];
  readonly strides: readonly number[];
  readonly wIsConst: boolean;
}

// for transposing weight tensor from [M, C/group, KH, KW] to [KH, KW, C/group, M]
const weightTransposeAttribute = [2, 3, 1, 0];

const validateInputs = (inputs: readonly TensorView[], attributes: ConvAttributes): void => {
  // Refer to the below link for all input checks
  // https://github.com/onnx/onnx/blob/master/docs/Operators.md#Conv
  if (!inputs || (inputs.length !== 2 && inputs.length !== 3)) {
    throw new Error('Conv requires 2 or 3 inputs');
  }

  if (inputs[0].dims.length > 5) {
    throw new Error('greater than 5D is not supported');
  }

  if (inputs[0].dims.length !== inputs[1].dims.length) {
    throw new Error('filter does not have same dimension as input');
  }

  // FILTER_IN_CHANNEL should be equal to DATA_CHANNEL
  const dataChannel = inputs[0].dims[attributes.format === 'NHWC' ? inputs[0].dims.length - 1 : 1];
  const filterInChannel = inputs[1].dims[1] * attributes.group;
  if (dataChannel !== filterInChannel) {
    throw new Error('FILTER_IN_CHANNEL should be equal to DATA_CHANNEL');
  }

  // if bias is provided it should be 1D and the number of elements should be equal to the number of feature maps
  if (inputs.length === 3 && (inputs[2].dims.length !== 1 || inputs[1].dims[0] !== inputs[2].dims[0])) {
    throw new Error('invalid bias');
  }

  const spatialRank = inputs[0].dims.length - 2;
  // wrong dilations dimension
  if (attributes.dilations.length !== spatialRank) {
    throw new Error(`dilations should be ${spatialRank}D`);
  }

  // Wrong strides dimension
  if (attributes.strides.length !== spatialRank) {
    throw new Error(`strides should be ${spatialRank}D`);
  }

  // Wrong pads dimension
  if (attributes.pads.length !== spatialRank * 2) {
    throw new Error(`pads should be ${spatialRank * 2}D`);
  }

  // if kernelShape is specified, it's data length must be 2 less than dims length of the weights tensor
  // (the first 2 dims are batch_size and channels)
  if (attributes.kernelShape.length !== 0 && attributes.kernelShape.length !== inputs[1].dims.length - 2) {
    throw new Error('invalid kernel shape');
  }
};

const getAdjustedConvAttributes = <T extends ConvAttributes>(attributes: T, inputs: readonly TensorView[]): T => {
  const kernelShape = attributes.kernelShape.slice();
  // if kernelShape is not well specified in the attributes, infer it from the weight tensor dims
  if (kernelShape.length < inputs[1].dims.length - 2) {
    kernelShape.push(...Array(inputs[1].dims.length - 2 - kernelShape.length).fill(0));
  }
  for (let i = 2; i < inputs[1].dims.length; ++i) {
    if (kernelShape[i - 2] === 0) {
      kernelShape[i - 2] = inputs[1].dims[i];
    }
  }
  const pads = attributes.pads.slice();
  PoolConvUtil.adjustPadsBasedOnAutoPad(
    inputs[0].dims,
    attributes.strides,
    attributes.dilations,
    kernelShape,
    pads,
    attributes.format === 'NHWC',
    attributes.autoPad,
  );

  // always return a new object so does not modify the original attributes
  const newAttributes: T = Object.assign({}, attributes);
  Object.assign(newAttributes, { kernelShape, pads });
  return newAttributes;
};

export const parseConvAttributes = (attributes: Record<string, unknown>): ConvAttributes => {
  const activationAttributes = parseInternalActivationAttributes(attributes);
  // TODO : Make this generic enough to compute default attributes for multi-dimensional conv
  const format = attributes.format as 'NHWC' | 'NCHW';
  const autoPad = ['NOTSET', 'VALID', 'SAME_UPPER', 'SAME_LOWER'][attributes.auto_pad as number];
  const dilations = attributes.dilations as number[];
  const group = attributes.group as number;
  const kernelShape = attributes.kernel_shape as number[];
  const pads = attributes.pads as number[];
  const strides = attributes.strides as number[];
  const wIsConst = (attributes.w_is_const as () => boolean)();

  return {
    autoPad,
    format,
    dilations,
    group,
    kernelShape,
    pads,
    strides,
    wIsConst,
    ...activationAttributes,
    cacheKey: `${attributes.format};${activationAttributes.activation};`,
  };
};

const conv2d = (
  context: ComputeContext,
  inputs: readonly TensorView[],
  attributes: ConvAttributes,
  squeezeOutputShapeFunction?: (shape: readonly number[]) => number[],
): void => {
  // check attributes

  // const hasPreluActivationWeights = false; /* TODO: add support for prelu activation weights */
  const isChannelsLast = attributes.format === 'NHWC';
  const outputShape = calculateOutputShape(
    inputs[0].dims,
    inputs[1].dims,
    attributes.dilations,
    attributes.pads,
    attributes.strides,
    isChannelsLast,
  );
  if (attributes.group !== 1) {
    const convInputs = [inputs[0]];
    if (isChannelsLast) {
      const transposedWeight =
        (context.kernelCustomData.wT as TensorView | undefined) ??
        context.compute(createTransposeProgramInfo(inputs[1], weightTransposeAttribute), {
          inputs: [1],
          outputs: [attributes.wIsConst ? -2 : -1],
        })[0];
      if (attributes.wIsConst && !context.kernelCustomData.wT) {
        context.kernelCustomData.wT = transposedWeight;
      }
      convInputs.push(transposedWeight);
    } else {
      convInputs.push(inputs[1]);
    }
    if (inputs.length === 3) {
      convInputs.push(inputs[2]);
    }
    // NVIDIA GPU with ampere architecture fails with below 2 cases, but we couldn't repro them with any other
    // GPUs. So just disable vectorize on NVIDIA ampere to ensure always correct outputs.
    // [webgpu]Conv - conv - vectorize group - B
    // [webgpu]Conv - conv - vectorize group - D
    const enableGroupedConvVectorize = !context.adapterInfo.isArchitecture('ampere');
    if (
      enableGroupedConvVectorize &&
      isChannelsLast &&
      inputs[1].dims[0] === attributes.group &&
      inputs[1].dims[1] === 1 &&
      attributes.dilations[0] === 1 &&
      attributes.dilations[1] === 1
    ) {
      context.compute(
        createGroupedConvVectorizeProgramInfo(convInputs, attributes, outputShape, squeezeOutputShapeFunction),
        { inputs: convInputs },
      );
    } else {
      context.compute(createGroupedConvProgramInfo(convInputs, attributes, outputShape, squeezeOutputShapeFunction), {
        inputs: convInputs,
      });
    }
    return;
  }

  const hasBias = inputs.length === 3;
  const inputHeight = inputs[0].dims[isChannelsLast ? 1 : 2];
  const inputWidth = inputs[0].dims[isChannelsLast ? 2 : 3];
  const inputChannels = inputs[0].dims[isChannelsLast ? 3 : 1];
  const weightHeight = inputs[1].dims[2];
  const weightWidth = inputs[1].dims[3];

  const outHeight = outputShape[isChannelsLast ? 1 : 2];
  const outWidth = outputShape[isChannelsLast ? 2 : 3];
  const outChannels = outputShape[isChannelsLast ? 3 : 1];

  const sameSize =
    isChannelsLast &&
    weightHeight === inputHeight &&
    weightWidth === inputWidth &&
    attributes.pads[0] === 0 &&
    attributes.pads[1] === 0;
  if (
    sameSize ||
    (weightHeight === 1 &&
      weightWidth === 1 &&
      attributes.dilations[0] === 1 &&
      attributes.dilations[1] === 1 &&
      attributes.strides[0] === 1 &&
      attributes.strides[1] === 1 &&
      attributes.pads[0] === 0 &&
      attributes.pads[1] === 0)
  ) {
    // conv2dByMatMul
    const batch = outputShape[0];
    let xReshaped, wReshaped, matmulOutputShape;
    const matmulInputs = [];
    if (isChannelsLast) {
      const transposedWeight =
        (context.kernelCustomData.wT as TensorView | undefined) ??
        context.compute(createTransposeProgramInfo(inputs[1], weightTransposeAttribute), {
          inputs: [1],
          outputs: [attributes.wIsConst ? -2 : -1],
        })[0];
      if (attributes.wIsConst && !context.kernelCustomData.wT) {
        context.kernelCustomData.wT = transposedWeight;
      }
      if (sameSize) {
        const sharedDim = inputHeight * inputWidth * inputChannels;
        xReshaped = inputs[0].reshape([1, batch, sharedDim]);
        wReshaped = transposedWeight.reshape([1, sharedDim, outChannels]);
        matmulOutputShape = [1, batch, outChannels];
      } else {
        xReshaped = inputs[0].reshape([batch, inputHeight * inputWidth, inputChannels]);
        wReshaped = transposedWeight.reshape([1, inputChannels, outChannels]);
        matmulOutputShape = [batch, outHeight * outWidth, outChannels];
      }
      matmulInputs.push(xReshaped);
      matmulInputs.push(wReshaped);
    } else {
      xReshaped = inputs[0].reshape([batch, inputChannels, inputHeight * inputWidth]);
      wReshaped = inputs[1].reshape([1, outChannels, inputChannels]);
      matmulOutputShape = [batch, outChannels, outHeight * outWidth];
      matmulInputs.push(wReshaped);
      matmulInputs.push(xReshaped);
    }
    if (hasBias) {
      matmulInputs.push(inputs[2]);
    }
    const N = matmulOutputShape[2];
    const K = matmulInputs[0].dims[matmulInputs[0].dims.length - 1];
    // Tune the threshold.
    if (N < 8 && K < 8) {
      context.compute(
        createNaiveMatmulProgramInfo(
          matmulInputs,
          attributes,
          outputShape,
          matmulOutputShape,
          isChannelsLast,
          squeezeOutputShapeFunction,
        ),
        { inputs: matmulInputs },
      );
    } else {
      context.compute(
        createMatmulProgramInfo(
          matmulInputs,
          attributes,
          outputShape,
          matmulOutputShape,
          isChannelsLast,
          squeezeOutputShapeFunction,
        ),
        { inputs: matmulInputs },
      );
    }
    return;
  }

  // TODO: implement conv2dWithIm2Col()

  const sequentialAccessByThreads = /* backend.adapterInfo.isIntel() */ true;

  // STEP.1: transpose weight
  const transposedWeight =
    (context.kernelCustomData.wT as TensorView | undefined) ??
    context.compute(createTransposeProgramInfo(inputs[1], weightTransposeAttribute), {
      inputs: [1],
      outputs: [attributes.wIsConst ? -2 : -1],
    })[0];
  if (attributes.wIsConst && !context.kernelCustomData.wT) {
    context.kernelCustomData.wT = transposedWeight;
  }

  // STEP.2: prepare reshaped inputs
  const convInputs = [inputs[0], transposedWeight];
  if (hasBias) {
    convInputs.push(inputs[2]);
  }

  // STEP.3: compute matmul
  const dimAOuter = isChannelsLast ? outHeight * outWidth : outChannels;
  const dimBOuter = isChannelsLast ? outChannels : outHeight * outWidth;
  const dimInner = weightHeight * weightWidth * inputChannels;
  context.compute(
    createConv2DMatMulProgramInfo(
      convInputs,
      attributes,
      outputShape,
      dimAOuter,
      dimBOuter,
      dimInner,
      hasBias,
      sequentialAccessByThreads,
      squeezeOutputShapeFunction,
    ),
    { inputs: convInputs },
  );
};

const conv1d = (context: ComputeContext, attributes: ConvAttributes): void => {
  // extend the input to 2D by adding H dimension
  const isChannelLast = attributes.format === 'NHWC';
  const inputs = [
    context.inputs[0].reshape(
      isChannelLast
        ? // [N, W, C] -> [N, H=1, W, C]
          [context.inputs[0].dims[0], 1, context.inputs[0].dims[1], context.inputs[0].dims[2]]
        : // [N, C, W] -> [N, C, H=1, W]
          [context.inputs[0].dims[0], context.inputs[0].dims[1], 1, context.inputs[0].dims[2]],
    ),
    //[FILTER_OUT_CHANNEL, FILTER_IN_CHANNEL, kW] -> [FILTER_OUT_CHANNEL, FILTER_IN_CHANNEL, kH=1, kW]
    context.inputs[1].reshape([context.inputs[1].dims[0], context.inputs[1].dims[1], 1, context.inputs[1].dims[2]]),
  ];
  if (context.inputs.length === 3) {
    inputs.push(context.inputs[2]);
  }
  const pads = [0, attributes.pads[0], 0, attributes.pads[1]];
  const strides = [1].concat(attributes.strides);
  const dilations = [1].concat(attributes.dilations);
  const kernelShape = [1].concat(attributes.kernelShape);
  const adjustedAttributes = getAdjustedConvAttributes(
    { ...attributes, pads, strides, dilations, kernelShape },
    inputs,
  );
  conv2d(context, inputs, adjustedAttributes, (outputShape) =>
    isChannelLast ? [outputShape[0], outputShape[2], outputShape[3]] : [outputShape[0], outputShape[1], outputShape[3]],
  );
};

const conv3d = (context: ComputeContext, inputs: readonly TensorView[], attributes: ConvAttributes): void => {
  const format = attributes.format === 'NHWC' ? 'channelsLast' : 'channelsFirst';
  const adjustedAttributes = getAdjustedConvAttributes(attributes, inputs);
  const pads = attributes.autoPad === 'NOTSET' ? attributes.pads : attributes.autoPad;
  const convInfo = computeConv3DInfo(
    inputs[0].dims as [number, number, number, number, number],
    inputs[1].dims as [number, number, number, number, number],
    attributes.strides as number | [number, number, number],
    attributes.dilations as number | [number, number, number],
    pads as string | number[],
    false,
    format,
  );
  context.compute(
    createConv3DNaiveProgramInfo(
      inputs,
      adjustedAttributes,
      convInfo.outShape,
      [convInfo.filterDepth, convInfo.filterHeight, convInfo.filterWidth],
      [convInfo.padInfo.front, convInfo.padInfo.top, convInfo.padInfo.left],
      format,
    ),
  );
};

export const conv = (context: ComputeContext, attributes: ConvAttributes): void => {
  validateInputs(context.inputs, attributes);
  if (context.inputs[0].dims.length === 3) {
    conv1d(context, attributes);
  } else if (context.inputs[0].dims.length === 5) {
    conv3d(context, context.inputs, attributes);
  } else {
    const adjustedAttributes = getAdjustedConvAttributes(attributes, context.inputs);
    conv2d(context, context.inputs, adjustedAttributes);
  }
};
