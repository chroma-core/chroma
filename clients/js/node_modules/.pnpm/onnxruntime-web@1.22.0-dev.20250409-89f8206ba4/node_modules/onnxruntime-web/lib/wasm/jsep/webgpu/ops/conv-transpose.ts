// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { TensorView } from '../../tensor-view';
import { ComputeContext } from '../types';

import { createConvTranspose2DProgramInfo } from './3rd-party/conv_backprop_webgpu';
import { ConvAttributes } from './conv';
import { parseInternalActivationAttributes } from './fuse-utils';
import { createTransposeProgramInfo } from './transpose';

const computeTotalPad = (
  inDim: number,
  stride: number,
  adj: number,
  kernel: number,
  dilation: number,
  outSize: number,
) => (inDim - 1) * stride + adj + (kernel - 1) * dilation + 1 - outSize;

const distributePadding = (totalPad: number, autoPad: string, pads: number[], head: number, tail: number) => {
  const smallPad = Math.floor(totalPad / 2);
  if (autoPad === 'SAME_UPPER') {
    pads[head] = smallPad;
    pads[tail] = totalPad - smallPad;
  } else if (autoPad === 'SAME_LOWER') {
    pads[head] = totalPad - smallPad;
    pads[tail] = smallPad;
  }
};

const calculateOutputShapeAndPads = (
  inputShape: readonly number[],
  kernelShape: readonly number[],
  dilations: readonly number[],
  autoPad: string,
  group: number,
  pads: number[],
  strides: readonly number[],
  isChannelLast: boolean,
  outputPadding: number[],
  outputShape: number[],
) => {
  const spatialRank = inputShape.length - 2;
  const updateOutputShape = outputShape.length === 0;
  if (outputPadding.length < spatialRank) {
    outputPadding.push(...Array(spatialRank - outputPadding.length).fill(0));
  }
  const batchSize = inputShape[0];
  const outChannels = kernelShape[isChannelLast ? 3 : 1] * group;
  for (let i = 0, j = inputShape.length - spatialRank - (isChannelLast ? 1 : 0); i < spatialRank; ++i, ++j) {
    const inSize = inputShape[j];
    const outSize = updateOutputShape ? inSize * strides[i] : outputShape[i];
    const totalPad = computeTotalPad(inSize, strides[i], pads[i], kernelShape[j], dilations[i], outSize);
    distributePadding(totalPad, autoPad, pads, i, i + spatialRank);
    if (updateOutputShape) {
      outputShape.push(
        strides[i] * (inSize - 1) +
          outputPadding[i] +
          (kernelShape[j] - 1) * dilations[i] +
          1 -
          pads[i] -
          pads[i + spatialRank],
      );
    }
  }
  outputShape.splice(0, 0, batchSize);
  outputShape.splice(isChannelLast ? 3 : 1, 0, outChannels);
};

export interface ConvTransposeAttributes extends ConvAttributes {
  readonly outputPadding: readonly number[];
  readonly outputShape: readonly number[];
}

const getAdjustedConvTransposeAttributes = <T extends ConvTransposeAttributes>(
  attributes: T,
  inputs: readonly TensorView[],
): T => {
  const kernelShape = attributes.kernelShape.slice();
  // if kernelShape is not specified in the attributes of this op, infer it from the weight tensor dims
  if (attributes.kernelShape.length === 0 || attributes.kernelShape.reduce((a, b) => a * b, 1) === 0) {
    kernelShape.length = 0;
    for (let i = 2; i < inputs[1].dims.length; ++i) {
      kernelShape.push(inputs[1].dims[i]);
    }
  }
  const isChannelsLast = attributes.format === 'NHWC';
  kernelShape.splice(0, 0, inputs[1].dims[0]);
  kernelShape.splice(isChannelsLast ? 3 : 1, 0, inputs[1].dims[1]);

  const pads = attributes.pads.slice();
  const outputShape = attributes.outputShape.slice();
  const outputPadding = attributes.outputPadding.slice();
  const inputShape = inputs[0].dims;
  let dilations = attributes.dilations.slice();
  if (dilations.reduce((a, b) => a + b, 0) === 0) {
    const spatialRank = inputs[0].dims.length - 2;
    dilations = new Array(spatialRank).fill(1);
  }
  let strides = attributes.strides.slice();
  if (strides.reduce((a, b) => a + b, 0) === 0) {
    const spatialRank = inputs[0].dims.length - 2;
    strides = new Array(spatialRank).fill(1);
  }
  // If outputShape is not specified in the attributes of this op, infer it from the parameters
  // Similarly, automatically infer pads if not specified
  calculateOutputShapeAndPads(
    inputShape,
    kernelShape,
    dilations,
    attributes.autoPad,
    attributes.group,
    pads,
    strides,
    isChannelsLast,
    outputPadding,
    outputShape,
  );

  // always return a new object so does not modify the original attributes
  const newAttributes: T = Object.assign({}, attributes);
  Object.assign(newAttributes, { kernelShape, pads, outputPadding, outputShape, dilations, strides });
  return newAttributes;
};

export const parseConvTransposeAttributes = (attributes: Record<string, unknown>): ConvTransposeAttributes => {
  const activationAttributes = parseInternalActivationAttributes(attributes);
  // TODO : Make this generic enough to compute default attributes for multi-dimensional conv
  const format = attributes.format as 'NHWC' | 'NCHW';
  const autoPad = ['NOTSET', 'VALID', 'SAME_UPPER', 'SAME_LOWER'][
    typeof attributes.autoPad == 'undefined' ? 0 : (attributes.autoPad as number)
  ];
  const dilations = attributes.dilations as [number, number];
  const group = attributes.group as number;
  const kernelShape = attributes.kernelShape as [number, number];
  const pads = attributes.pads as [number, number, number, number];
  const strides = attributes.strides as [number, number];
  const wIsConst = (attributes.wIsConst as () => boolean)();
  const outputPadding = attributes.outputPadding as [number, number, number, number];
  const outputShape = attributes.outputShape as [number, number];
  return {
    autoPad,
    format,
    dilations,
    group,
    kernelShape,
    outputPadding,
    outputShape,
    pads,
    strides,
    wIsConst,
    ...activationAttributes,
    cacheKey: `${attributes.format};${activationAttributes.activation};`,
  };
};

const validateInputs = (inputs: readonly TensorView[], attributes: ConvTransposeAttributes): void => {
  // Refer to the below link for all input checks
  // https://github.com/onnx/onnx/blob/main/docs/Operators.md#ConvTranspose
  if (!inputs || (inputs.length !== 2 && inputs.length !== 3)) {
    throw new Error('Conv requires 2 or 3 inputs');
  }

  // TODO : Need to add support for multi-dimensional conv
  if (inputs[0].dims.length !== 4 && inputs[0].dims.length !== 3) {
    throw new Error('currently only support 2-dimensional conv');
  }

  if (inputs[0].dims.length !== inputs[1].dims.length) {
    throw new Error('filter does not have same dimension as input');
  }

  // FILTER_IN_CHANNEL should be equal to DATA_CHANNEL
  const dataChannel = inputs[0].dims[attributes.format === 'NHWC' ? inputs[0].dims.length - 1 : 1];
  const filterInChannel = inputs[1].dims[0];
  if (dataChannel !== filterInChannel) {
    throw new Error('FILTER_IN_CHANNEL should be equal to DATA_CHANNEL');
  }

  const featureMaps = inputs[1].dims[1] * attributes.group;

  // if bias is provided it should be 1D and the number of elements should be equal to the number of feature maps
  if (inputs.length === 3 && (inputs[2].dims.length !== 1 || inputs[2].dims[0] !== featureMaps)) {
    throw new Error('invalid bias');
  }

  const spatialRank = inputs[0].dims.length - 2;
  const dilationsSet = attributes.dilations.reduce((a, b) => a + b, 0) > 0;
  // wrong dilations dimension
  if (dilationsSet && attributes.dilations.length !== spatialRank) {
    throw new Error(`dilations should be ${spatialRank}D`);
  }

  const stridesSet = attributes.strides.reduce((a, b) => a + b, 0) > 0;
  // Wrong strides dimension
  if (stridesSet && attributes.strides.length !== spatialRank) {
    throw new Error(`strides should be ${spatialRank}D`);
  }

  // Wrong pads dimension
  const padsSet = attributes.pads.reduce((a, b) => a + b, 0) > 0;
  if (padsSet && attributes.pads.length !== spatialRank * 2) {
    throw new Error(`pads should be ${spatialRank * 2}D`);
  }

  // Wrong output padding dimension
  if (attributes.outputPadding.length !== spatialRank && attributes.outputPadding.length !== 0) {
    throw new Error(`output_padding should be ${spatialRank}D`);
  }

  // if kernelShape is specified, it's data length must be 2 less than dims length of the weights tensor
  // (the first 2 dims are batch_size and channels)
  const kernelShapeSet = attributes.kernelShape.reduce((a, b) => a + b, 0) > 0;
  if (
    kernelShapeSet &&
    attributes.kernelShape.length !== 0 &&
    attributes.kernelShape.length !== inputs[1].dims.length - 2
  ) {
    throw new Error('invalid kernel shape');
  }

  // as with kernelShape, must have same number of spatial dims as input
  if (attributes.outputShape.length !== 0 && attributes.outputShape.length !== inputs[0].dims.length - 2) {
    throw new Error('invalid output shape');
  }
};

const convTranspose2d = (
  context: ComputeContext,
  inputs: readonly TensorView[],
  attributes: ConvTransposeAttributes,
  squeezeOutputShapeFunction?: (shape: readonly number[]) => number[],
): void => {
  // STEP.1: transpose weight
  const transposedWeight =
    (context.kernelCustomData.wT as TensorView | undefined) ??
    context.compute(createTransposeProgramInfo(inputs[1], [2, 3, 0, 1]), {
      inputs: [1],
      outputs: [attributes.wIsConst ? -2 : -1],
    })[0];
  if (attributes.wIsConst && !context.kernelCustomData.wT) {
    context.kernelCustomData.wT = transposedWeight;
  }

  // STEP.2: prepare reshaped inputs
  const convTransposeInputs = [inputs[0], transposedWeight];
  if (inputs.length === 3) {
    convTransposeInputs.push(inputs[2]);
  }
  context.compute(createConvTranspose2DProgramInfo(convTransposeInputs, attributes, squeezeOutputShapeFunction), {
    inputs: convTransposeInputs,
  });
};

const convTranspose1d = (context: ComputeContext, attributes: ConvTransposeAttributes): void => {
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
  let kernelShape = attributes.kernelShape;
  if (kernelShape.length === 0 || kernelShape[0] === 0) {
    kernelShape = [context.inputs[1].dims[2]];
  }
  let dilations = attributes.dilations;
  if (dilations.length === 0 || dilations[0] === 0) {
    dilations = [1];
  }
  let strides = attributes.strides;
  if (strides.length === 0 || strides[0] === 0) {
    strides = [1];
  }
  let pads = attributes.pads;
  if (pads.length === 0) {
    pads = [0, 0];
  }
  pads = [0, pads[0], 0, pads[1]];
  strides = [1].concat(strides);
  dilations = [1].concat(dilations);
  kernelShape = [1].concat(kernelShape);
  let outputPadding = attributes.outputPadding;
  outputPadding = [0].concat(outputPadding);
  const adjustedAttributes = getAdjustedConvTransposeAttributes(
    { ...attributes, pads, strides, dilations, kernelShape, outputPadding },
    inputs,
  );

  convTranspose2d(context, inputs, adjustedAttributes, (outputShape) =>
    isChannelLast ? [outputShape[0], outputShape[2], outputShape[3]] : [outputShape[0], outputShape[1], outputShape[3]],
  );
};

export const convTranspose = (context: ComputeContext, attributes: ConvTransposeAttributes): void => {
  validateInputs(context.inputs, attributes);
  if (context.inputs[0].dims.length === 3) {
    convTranspose1d(context, attributes);
  } else {
    const adjustedAttributes = getAdjustedConvTransposeAttributes(attributes, context.inputs);
    convTranspose2d(context, context.inputs, adjustedAttributes);
  }
};
