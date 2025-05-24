// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { ComputeContext, ProgramInputTensorInfoDependency, ProgramUniform } from '../types';
import { createTransposeProgramInfo } from './transpose';

import {
  createTensorShapeVariables,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  sumVector,
  tensorTypeToWsglStorageType,
} from './common';

export interface InstanceNormAttributes {
  epsilon: number;
  format: 'NHWC' | 'NCHW';
}

const computeChannelScaleShift = (
  context: ComputeContext,
  input: TensorView,
  scale: TensorView,
  bias: TensorView,
  n: number,
  h: number,
  c: number,
  epsilon: number,
) => {
  const components = getMaxComponents(h);
  const f32Type = components === 1 ? 'f32' : `vec${components}f`;
  const wgType = components === 1 ? 'vec2f' : `mat2x${components}f`;
  const unitsOfWork = n * c;
  let workgroupSize = 64;
  if (unitsOfWork === 1) {
    workgroupSize = 256;
  }
  const inputShape = [n, c, h / components];
  const outputShape = [n, c, 2];
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['rank', 'type', 'type'];
  const programUniforms: ProgramUniform[] = [];
  programUniforms.push(...createTensorShapeVariables(inputShape, outputShape));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const x = inputVariable('x', input.dataType, 3, components);
    const s = inputVariable('scale', scale.dataType, scale.dims);
    const b = inputVariable('bias', bias.dataType, bias.dims);
    const output = outputVariable('output', DataType.float, 3, 2);
    const variables = [x, s, b, output];
    return `
  var<workgroup> workgroup_shared : array<${wgType}, ${workgroupSize}>;
  const workgroup_size = ${workgroupSize}u;
  ${shaderHelper.declareVariables(...variables)}
  ${shaderHelper.mainStart(workgroupSize)}
    let batch = workgroup_index / uniforms.x_shape[1];
    let channel = workgroup_index % uniforms.x_shape[1];
    let hight = uniforms.x_shape[2];
    // initialize workgroup memory
    var sum = ${f32Type}(0);
    var squared_sum = ${f32Type}(0);
    for (var h = local_idx; h < hight; h += workgroup_size) {
      let value = ${f32Type}(${x.get('batch', 'channel', 'h')});
      sum += value;
      squared_sum += value * value;
    }
    workgroup_shared[local_idx] = ${wgType}(sum, squared_sum);
    workgroupBarrier();

    for (var currSize = workgroup_size >> 1;  currSize > 0; currSize = currSize >> 1) {
      if (local_idx < currSize) {
        workgroup_shared[local_idx] = workgroup_shared[local_idx] + workgroup_shared[local_idx + currSize];
      }
      workgroupBarrier();
    }
    if (local_idx == 0) {
      let sum_final = ${sumVector('workgroup_shared[0][0]', components)} / f32(hight * ${components});
      let squared_sum_final = ${sumVector('workgroup_shared[0][1]', components)} / f32(hight * ${components});

      let inv_std_dev = inverseSqrt(squared_sum_final - sum_final * sum_final + f32(${epsilon}));
      let channel_scale = inv_std_dev * f32(scale[channel]);
      let channel_shift = f32(bias[channel]) - sum_final * channel_scale;
      output[workgroup_index] = vec2f(channel_scale, channel_shift);
    }
  }`;
  };

  return context.compute(
    {
      name: 'InstanceNormComputeChannelScaleShift',
      // TODO: use epsilon as uniform. Currently epsilon as uniform fails test_instancenorm_epsilon.
      shaderCache: { hint: `${components};${epsilon};${workgroupSize}`, inputDependencies },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: DataType.float }],
        dispatchGroup: { x: unitsOfWork },
        programUniforms,
      }),
      getShaderSource,
    },
    { inputs: [input, scale, bias], outputs: [-1] },
  )[0];
};

const createInstanceNormProgramInfo = (
  context: ComputeContext,
  inputs: readonly TensorView[],
  attributes: InstanceNormAttributes,
) => {
  const xShape = inputs[0].dims;
  const outputShape = xShape;
  const axis = 2;
  const N = xShape[0];
  const C = xShape[1];
  const H = ShapeUtil.sizeFromDimension(xShape, axis);
  const components = getMaxComponents(H);
  const outputSize = ShapeUtil.size(outputShape) / components;
  // compute channel scale and channel shift.
  const channelScaleShift = computeChannelScaleShift(
    context,
    inputs[0],
    inputs[1],
    inputs[2],
    N,
    H,
    C,
    attributes.epsilon,
  );

  const inputShape = [N, C, H / components];
  const scaleShape = [N, C];
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['type', 'none'];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const x = inputVariable('x', inputs[0].dataType, inputShape.length, components);
    const scale = inputVariable('scale_shift', DataType.float, scaleShape.length, 2);
    const output = outputVariable('output', inputs[0].dataType, inputShape.length, components);
    const variables = [x, scale, output];
    return `
  ${shaderHelper.registerUniform('output_size', 'u32').declareVariables(...variables)}
  ${shaderHelper.mainStart()}
  ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
      let outputIndices = ${output.offsetToIndices('global_idx')};
      let batch = outputIndices[0];
      let channel = outputIndices[1];
      let scale_shift = ${scale.getByIndices('vec2<u32>(batch, channel)')};
      let value = ${x.getByOffset('global_idx')} * ${output.type.value}(scale_shift.x) + ${output.type.value}(scale_shift.y);
      ${output.setByOffset('global_idx', 'value')};
  }`;
  };

  context.compute(
    {
      name: 'InstanceNormalization',
      shaderCache: { hint: `${components}`, inputDependencies },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms: [
          { type: DataType.uint32, data: outputSize },
          ...createTensorShapeVariables(inputShape, scaleShape, inputShape),
        ],
      }),
      getShaderSource,
    },
    { inputs: [inputs[0], channelScaleShift] },
  );
};

const createInstanceNormNHWCProgramInfo = (
  context: ComputeContext,
  inputs: readonly TensorView[],
  attributes: InstanceNormAttributes,
) => {
  const xShape = inputs[0].dims;
  const outputShape = xShape;
  const N = xShape[0];
  const C = xShape[xShape.length - 1];
  const H = ShapeUtil.sizeFromDimension(xShape, 1) / C;
  const components = getMaxComponents(C);
  const outputSize = ShapeUtil.size(outputShape) / components;
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: H },
    { type: DataType.uint32, data: Math.floor(C / components) },
  ];
  const inputDependencies: ProgramInputTensorInfoDependency[] = ['type', 'type'];

  // 1. transpose x from NHWC to NCHW
  let needTranspose = false;
  const transposedXPerm = [0, xShape.length - 1];
  for (let i = 0; i < xShape.length - 2; i++) {
    needTranspose = needTranspose || xShape[i + 1] !== 1;
    transposedXPerm.push(i + 1);
  }

  needTranspose = needTranspose && xShape[xShape.length - 1] !== 1;

  const transposedX = needTranspose
    ? context.compute(createTransposeProgramInfo(context.inputs[0], transposedXPerm), {
        inputs: [context.inputs[0]],
        outputs: [-1],
      })[0]
    : context.inputs[0].reshape(Array.from({ length: xShape.length }, (_, i) => xShape[transposedXPerm[i]]));
  // 2. compute channel scale and channel shift.
  const channelScaleShift = computeChannelScaleShift(
    context,
    transposedX,
    inputs[1],
    inputs[2],
    N,
    H,
    C,
    attributes.epsilon,
  );
  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);
    const scaleType = components === 1 ? 'vec2f' : `mat${components}x2f`;
    const scaleData = (num: number) => {
      const index = num === 0 ? 'x' : 'y';
      const f32Type = components === 1 ? 'f32' : `vec${components}f`;
      switch (components) {
        case 1:
          return `${dataType}(${f32Type}(scale.${index}))`;
        case 2:
          return `vec2<${dataType}>(${f32Type}(scale[0].${index}, scale[1].${index}))`;
        case 4:
          return `vec4<${dataType}>(${f32Type}(scale[0].${index}, scale[1].${index}, scale[2].${index}, scale[3].${index}))`;
        default:
          throw new Error(`Not supported compoents ${components}`);
      }
    };
    const inputHelper = inputVariable('input', inputs[0].dataType, inputs[0].dims, components);
    const outputHelper = outputVariable('output', inputs[0].dataType, outputShape, components);

    return `
  @group(0) @binding(0) var<storage, read> input : array<${inputHelper.type.storage}>;
  @group(0) @binding(1) var<storage, read> scale_input : array<${scaleType}>;
  @group(0) @binding(2) var<storage, read_write> output : array<${outputHelper.type.storage}>;
  struct Uniforms {H: u32, C : u32};
  @group(0) @binding(3) var<uniform> uniforms: Uniforms;

  ${shaderHelper.mainStart()}
    let current_image_number = global_idx / (uniforms.C * uniforms.H);
    let current_channel_number = global_idx % uniforms.C;

    let scale_offset = current_image_number * uniforms.C + current_channel_number;
    let scale = scale_input[scale_offset];
    output[global_idx] = fma(input[global_idx], ${scaleData(0)}, ${scaleData(1)});
  }`;
  };
  context.compute(
    {
      name: 'InstanceNormalizationNHWC',
      shaderCache: { hint: `${components}`, inputDependencies },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: inputs[0].dataType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms,
      }),
      getShaderSource,
    },
    { inputs: [inputs[0], channelScaleShift] },
  );
};

export const instanceNorm = (context: ComputeContext, attributes: InstanceNormAttributes): void => {
  if (attributes.format === 'NHWC') {
    createInstanceNormNHWCProgramInfo(context, context.inputs, attributes);
  } else {
    createInstanceNormProgramInfo(context, context.inputs, attributes);
  }
};
