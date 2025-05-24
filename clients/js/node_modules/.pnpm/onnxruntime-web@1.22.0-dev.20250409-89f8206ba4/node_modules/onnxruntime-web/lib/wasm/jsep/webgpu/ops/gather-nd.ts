// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramUniform } from '../types';

import { createTensorShapeVariables, inputVariable, outputVariable, ShaderHelper, UniformsArrayType } from './common';

export interface GatherNDAttributes extends AttributeWithCacheKey {
  readonly batchDims: number;
}

const computeSliceOffsets = (
  context: ComputeContext,
  indicesData: TensorView,
  sizesFromSliceDimsData: number[],
  batchDims: number,
  inputDims: readonly number[],
  numSlices: number,
  numSlicesPerBatch: number,
  inputBatchStride: number,
  numSliceDims: number,
) => {
  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: numSlices },
    { type: DataType.uint32, data: batchDims },
    { type: DataType.uint32, data: inputDims },
    { type: DataType.uint32, data: sizesFromSliceDimsData },
    { type: DataType.uint32, data: numSlicesPerBatch },
    { type: DataType.uint32, data: inputBatchStride },
    { type: DataType.uint32, data: numSliceDims },
  ];

  const outputShape = [numSlices];
  programUniforms.push(...createTensorShapeVariables(indicesData.dims, outputShape));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const indices = inputVariable('indices_data', indicesData.dataType, indicesData.dims.length);
    const output = outputVariable('input_slice_offsets_data', DataType.uint32, 1, 1);
    const variables = [indices, output];
    const uniforms: UniformsArrayType = [
      { name: 'output_size', type: 'u32' },
      { name: 'batch_dims', type: 'u32' },
      { name: 'input_dims', type: 'u32', length: inputDims.length },
      { name: 'sizes_from_slice_dims_data', type: 'u32', length: sizesFromSliceDimsData.length },
      { name: 'num_slices_per_batch', type: 'u32' },
      { name: 'input_batch_stride', type: 'u32' },
      { name: 'num_slice_dims', type: 'u32' },
    ];
    return `
  ${shaderHelper.registerUniforms(uniforms).declareVariables(...variables)}
  ${shaderHelper.mainStart()}
    ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
    let batch_idx = global_idx / uniforms.num_slices_per_batch;
    let base_offset = batch_idx * uniforms.input_batch_stride;

    let slice_indices_base_offset = global_idx * uniforms.num_slice_dims;
    var relative_slice_offset = 0;
    for (var dim_idx = 0u; dim_idx < uniforms.num_slice_dims; dim_idx ++) {
      var index = i32(indices_data[dim_idx + slice_indices_base_offset].x);
      let input_dim_idx = uniforms.batch_dims + dim_idx;
      if (index < 0) {
        ${
          inputDims.length === 1
            ? 'index += i32(uniforms.input_dims);'
            : 'index += i32(uniforms.input_dims[input_dim_idx]);'
        }
      }
      ${
        sizesFromSliceDimsData.length === 1
          ? 'relative_slice_offset += index * i32(uniforms.sizes_from_slice_dims_data);'
          : 'relative_slice_offset += index * i32(uniforms.sizes_from_slice_dims_data[dim_idx]);'
      }
    }

    input_slice_offsets_data[global_idx] =  base_offset + u32(relative_slice_offset);
  }`;
  };

  return context.compute(
    {
      name: 'computeSliceOffsets',
      shaderCache: { hint: `${inputDims.length}_${sizesFromSliceDimsData.length}`, inputDependencies: ['rank'] },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: context.inputs[1].dataType }],
        dispatchGroup: { x: Math.ceil(numSlices / 64) },
        programUniforms,
      }),
      getShaderSource,
    },
    { inputs: [indicesData], outputs: [-1] },
  )[0];
};

export const gatherND = (context: ComputeContext, attributes: GatherNDAttributes) => {
  const inputs = context.inputs;
  const inputShape = inputs[0].dims;
  const inputType = inputs[0].dataType;
  const indicesShape = inputs[1].dims;
  const numSliceDims = indicesShape[indicesShape.length - 1];
  const numSlices = ShapeUtil.sizeToDimension(indicesShape, indicesShape.length - 1);
  const sliceSize = ShapeUtil.sizeFromDimension(inputShape, attributes.batchDims + numSliceDims);
  const numBatches = ShapeUtil.sizeToDimension(inputShape, attributes.batchDims);
  const inputBatchStride = ShapeUtil.sizeFromDimension(inputShape, attributes.batchDims);
  const numSlicesPerBatch = numSlices / numBatches;
  const sizesFromSliceDims = new Array(numSliceDims);
  let runningProduct = sliceSize;
  for (let i = 0; i < numSliceDims; ++i) {
    sizesFromSliceDims[numSliceDims - 1 - i] = runningProduct;
    runningProduct *= inputShape[attributes.batchDims + numSliceDims - 1 - i];
  }

  const inputSliceOffsets = computeSliceOffsets(
    context,
    inputs[1],
    sizesFromSliceDims,
    attributes.batchDims,
    inputShape,
    numSlices,
    numSlicesPerBatch,
    inputBatchStride,
    numSliceDims,
  );

  const lastIndicesDimension = attributes.batchDims + numSliceDims;
  if (lastIndicesDimension > inputShape.length) {
    throw new Error('last dimension of indices must not be larger than rank of input tensor');
  }

  const outputShape = indicesShape.slice(0, -1).concat(inputShape.slice(lastIndicesDimension));
  const outputSize = ShapeUtil.size(outputShape);

  const programUniforms: ProgramUniform[] = [
    { type: DataType.uint32, data: outputSize },
    { type: DataType.uint32, data: sliceSize },
    ...createTensorShapeVariables(inputs[0].dims, inputSliceOffsets.dims, outputShape),
  ];

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const input = inputVariable('data', inputs[0].dataType, inputs[0].dims.length);
    const indices = inputVariable('slice_offsets', DataType.uint32, inputSliceOffsets.dims.length);

    const output = outputVariable('output', inputs[0].dataType, outputShape.length);
    return `
          ${shaderHelper
            .registerUniform('output_size', 'u32')
            .registerUniform('slice_size', 'u32')
            .declareVariables(input, indices, output)}
            ${shaderHelper.mainStart()}
            ${shaderHelper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.output_size')}
          let slice_offset = slice_offsets[global_idx / uniforms.slice_size];
          output[global_idx] = data[u32(slice_offset) + global_idx % uniforms.slice_size];
        }`;
  };
  context.compute(
    {
      name: 'GatherND',
      shaderCache: { hint: attributes.cacheKey, inputDependencies: ['rank', 'rank'] },
      getRunData: () => ({
        outputs: [{ dims: outputShape, dataType: inputType }],
        dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
        programUniforms,
      }),
      getShaderSource,
    },
    { inputs: [inputs[0], inputSliceOffsets] },
  );
};

export const parseGatherNDAttributes = (attributes: Record<string, unknown>): GatherNDAttributes => {
  const batchDims = attributes.batch_dims as number;
  return {
    batchDims,
    cacheKey: '',
  };
};
