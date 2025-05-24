// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo, ProgramUniform } from '../types';

import {
  createTensorShapeVariables,
  getMaxComponents,
  inputVariable,
  outputVariable,
  ShaderHelper,
  tensorTypeToWsglStorageType,
} from './common';

//  TODO support quantization bits not equal to 4
export interface MatMulNBitsAttributes extends AttributeWithCacheKey {
  k: number;
  n: number;
  accuracyLevel: number;
  bits: number;
  blockSize: number;
}

const validateInputs = (inputs: readonly TensorView[], attributes: MatMulNBitsAttributes): void => {
  if (inputs.length < 3 || inputs.length > 4) {
    throw new Error('MatMulNBits requires 3 or 4 inputs');
  }
  const a = inputs[0];
  const aRank = a.dims.length;
  if (a.dims[aRank - 1] !== attributes.k) {
    throw new Error('The last dim of input shape does not match the k value');
  }
  const nBlocksPerCol = Math.floor((attributes.k + attributes.blockSize - 1) / attributes.blockSize);
  const blobSize = (attributes.blockSize / 8) * attributes.bits;
  const b = inputs[1];
  if (!ShapeUtil.areEqual(b.dims, [attributes.n, nBlocksPerCol, blobSize])) {
    throw new Error('The second inputs must be 3D tensor with shape N X nBlocksPerCol X blobSize');
  }
  const scales = inputs[2];
  const scalesShape = scales.dims;
  if (ShapeUtil.size(scalesShape) !== attributes.n * nBlocksPerCol) {
    throw new Error('scales input size error.');
  }
  if (inputs.length === 4) {
    const zeroPoints = inputs[3];
    const zeroPointsShape = zeroPoints.dims;
    const expectedZeroPointsSize =
      attributes.bits > 4 ? attributes.n * nBlocksPerCol : attributes.n * Math.floor((nBlocksPerCol + 1) / 2);
    if (ShapeUtil.size(zeroPointsShape) !== expectedZeroPointsSize) {
      throw new Error('zeroPoints input size error.');
    }
  }
};

export const createMatMulNBitsProgramInfo = (
  inputs: readonly TensorView[],
  attributes: MatMulNBitsAttributes,
): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const aRank = inputShape.length;
  const dimAOuter = inputShape[aRank - 2];
  const dimInner = attributes.k;
  const dimBOuter = attributes.n;
  const batchDims = inputShape.slice(0, aRank - 2);
  const batchSize = ShapeUtil.size(batchDims);
  const blobSize = inputs[1].dims[2];
  const blobSizeInWords = blobSize / 4;
  const dataType = inputs[0].dataType;
  const aComponents = getMaxComponents(attributes.k);
  const bComponents = getMaxComponents(blobSizeInWords);
  const components = getMaxComponents(dimBOuter);
  const outputShape = batchDims.concat([dimAOuter, dimBOuter]);
  const outputNumber = dimAOuter > 1 && (dimBOuter / components) % 2 === 0 ? 2 : 1;
  const dispatchSize = ShapeUtil.size(outputShape) / components / outputNumber;

  const workgroupSize = 64;

  const programUniforms: ProgramUniform[] = [];
  const inputShapeTemp = [batchSize, dimAOuter, dimInner / aComponents];
  const bShape = ShapeUtil.convertShape(inputs[1].dims).slice();
  bShape.splice(-1, 1, blobSizeInWords / bComponents);
  programUniforms.push(...createTensorShapeVariables(inputShapeTemp));
  programUniforms.push(...createTensorShapeVariables(bShape));
  programUniforms.push(...createTensorShapeVariables(inputs[2].dims));
  if (inputs.length === 4) {
    programUniforms.push(...createTensorShapeVariables(ShapeUtil.convertShape(inputs[3].dims)));
  }
  const outputShapeTemp = [batchSize, dimAOuter, dimBOuter / components];
  programUniforms.push(...createTensorShapeVariables(outputShapeTemp));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const inputRank = inputShapeTemp.length;
    const a = inputVariable('a', inputs[0].dataType, inputRank, aComponents);
    const b = inputVariable('b', DataType.uint32, bShape.length, bComponents);
    const scales = inputVariable('scales', inputs[2].dataType, inputs[2].dims.length);
    const inputVariables = [a, b, scales];
    const zeroPoints =
      inputs.length === 4 ? inputVariable('zero_points', DataType.uint32, inputs[3].dims.length) : undefined;
    if (zeroPoints) {
      inputVariables.push(zeroPoints);
    }
    const outputRank = outputShapeTemp.length;
    const output = outputVariable('output', inputs[0].dataType, outputRank, components);
    const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);

    const qDqDataType = (() => {
      switch (aComponents) {
        case 1:
          return `array<${dataType}, 8>`;
        case 2:
          return `mat4x2<${dataType}>`;
        case 4:
          return `mat2x4<${dataType}>`;
        default:
          throw new Error(`${aComponents}-component is not supported.`);
      }
    })();

    const processOneWord = (): string => {
      let calcStr = `
          // reuse a data
            var input_offset = ${a.indicesToOffset(`${a.type.indices}(batch, row, word_offset)`)};
            var a_data: ${qDqDataType};
            for (var j: u32 = 0; j < ${8 / aComponents}; j++) {
              a_data[j] = ${a.getByOffset('input_offset')};
              input_offset++;
            }
          `;
      for (let c = 0; c < components * outputNumber; c++) {
        calcStr += `
            b_value = ${bComponents === 1 ? `b${c}_data` : `b${c}_data[i]`};
            b_value_lower = unpack4xU8(b_value & b_mask);
            b_value_upper = unpack4xU8((b_value >> 4) & b_mask);
            b_quantized_values = ${qDqDataType}(${Array.from(
              { length: 4 },
              (_, i) => `${dataType}(b_value_lower[${i}]), ${dataType}(b_value_upper[${i}])`,
            ).join(', ')});
            b_dequantized_values = ${(() => {
              if (aComponents === 1) {
                return `${qDqDataType}(${Array.from(
                  { length: 8 },
                  (_, i) => `(b_quantized_values[${i}] - ${zeroPoints ? `zero_point${c}` : 'zero_point'}) * scale${c}`,
                ).join(', ')});`;
              } else {
                return `(b_quantized_values - ${qDqDataType}(${Array(8)
                  .fill(`${zeroPoints ? `zero_point${c}` : 'zero_point'}`)
                  .join(',')})) * scale${c};`;
              }
            })()};
            workgroup_shared[local_id.x * ${outputNumber} + ${Math.floor(c / components)}]${components > 1 ? `[${c % components}]` : ''} += ${Array.from(
              { length: 8 / aComponents },
              (_, i) =>
                `${
                  aComponents === 1
                    ? `a_data[${i}] * b_dequantized_values[${i}]`
                    : `dot(a_data[${i}], b_dequantized_values[${i}])`
                }`,
            ).join(' + ')};
          `;
      }
      return calcStr;
    };
    const prepareScaleAndZeroPoint = (): string => {
      let calcStr = `
            var col_index = col * ${components};
            ${
              zeroPoints
                ? `
            let zero_point_bytes_per_col = (nBlocksPerCol + 1) / 2;
            var zero_point_byte_count: u32;
            var zero_point_word_index: u32;
            var zero_point_byte_offset: u32;
            let zero_point_nibble_offset: u32 = block & 0x1u;
            var zero_point_bits_offset: u32;
            var zero_point_word: u32;`
                : `
            // The default zero point is 8 for unsigned 4-bit quantization.
            let zero_point = ${dataType}(${8.0});`
            }
            `;
      for (let c = 0; c < components * outputNumber; c++) {
        calcStr += `
            let scale${c} = ${scales.getByOffset(`col_index * nBlocksPerCol + block`)};
            ${
              zeroPoints
                ? `
            zero_point_byte_count = col_index * zero_point_bytes_per_col + (block >> 0x1u);
            zero_point_word_index = zero_point_byte_count >> 0x2u;
            zero_point_byte_offset = zero_point_byte_count & 0x3u;
            zero_point_bits_offset = (zero_point_byte_offset << 3) + (zero_point_nibble_offset << 2);
            zero_point_word = ${zeroPoints.getByOffset('zero_point_word_index')} >> zero_point_bits_offset;
            let zero_point${c} = ${dataType}((zero_point_word) & 0xFu);`
                : ''
            }
            col_index += 1;`;
      }
      return calcStr;
    };
    const prepareBData = (): string => {
      let calcStr = `col_index = col * ${components};`;
      for (let c = 0; c < components * outputNumber; c++) {
        calcStr += `
            let b${c}_data = ${b.getByIndices(`${b.type.indices}(col_index, block, word)`)};
            col_index += 1;`;
      }
      calcStr += `
            var b_value: u32;
            let b_mask: u32 = 0x0F0F0F0Fu;
            var b_value_lower: vec4<u32>;
            var b_value_upper: vec4<u32>;
            var b_quantized_values: ${qDqDataType};
            var b_dequantized_values: ${qDqDataType};`;
      return calcStr;
    };
    return `
        var<workgroup> workgroup_shared: array<${output.type.value}, ${outputNumber * workgroupSize}>;
        ${shaderHelper.declareVariables(...inputVariables, output)}
        ${shaderHelper.mainStart([workgroupSize, 1, 1])}
          let output_indices = ${output.offsetToIndices(`(global_idx / ${workgroupSize}) * ${outputNumber}`)};
          let col = output_indices[2];
          let row = output_indices[1];
          let batch = output_indices[0];
          let nBlocksPerCol = uniforms.b_shape[1];

          for (var block = local_id.x; block < nBlocksPerCol; block += ${workgroupSize}) {
            //process one block
            var word_offset: u32 = block * ${attributes.blockSize / aComponents};
            ${prepareScaleAndZeroPoint()}
            for (var word: u32 = 0; word < ${blobSizeInWords}; word += ${bComponents}) {
              ${prepareBData()}
              for (var i: u32 = 0; i < ${bComponents}; i++) {
                ${processOneWord()}
                word_offset += ${8 / aComponents};
              }
            }
          }
          workgroupBarrier();

          if (local_id.x < ${outputNumber}) {
            var output_value: ${output.type.value} = ${output.type.value}(0);
            var workgroup_shared_offset: u32 = local_id.x;
            for (var b: u32 = 0u; b < ${workgroupSize}u; b++) {
              output_value += workgroup_shared[workgroup_shared_offset];
              workgroup_shared_offset += ${outputNumber};
            }
            ${output.setByIndices(`${output.type.indices}(batch, row, col + local_id.x)`, 'output_value')};
          }
        }`;
  };
  return {
    name: 'MatMulNBits',
    shaderCache: {
      hint: `${attributes.blockSize};${attributes.bits};${aComponents};${bComponents};${components};${outputNumber};${workgroupSize}`,
      inputDependencies: Array(inputs.length).fill('rank'),
    },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType }],
      dispatchGroup: { x: dispatchSize },
      programUniforms,
    }),
    getShaderSource,
  };
};

// Currently, only support blockSize = 32.
export const createMatMulNBitsBlockSize32ProgramInfo = (
  inputs: readonly TensorView[],
  attributes: MatMulNBitsAttributes,
): ProgramInfo => {
  const inputShape = inputs[0].dims;
  const aRank = inputShape.length;
  const dimAOuter = inputShape[aRank - 2];
  const dimInner = attributes.k;
  const dimBOuter = attributes.n;
  const batchDims = inputShape.slice(0, aRank - 2);
  const batchSize = ShapeUtil.size(batchDims);
  const blobSize = inputs[1].dims[2];
  const blobSizeInWords = blobSize / 4;
  const dataType = inputs[0].dataType;
  const aComponents = getMaxComponents(attributes.k);
  const bComponents = getMaxComponents(blobSizeInWords);
  const outputShape = batchDims.concat([dimAOuter, dimBOuter]);

  const workgroupSize = 128;
  const workgroupY = dimBOuter % 8 === 0 ? 8 : dimBOuter % 4 === 0 ? 4 : 1;
  const workgroupX = workgroupSize / workgroupY;
  const tileSize = workgroupX * bComponents * 8; // each uint32 has 8 data.
  const aLengthPerTile = tileSize / aComponents;
  const blocksPerTile = tileSize / attributes.blockSize;
  const dispatchSize = ShapeUtil.size(outputShape) / workgroupY;

  const programUniforms: ProgramUniform[] = [];
  const inputShapeTemp = [batchSize, dimAOuter, dimInner / aComponents];
  const bShape = ShapeUtil.convertShape(inputs[1].dims).slice();
  bShape.splice(-1, 1, blobSizeInWords / bComponents);
  programUniforms.push(...createTensorShapeVariables(inputShapeTemp));
  programUniforms.push(...createTensorShapeVariables(bShape));
  programUniforms.push(...createTensorShapeVariables(inputs[2].dims));
  if (inputs.length === 4) {
    programUniforms.push(...createTensorShapeVariables(ShapeUtil.convertShape(inputs[3].dims)));
  }
  const outputShapeTemp = [batchSize, dimAOuter, dimBOuter];
  programUniforms.push(...createTensorShapeVariables(outputShapeTemp));

  const getShaderSource = (shaderHelper: ShaderHelper) => {
    const inputRank = inputShapeTemp.length;
    const a = inputVariable('a', inputs[0].dataType, inputRank, aComponents);
    const b = inputVariable('b', DataType.uint32, bShape.length, bComponents);
    const scales = inputVariable('scales', inputs[2].dataType, inputs[2].dims.length);
    const inputVariables = [a, b, scales];
    const zeroPoints =
      inputs.length === 4 ? inputVariable('zero_points', DataType.uint32, inputs[3].dims.length) : undefined;
    if (zeroPoints) {
      inputVariables.push(zeroPoints);
    }
    const outputRank = outputShapeTemp.length;
    const output = outputVariable('output', inputs[0].dataType, outputRank);
    const dataType = tensorTypeToWsglStorageType(inputs[0].dataType);
    const readA = () => {
      switch (aComponents) {
        case 1:
          return `
          let a_data0 = vec4<${dataType}>(sub_a[word_offset], sub_a[word_offset + 1], sub_a[word_offset + 2], sub_a[word_offset + 3]);
          let a_data1 = vec4<${dataType}>(sub_a[word_offset + 4], sub_a[word_offset + 5], sub_a[word_offset + 6], sub_a[word_offset + 7]);`;
        case 2:
          return `
          let a_data0 = vec4<${dataType}>(sub_a[word_offset], sub_a[word_offset + 1]);
          let a_data1 = vec4<${dataType}>(sub_a[word_offset + 2], sub_a[word_offset + 3]);`;
        case 4:
          return `
          let a_data0 = sub_a[word_offset];
          let a_data1 = sub_a[word_offset + 1];`;
        default:
          throw new Error(`${aComponents}-component is not supported.`);
      }
    };

    return `
        var<workgroup> sub_a: array<${a.type.value}, ${aLengthPerTile}>;
        var<workgroup> inter_results: array<array<${output.type.value}, ${workgroupX}>, ${workgroupY}>;
        ${shaderHelper.declareVariables(...inputVariables, output)}
        ${shaderHelper.mainStart([workgroupX, workgroupY, 1])}
          let output_indices = ${output.offsetToIndices(`workgroup_index * ${workgroupY}`)};
          let col = output_indices[2];
          let row = output_indices[1];
          let batch = output_indices[0];
          let n_blocks_per_col = uniforms.b_shape[1];
          let num_tiles =  (n_blocks_per_col - 1) / ${blocksPerTile} + 1;

          // Loop over shared dimension.
          for (var tile: u32 = 0; tile < num_tiles; tile += 1) {
            let a_col_start = tile * ${aLengthPerTile};
            // load one tile A data into shared memory.
            for (var a_offset = local_idx; a_offset < ${aLengthPerTile}; a_offset += ${workgroupSize})
            {
              let a_col = a_col_start + a_offset;
              if (a_col < uniforms.a_shape[2])
              {
                sub_a[a_offset] = ${a.getByIndices(`${a.type.indices}(batch, row, a_col)`)};
              } else {
                sub_a[a_offset] = ${a.type.value}(0);
              }
            }
            workgroupBarrier();

            // each thread process one block
            let b_row = col + local_id.y;
            let block = tile * ${blocksPerTile} + local_id.x;
            ${
              zeroPoints
                ? `
            let zero_point_bytes_per_col = (n_blocks_per_col + 1) / 2;
            let zero_point_byte_count = b_row * zero_point_bytes_per_col + (block >> 0x1u);
            let zero_point_word_index = zero_point_byte_count >> 0x2u;
            let zero_point_byte_offset = zero_point_byte_count & 0x3u;
            let zero_point_nibble_offset: u32 = block & 0x1u;
            let zero_point_bits_offset = (zero_point_byte_offset << 3) + (zero_point_nibble_offset << 2);
            let zero_point_word = ${zeroPoints.getByOffset('zero_point_word_index')} >> zero_point_bits_offset;
            let zero_point = ${dataType}((zero_point_word) & 0xFu);`
                : `
            // The default zero point is 8 for unsigned 4-bit quantization.
            let zero_point = ${dataType}(${8.0});`
            }
            let scale = ${scales.getByOffset(`b_row * n_blocks_per_col + block`)};
            let b_data = ${b.getByIndices(`${b.type.indices}(b_row, block, 0)`)};
            var word_offset = local_id.x * ${attributes.blockSize / aComponents};
            for (var i: u32 = 0; i < ${bComponents}; i++) {
              ${readA()}
              let b_value = ${bComponents === 1 ? `b_data` : `b_data[i]`};
              let b_value_lower = unpack4xU8(b_value & 0x0F0F0F0Fu);
              let b_value_upper = unpack4xU8((b_value >> 4) & 0x0F0F0F0Fu);
              let b_quantized_values = mat2x4<${dataType}>(${Array.from(
                { length: 4 },
                (_, i) => `${dataType}(b_value_lower[${i}]), ${dataType}(b_value_upper[${i}])`,
              ).join(', ')});
              let b_dequantized_values = (b_quantized_values - mat2x4<${dataType}>(${Array(8).fill('zero_point').join(',')})) * scale;
              inter_results[local_id.y][local_id.x] += ${Array.from(
                { length: 2 },
                (_, i) => `${`dot(a_data${i}, b_dequantized_values[${i}])`}`,
              ).join(' + ')};
              word_offset += ${8 / aComponents};
            }
            workgroupBarrier();
          }

          if (local_idx < ${workgroupY}) {
            var output_value: ${output.type.value} = ${output.type.value}(0);
            for (var b = 0u; b < ${workgroupX}; b++) {
              output_value += inter_results[local_idx][b];
            }
            if (col + local_idx < uniforms.output_shape[2])
            {
              ${output.setByIndices(`${output.type.indices}(batch, row, col + local_idx)`, 'output_value')}
            }
          }
        }`;
  };
  return {
    name: 'BlockwiseMatMulNBits32',
    shaderCache: {
      hint: `${attributes.blockSize};${aComponents};${bComponents};${workgroupX};${workgroupY}`,
      inputDependencies: Array(inputs.length).fill('rank'),
    },
    getRunData: () => ({
      outputs: [{ dims: outputShape, dataType }],
      dispatchGroup: { x: dispatchSize },
      programUniforms,
    }),
    getShaderSource,
  };
};

export const matMulNBits = (context: ComputeContext, attributes: MatMulNBitsAttributes): void => {
  validateInputs(context.inputs, attributes);
  if (
    attributes.blockSize === 32 &&
    context.adapterInfo.isVendor('intel') &&
    context.adapterInfo.isArchitecture('gen-12lp')
  ) {
    context.compute(createMatMulNBitsBlockSize32ProgramInfo(context.inputs, attributes));
  } else {
    context.compute(createMatMulNBitsProgramInfo(context.inputs, attributes));
  }
};

export const parseMatMulNBitsAttributes = (attributes: Record<string, unknown>): MatMulNBitsAttributes =>
  createAttributeWithCacheKey(attributes as Omit<MatMulNBitsAttributes, keyof AttributeWithCacheKey>);
