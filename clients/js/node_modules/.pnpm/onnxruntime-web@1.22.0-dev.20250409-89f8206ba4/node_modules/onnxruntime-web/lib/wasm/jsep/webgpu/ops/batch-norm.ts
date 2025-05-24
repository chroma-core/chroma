// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { env } from 'onnxruntime-common';

import { DataType } from '../../../wasm-common';
import { TensorView } from '../../tensor-view';
import { ShapeUtil } from '../../util';
import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../attribute-with-cache-key';
import { ComputeContext, ProgramInfo } from '../types';

import { createTensorShapeVariables, getMaxComponents, inputVariable, outputVariable, ShaderHelper } from './common';

export interface BatchNormAttributes extends AttributeWithCacheKey {
  readonly epsilon: number;
  readonly momentum: number;
  readonly spatial: boolean;
  readonly trainingMode: boolean;
  readonly format: 'NHWC' | 'NCHW';
  readonly outputCount: number;
}

const validateInputs = (inputs: readonly TensorView[], attributes: BatchNormAttributes): void => {
  if (!inputs || inputs.length !== 5) {
    throw new Error('BatchNormalization requires 5 inputs');
  }

  const checkShapeEqual = (actual: readonly number[], expected: readonly number[], message: string) => {
    const r = expected.length;
    if (r !== actual.length) {
      throw new Error(`${message}: num dimensions != ${r}`);
    }
    expected.forEach((v, i) => {
      if (v !== actual[i]) {
        throw new Error(`${message}: dim[${i}] do not match`);
      }
    });
  };

  if (inputs[0].dims.length > 1) {
    const shape =
      attributes.format === 'NHWC'
        ? attributes.spatial
          ? inputs[0].dims.slice(-1)
          : inputs[0].dims.slice(-1).concat(inputs[0].dims.slice(1, inputs[0].dims.length - 1))
        : inputs[0].dims.slice(1, attributes.spatial ? 2 : undefined);
    checkShapeEqual(inputs[1].dims, shape, 'Invalid input scale');
    checkShapeEqual(inputs[2].dims, shape, 'Invalid input B');
    checkShapeEqual(inputs[3].dims, shape, 'Invalid input mean');
    checkShapeEqual(inputs[4].dims, shape, 'Invalid input var');
  } else {
    checkShapeEqual(inputs[1].dims, [1], 'Invalid input scale');
    checkShapeEqual(inputs[2].dims, [1], 'Invalid input B');
    checkShapeEqual(inputs[3].dims, [1], 'Invalid input mean');
    checkShapeEqual(inputs[4].dims, [1], 'Invalid input var');
  }
};

const createBatchNormInferenceProgramInfo = (
  inputs: readonly TensorView[],
  attributes: BatchNormAttributes,
): ProgramInfo => {
  const { epsilon, spatial, format } = attributes;
  const yShape = inputs[0].dims;
  const components = spatial ? getMaxComponents(yShape[yShape.length - 1]) : 1;
  const cComponents = format === 'NHWC' && yShape.length > 1 ? components : 1;
  const outputSize = ShapeUtil.size(yShape) / components;
  // Only support uniforms for opset version >= 9 (spatial = true).
  const useShapesUniforms = spatial;
  const shapeOrRank = useShapesUniforms ? yShape.length : yShape;
  const x = inputVariable('x', inputs[0].dataType, inputs[0].dims, components);
  const scale = inputVariable('scale', inputs[1].dataType, inputs[1].dims, cComponents);
  const bias = inputVariable('bias', inputs[2].dataType, inputs[2].dims, cComponents);
  const inputMean = inputVariable('inputMean', inputs[3].dataType, inputs[3].dims, cComponents);
  const inputVar = inputVariable('inputVar', inputs[4].dataType, inputs[4].dims, cComponents);
  const y = outputVariable('y', inputs[0].dataType, shapeOrRank, components);
  // TODO: support inputs with different data type. Current we need to make sure all inputs have the same data type.
  // Otherwise, the shader compilation will fail.
  const calcCOffset = (): string => {
    let cOffset = '';
    if (spatial) {
      cOffset = `let cOffset = ${
        yShape.length === 1
          ? '0u'
          : format === 'NHWC'
            ? `outputIndices[${yShape.length - 1}] / ${components}`
            : 'outputIndices[1]'
      };`;
    } else {
      if (format === 'NCHW') {
        cOffset = `
            ${y.indicesSet('outputIndices', '0', '0')}
            let cOffset = ${y.indicesToOffset('outputIndices')};`;
      } else {
        // update C channel.
        cOffset = `var cIndices = ${scale.type.indices}(0);
                       cIndices[0] = outputIndices[${yShape.length - 1}];`;
        // update D1 x ... x Dn channels.
        for (let i = 1; i < scale.rank; i++) {
          cOffset += `cIndices[${i}] = outputIndices[${i}];`;
        }
        cOffset += `let cOffset = ${scale.indicesToOffset('cIndices')};`;
      }
    }
    return cOffset;
  };
  const getInferenceModeShaderSource = (helper: ShaderHelper) => `
  const epsilon = ${epsilon};
  ${helper.registerUniform('outputSize', 'u32').declareVariables(x, scale, bias, inputMean, inputVar, y)}
  ${helper.mainStart()}
  ${helper.guardAgainstOutOfBoundsWorkgroupSizes('uniforms.outputSize')}
    var outputIndices = ${y.offsetToIndices(`global_idx * ${components}`)};
    ${calcCOffset()}
    let scale = ${scale.getByOffset('cOffset')};
    let bias = ${bias.getByOffset('cOffset')};
    let inputMean = ${inputMean.getByOffset('cOffset')};
    let inputVar = ${inputVar.getByOffset('cOffset')};
    let x = ${x.getByOffset('global_idx')};
    let value = (x - inputMean) * inverseSqrt(inputVar + epsilon) * scale + bias;
    ${y.setByOffset('global_idx', 'value')}
  }`;
  return {
    name: 'BatchNormalization',
    shaderCache: {
      hint: `${attributes.epsilon}_${attributes.format}_${spatial}_${components}`,
      inputDependencies: useShapesUniforms ? ['rank', 'type', 'type', 'type', 'type'] : undefined,
    },
    getShaderSource: getInferenceModeShaderSource,
    getRunData: () => ({
      outputs: [{ dims: inputs[0].dims, dataType: inputs[0].dataType }],
      dispatchGroup: { x: Math.ceil(outputSize / 64 /* workgroup size */) },
      programUniforms: useShapesUniforms
        ? [{ type: DataType.uint32, data: outputSize }, ...createTensorShapeVariables(yShape)]
        : [{ type: DataType.uint32, data: outputSize }],
    }),
  };
};

export const parseBatchNormAttributes = (attributes: Record<string, unknown>): BatchNormAttributes =>
  createAttributeWithCacheKey(attributes as Omit<BatchNormAttributes, keyof AttributeWithCacheKey>);

export const batchNorm = (context: ComputeContext, attributes: Record<string, unknown>): void => {
  const { inputs, outputCount } = context;
  const updatedAttributes = parseBatchNormAttributes({ ...attributes, outputCount });
  if (env.webgpu.validateInputContent) {
    validateInputs(inputs, updatedAttributes);
  }
  if (attributes.trainingMode) {
    throw new Error('BatchNormalization trainingMode is not supported yet.');
  } else {
    context.compute(createBatchNormInferenceProgramInfo(inputs, updatedAttributes));
  }
};
