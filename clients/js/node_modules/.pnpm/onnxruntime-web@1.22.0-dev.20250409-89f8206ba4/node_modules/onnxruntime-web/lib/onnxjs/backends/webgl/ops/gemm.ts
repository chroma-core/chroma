// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { AttributeWithCacheKey, createAttributeWithCacheKey } from '../../../attribute-with-cache-key';
import { Graph } from '../../../graph';
import { OperatorImplementation, OperatorInitialization } from '../../../operators';
import { Tensor } from '../../../tensor';
import { GemmUtil } from '../../../util';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, ProgramMetadata, TextureType } from '../types';

export interface GemmAttributes extends AttributeWithCacheKey {
  transA: boolean;
  transB: boolean;
  alpha: number;
  beta: number;
  isOptionalC: boolean; // in opset 11, C becomes optional
}

export const gemm: OperatorImplementation<GemmAttributes> = (
  inferenceHandler: WebGLInferenceHandler,
  inputs: Tensor[],
  attributes: GemmAttributes,
): Tensor[] => {
  validateInputs(inputs, attributes);
  const output = inferenceHandler.run(createGemmProgramInfoLoader(inputs, attributes), inputs);
  return [output];
};

const parseGemmAttributes = (node: Graph.Node, isOptionalC: boolean): GemmAttributes => {
  const transA = node.attributes.getInt('transA', 0) !== 0;
  const transB = node.attributes.getInt('transB', 0) !== 0;
  const alpha = node.attributes.getFloat('alpha', 1.0);
  const beta = node.attributes.getFloat('beta', 1.0);
  return createAttributeWithCacheKey({ transA, transB, alpha, beta, isOptionalC });
};

export const parseGemmAttributesV7: OperatorInitialization<GemmAttributes> = (node: Graph.Node): GemmAttributes =>
  parseGemmAttributes(node, false);

export const parseGemmAttributesV11: OperatorInitialization<GemmAttributes> = (node: Graph.Node): GemmAttributes =>
  parseGemmAttributes(node, true);

const createGemmProgramInfoLoader = (inputs: Tensor[], attributes: GemmAttributes): ProgramInfoLoader => {
  const metadata = {
    name: 'Gemm',
    inputNames: inputs.length === 3 ? ['A', 'B', 'C'] : ['A', 'B'],
    inputTypes:
      inputs.length === 3
        ? [TextureType.unpacked, TextureType.unpacked, TextureType.unpacked]
        : [TextureType.unpacked, TextureType.unpacked],
    key: attributes.cacheKey,
  };

  return { ...metadata, get: () => createGemmProgramInfo(metadata, inputs, attributes) };
};

const createGemmProgramInfo = (
  metadata: ProgramMetadata,
  inputs: Tensor[],
  attributes: GemmAttributes,
): ProgramInfo => {
  const aShape = inputs[0].dims.slice();
  const bShape = inputs[1].dims.slice();
  const [M, N] = GemmUtil.getShapeOfGemmResult(
    aShape,
    attributes.transA,
    bShape,
    attributes.transB,
    inputs.length === 3 ? inputs[2].dims : undefined,
  );
  const outputShape = [M, N];
  if (!outputShape) {
    throw new Error("Can't use gemm on the given tensors");
  }
  let sharedDim = aShape[aShape.length - 1];
  let line = '';
  if (attributes.transA) {
    sharedDim = aShape[0];
  }
  if (attributes.transA && attributes.transB) {
    line = 'value += _A_T(a) * _B_T(b);';
  } else if (attributes.transA && !attributes.transB) {
    line = 'value += _A_T(a) * _B(b);';
  } else if (!attributes.transA && attributes.transB) {
    line = 'value += _A(a) * _B_T(b);';
  } else if (!attributes.transA && !attributes.transB) {
    line = 'value += _A(a) * _B(b);';
  }
  const rank = outputShape.length;
  const declareC = inputs.length === 3 ? `int c[${inputs[2].dims.length}];` : '';
  const broadcastC = inputs.length === 3 ? 'bcastIndices_C(indices, c);' : '';
  const calculateC = inputs.length === 3 ? 'value += beta * _C(c);' : '';
  const shaderSource = `
      float process(int indices[${rank}]) {
          int a[${rank}];
          int b[${rank}];
          ${declareC}

          copyVec(indices, a);
          copyVec(indices, b);
          ${broadcastC}

          float value = 0.0;
          for (int k=0; k<${sharedDim}; ++k) {
              a[${rank - 1}] = k;
              b[${rank - 2}] = k;
              ${line}
          }

          value = value * alpha;
          ${calculateC}
          return value;
      }`;
  return {
    ...metadata,
    output: { dims: outputShape, type: inputs[0].type, textureType: TextureType.unpacked },
    variables: [
      { name: 'alpha', type: 'float', data: attributes.alpha },
      { name: 'beta', type: 'float', data: attributes.beta },
    ],
    shaderSource,
  };
};

const validateInputs = (inputs: Tensor[], attributes: GemmAttributes): void => {
  if (!inputs) {
    throw new Error('Input is missing');
  }
  if (attributes.isOptionalC && (inputs.length < 2 || inputs.length > 3)) {
    throw new Error('Invaid input shape.');
  }
  if (!attributes.isOptionalC && inputs.length !== 3) {
    throw new Error('Gemm requires 3 inputs');
  }

  // 'C' can be of dimensionality 1 or 2 only
  if (inputs.length === 3 && inputs[2].dims.length !== 1 && inputs[2].dims.length !== 2) {
    throw new Error('Invalid input shape of C');
  }

  if (
    (inputs[0].type !== 'float32' && inputs[0].type !== 'float64') ||
    (inputs[1].type !== 'float32' && inputs[1].type !== 'float64') ||
    (inputs.length === 3 && inputs[2].type !== 'float32' && inputs[2].type !== 'float64')
  ) {
    throw new Error('Invalid input type.');
  }

  if (inputs[0].type !== inputs[1].type || (inputs.length === 3 && inputs[0].type !== inputs[2].type)) {
    throw new Error('Input types are mismatched');
  }
};
