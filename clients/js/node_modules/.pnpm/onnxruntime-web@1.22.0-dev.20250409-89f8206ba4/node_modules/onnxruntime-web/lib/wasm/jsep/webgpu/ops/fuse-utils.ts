// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { DataType } from '../../../wasm-common';
import { MAX_CLIP, MIN_CLIP } from '../../util';
import { ProgramUniform } from '../types';

import { UniformsArrayType } from './common';

export interface InternalActivationAttributes {
  readonly activation: string;
  readonly clipMin?: number;
  readonly clipMax?: number;
  readonly alpha?: number;
  readonly beta?: number;
}

export const getActivationSnippet = (
  attributes: InternalActivationAttributes,
  valueType: string,
  baseType = 'f32',
): string => {
  switch (attributes.activation) {
    case 'Relu':
      return `value = max(value, ${valueType}(0.0));`;
    case 'Sigmoid':
      return `value = (${valueType}(1.0) / (${valueType}(1.0) + exp(-value)));`;
    case 'Clip':
      return `value = clamp(value, ${valueType}(${baseType}(uniforms.clip_min)), ${valueType}(${
        baseType
      }(uniforms.clip_max)));`;
    case 'HardSigmoid':
      return `value = max(${valueType}(0.0), min(${valueType}(1.0), ${baseType}(uniforms.alpha) * value + ${
        baseType
      }(uniforms.beta)));`;
    case 'LeakyRelu':
      return `value = select(${baseType}(uniforms.alpha) * value, value, value >= ${valueType}(0.0));`;
    case 'Tanh':
      return `let e2x = exp(-2.0 * abs(value));
              value = sign(value) * (1.0 - e2x) / (1.0 + e2x);
        `;
    case '':
      return '';
    // TODO: adding other activations that can be fused.
    default:
      throw new Error(`Unsupported activation ${attributes.activation}`);
  }
};

export const appendActivationUniformsData = (
  attributes: InternalActivationAttributes,
  programUniform: ProgramUniform[],
) => {
  if (attributes.activation === 'Clip') {
    programUniform.push(
      { type: DataType.float, data: attributes.clipMax! },
      { type: DataType.float, data: attributes.clipMin! },
    );
  } else if (attributes.activation === 'HardSigmoid') {
    programUniform.push(
      { type: DataType.float, data: attributes.alpha! },
      { type: DataType.float, data: attributes.beta! },
    );
  } else if (attributes.activation === 'LeakyRelu') {
    programUniform.push({ type: DataType.float, data: attributes.alpha! });
  }
};

export const appendActivationUniforms = (attributes: InternalActivationAttributes, uniforms: UniformsArrayType) => {
  if (attributes.activation === 'Clip') {
    uniforms.push({ name: 'clip_max', type: 'f32' }, { name: 'clip_min', type: 'f32' });
  } else if (attributes.activation === 'HardSigmoid') {
    uniforms.push({ name: 'alpha', type: 'f32' }, { name: 'beta', type: 'f32' });
  } else if (attributes.activation === 'LeakyRelu') {
    uniforms.push({ name: 'alpha', type: 'f32' });
  }
};

export const parseInternalActivationAttributes = (
  attributes: Record<string, unknown> | undefined,
): InternalActivationAttributes => {
  const activation = (attributes?.activation as string) || '';
  if (activation === 'HardSigmoid') {
    const [alpha, beta] = (attributes?.activation_params as [number, number]) || [0.2, 0.5];
    return { activation, alpha, beta };
  } else if (activation === 'Clip') {
    const [clipMin, clipMax] = (attributes?.activation_params as [number, number]) || [MIN_CLIP, MAX_CLIP];
    return { activation, clipMax, clipMin };
  } else if (activation === 'LeakyRelu') {
    const [alpha] = (attributes?.activation_params as [number]) || [0.01];
    return { activation, alpha };
  }
  return { activation };
};
