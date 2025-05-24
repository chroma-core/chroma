// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Attribute } from '../../../attribute';
import { MAX_CLIP, MIN_CLIP } from '../../../util';
import { GlslValueFunction } from '../glsl-definitions';

import { glslClip, glslRelu, glslSigmoid } from './unary-op';

export interface InternalActivationAttributes {
  readonly activation: string;
  readonly clipMin?: number;
  readonly clipMax?: number;
  readonly activationCacheKey: string;
}

export function getActivationSnippet(attributes: InternalActivationAttributes) {
  let func: GlslValueFunction;
  switch (attributes.activation) {
    case 'Relu':
      func = glslRelu();
      break;
    case 'Sigmoid':
      func = glslSigmoid();
      break;
    case 'Clip':
      func = glslClip(attributes.clipMin!, attributes.clipMax!);
      break;
    // TODO: adding other activations that can be fused.
    default:
      return { activationFunction: '', applyActivation: '' };
  }

  const activationName = func.name;
  const activationFunction = func.body;
  const applyActivation = `value = ${activationName}_(value);`;
  return { activationFunction, applyActivation };
}

export const parseInternalActivationAttributes = (attributes: Attribute): InternalActivationAttributes => {
  const activation = attributes.getString('activation', '');

  if (activation === 'Clip') {
    const [clipMin, clipMax] = attributes.getFloats('activation_params', [MIN_CLIP, MAX_CLIP]);
    return { activation, clipMax, clipMin, activationCacheKey: `${activation}:${clipMin},${clipMax}` };
  }
  return { activation, activationCacheKey: activation };
};
