// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Tensor } from '../../../tensor';
import { BroadcastUtil, ShapeUtil } from '../../../util';
import { FunctionType, GlslValueFunction } from '../glsl-definitions';
import { getGlsl } from '../glsl-source';
import { WebGLInferenceHandler } from '../inference-handler';
import { ProgramInfo, ProgramInfoLoader, TextureType } from '../types';

export function glslAdd(): GlslValueFunction {
  const name = 'add_';
  const body = `
  float ${name}(float a, float b) {
    return a + b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 + v2;
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslDiv(): GlslValueFunction {
  const name = 'div_';
  const body = `
  float ${name}(float a, float b) {
    return a / b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 / v2;
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslMul(): GlslValueFunction {
  const name = 'mul_';
  const body = `
  float ${name}(float a, float b) {
    return a * b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 * v2;
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslSub(): GlslValueFunction {
  const name = 'sub_';
  const body = `
  float ${name}(float a, float b) {
    return a - b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 - v2;
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslEqual(): GlslValueFunction {
  const name = 'equal_';
  const body = `
  float ${name}(float a, float b) {
    return float(a == b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return vec4(equal(v1, v2));
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslGreater(): GlslValueFunction {
  const name = 'greater_';
  const body = `
  float ${name}(float a, float b) {
    return float(a > b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return vec4( v1.r > v2.r ,
      v1.g > v2.g,
      v1.b > v2.b,
      v1.a > v2.a );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslLess(): GlslValueFunction {
  const name = 'less_';
  const body = `
  float ${name}(float a, float b) {
    return float(a < b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return vec4( v1.r < v2.r ,
                v1.g < v2.g,
                v1.b < v2.b,
                v1.a < v2.a );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslAnd(): GlslValueFunction {
  const name = 'and_';
  const body = `
  float ${name}(float a, float b) {
    return float( bool(a) && bool(b) );
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    bvec4 b1 = bvec4(v1);
    bvec4 b2 = bvec4(v2);
    return vec4( b1.r && b2.r ,
                b1.g && b2.g,
                b1.b && b2.b,
                b1.a && b2.a );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslOr(): GlslValueFunction {
  const name = 'or_';
  const body = `
  float ${name}(float a, float b) {
    return float( bool(a) || bool(b) );
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    bvec4 b1 = bvec4(v1);
    bvec4 b2 = bvec4(v2);
    return vec4( b1.r || b2.r ,
                b1.g || b2.g,
                b1.b || b2.b,
                b1.a || b2.a );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslXor(): GlslValueFunction {
  const name = 'xor_';
  const body = `
  float ${name}(float a, float b) {
    return float( bool(a) ^^ bool(b) );
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    bvec4 b1 = bvec4(v1);
    bvec4 b2 = bvec4(v2);
    return vec4( b1.r ^^ b2.r ,
                b1.g ^^ b2.g,
                b1.b ^^ b2.b,
                b1.a ^^ b2.a );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}
export function glslPow(): GlslValueFunction {
  return glslBuiltinBinary('pow');
}
export function glslPRelu(): GlslValueFunction {
  const name = 'prelu_';
  const body = `
  float ${name}(float a, float b) {
    return a < 0.0 ? a * b: a;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return vec4(
      v1.r < 0.0 ? v1.r * v2.r: v1.r,
      v1.g < 0.0 ? v1.g * v2.g: v1.g,
      v1.b < 0.0 ? v1.b * v2.b: v1.b,
      v1.a < 0.0 ? v1.a * v2.a: v1.a
      );
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}

function glslBuiltinBinary(fname: string): GlslValueFunction {
  const name = `${fname}_`;
  const body = `
  float ${name}(float a, float b) {
    return ${fname}(a, b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return ${fname}(v1, v2);
  }
  `;
  return { body, name, type: FunctionType.ValueBased };
}

const createBinaryProgramInfoLoader = (
  handler: WebGLInferenceHandler,
  inputs: Tensor[],
  glslFunc: GlslValueFunction,
  outputTensorType: Tensor.DataType = inputs[0].type,
  cacheKey?: string,
): ProgramInfoLoader => {
  const textureType = handler.session.pack ? TextureType.packed : TextureType.unpacked;
  return {
    name: glslFunc.name,
    inputNames: ['A', 'B'],
    inputTypes: [textureType, textureType],
    cacheHint: cacheKey,
    get: () => createBinaryProgramInfo(handler, inputs, glslFunc, outputTensorType),
  };
};

const createBinaryProgramInfo = (
  handler: WebGLInferenceHandler,
  inputs: Tensor[],
  glslFunc: GlslValueFunction,
  outputTensorType: Tensor.DataType = inputs[0].type,
): ProgramInfo => {
  const textureType = handler.session.pack ? TextureType.packed : TextureType.unpacked;
  const isBroadcast = !ShapeUtil.areEqual(inputs[0].dims, inputs[1].dims);
  let outputShape = inputs[0].dims;

  const usePackedTexture = handler.session.pack;

  if (isBroadcast) {
    const calculatedShape = BroadcastUtil.calcShape(inputs[0].dims, inputs[1].dims, false);
    if (!calculatedShape) {
      throw new Error("Can't perform binary op on the given tensors");
    }
    outputShape = calculatedShape;
    const outputRank = outputShape.length;
    const aRank = inputs[0].dims.length !== 0 ? inputs[0].dims.length : 1;
    const bRank = inputs[1].dims.length !== 0 ? inputs[1].dims.length : 1;
    const aBcast = inputs[0].dims.length !== 0 ? 'bcastIndices_A(indices, aindices);' : 'aindices[0] = 0;';
    const bBcast = inputs[1].dims.length !== 0 ? 'bcastIndices_B(indices, bindices);' : 'bindices[0] = 0;';

    const glsl = getGlsl(handler.session.backend.glContext.version);
    const shaderSource = usePackedTexture
      ? `
      ${glslFunc.body}
      void main() {
        vec4 a = getAAtOutCoords();
        vec4 b = getBAtOutCoords();
        vec4 result = ${glslFunc.name}(a, b);
        ${glsl.output} = result;
      }`
      : `
      ${glslFunc.body}
      float process(int indices[${outputRank}]) {
        int aindices[${aRank}];
        int bindices[${bRank}];
        ${aBcast}
        ${bBcast}
        return ${glslFunc.name}(_A(aindices), _B(bindices));
      }`;

    return {
      name: glslFunc.name,
      inputNames: ['A', 'B'],
      inputTypes: [textureType, textureType],
      output: { dims: outputShape, type: outputTensorType, textureType },
      shaderSource,
      hasMain: usePackedTexture,
    };
  }
  const glsl = getGlsl(handler.session.backend.glContext.version);
  const shaderSource = `
    ${glslFunc.body}
    void main() {
      vec4 v1 = ${glsl.texture2D}(A, TexCoords);
      vec4 v2 = ${glsl.texture2D}(B, TexCoords);
      vec4 result = ${glslFunc.name}(v1, v2);
      ${glsl.output} = result;
    }
    `;

  return {
    name: glslFunc.name,
    inputNames: ['A', 'B'],
    inputTypes: [textureType, textureType],
    output: { dims: inputs[0].dims, type: outputTensorType, textureType },
    shaderSource,
    hasMain: true,
  };
};

export const add = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslAdd()), inputs),
];

export const and = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslAnd(), 'bool'), inputs),
];

export const div = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslDiv()), inputs),
];

export const equal = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslEqual(), 'bool'), inputs),
];

export const greater = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslGreater(), 'bool'), inputs),
];

export const less = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslLess(), 'bool'), inputs),
];

export const mul = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslMul()), inputs),
];

export const or = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslOr(), 'bool'), inputs),
];

export const pow = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslPow()), inputs),
];

export const pRelu = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslPRelu()), inputs),
];

export const sub = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslSub()), inputs),
];

export const xor = (handler: WebGLInferenceHandler, inputs: Tensor[]): Tensor[] => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslXor(), 'bool'), inputs),
];
