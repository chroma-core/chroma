'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.xor =
  exports.sub =
  exports.pRelu =
  exports.pow =
  exports.or =
  exports.mul =
  exports.less =
  exports.greater =
  exports.equal =
  exports.div =
  exports.and =
  exports.add =
  exports.glslPRelu =
  exports.glslPow =
  exports.glslXor =
  exports.glslOr =
  exports.glslAnd =
  exports.glslLess =
  exports.glslGreater =
  exports.glslEqual =
  exports.glslSub =
  exports.glslMul =
  exports.glslDiv =
  exports.glslAdd =
    void 0;
const util_1 = require('../../../util');
const glsl_definitions_1 = require('../glsl-definitions');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
function glslAdd() {
  const name = 'add_';
  const body = `
  float ${name}(float a, float b) {
    return a + b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 + v2;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslAdd = glslAdd;
function glslDiv() {
  const name = 'div_';
  const body = `
  float ${name}(float a, float b) {
    return a / b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 / v2;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslDiv = glslDiv;
function glslMul() {
  const name = 'mul_';
  const body = `
  float ${name}(float a, float b) {
    return a * b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 * v2;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslMul = glslMul;
function glslSub() {
  const name = 'sub_';
  const body = `
  float ${name}(float a, float b) {
    return a - b;
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return v1 - v2;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslSub = glslSub;
function glslEqual() {
  const name = 'equal_';
  const body = `
  float ${name}(float a, float b) {
    return float(a == b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return vec4(equal(v1, v2));
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslEqual = glslEqual;
function glslGreater() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslGreater = glslGreater;
function glslLess() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslLess = glslLess;
function glslAnd() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslAnd = glslAnd;
function glslOr() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslOr = glslOr;
function glslXor() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslXor = glslXor;
function glslPow() {
  return glslBuiltinBinary('pow');
}
exports.glslPow = glslPow;
function glslPRelu() {
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
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslPRelu = glslPRelu;
function glslBuiltinBinary(fname) {
  const name = `${fname}_`;
  const body = `
  float ${name}(float a, float b) {
    return ${fname}(a, b);
  }
  vec4 ${name}(vec4 v1, vec4 v2) {
    return ${fname}(v1, v2);
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
const createBinaryProgramInfoLoader = (handler, inputs, glslFunc, outputTensorType = inputs[0].type, cacheKey) => {
  const textureType = handler.session.pack ? types_1.TextureType.packed : types_1.TextureType.unpacked;
  return {
    name: glslFunc.name,
    inputNames: ['A', 'B'],
    inputTypes: [textureType, textureType],
    cacheHint: cacheKey,
    get: () => createBinaryProgramInfo(handler, inputs, glslFunc, outputTensorType),
  };
};
const createBinaryProgramInfo = (handler, inputs, glslFunc, outputTensorType = inputs[0].type) => {
  const textureType = handler.session.pack ? types_1.TextureType.packed : types_1.TextureType.unpacked;
  const isBroadcast = !util_1.ShapeUtil.areEqual(inputs[0].dims, inputs[1].dims);
  let outputShape = inputs[0].dims;
  const usePackedTexture = handler.session.pack;
  if (isBroadcast) {
    const calculatedShape = util_1.BroadcastUtil.calcShape(inputs[0].dims, inputs[1].dims, false);
    if (!calculatedShape) {
      throw new Error("Can't perform binary op on the given tensors");
    }
    outputShape = calculatedShape;
    const outputRank = outputShape.length;
    const aRank = inputs[0].dims.length !== 0 ? inputs[0].dims.length : 1;
    const bRank = inputs[1].dims.length !== 0 ? inputs[1].dims.length : 1;
    const aBcast = inputs[0].dims.length !== 0 ? 'bcastIndices_A(indices, aindices);' : 'aindices[0] = 0;';
    const bBcast = inputs[1].dims.length !== 0 ? 'bcastIndices_B(indices, bindices);' : 'bindices[0] = 0;';
    const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
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
  const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
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
const add = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslAdd()), inputs)];
exports.add = add;
const and = (handler, inputs) => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslAnd(), 'bool'), inputs),
];
exports.and = and;
const div = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslDiv()), inputs)];
exports.div = div;
const equal = (handler, inputs) => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslEqual(), 'bool'), inputs),
];
exports.equal = equal;
const greater = (handler, inputs) => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslGreater(), 'bool'), inputs),
];
exports.greater = greater;
const less = (handler, inputs) => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslLess(), 'bool'), inputs),
];
exports.less = less;
const mul = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslMul()), inputs)];
exports.mul = mul;
const or = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslOr(), 'bool'), inputs)];
exports.or = or;
const pow = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslPow()), inputs)];
exports.pow = pow;
const pRelu = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslPRelu()), inputs)];
exports.pRelu = pRelu;
const sub = (handler, inputs) => [handler.run(createBinaryProgramInfoLoader(handler, inputs, glslSub()), inputs)];
exports.sub = sub;
const xor = (handler, inputs) => [
  handler.run(createBinaryProgramInfoLoader(handler, inputs, glslXor(), 'bool'), inputs),
];
exports.xor = xor;
//# sourceMappingURL=binary-op.js.map
