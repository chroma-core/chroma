'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.tanh =
  exports.tan =
  exports.sqrt =
  exports.sin =
  exports.sigmoid =
  exports.relu =
  exports.not =
  exports.neg =
  exports.log =
  exports.parseLeakyReluAttributes =
  exports.leakyRelu =
  exports.identity =
  exports.floor =
  exports.exp =
  exports.parseEluAttributes =
  exports.elu =
  exports.cos =
  exports.ceil =
  exports.clipV11 =
  exports.parseClipAttributes =
  exports.clip =
  exports.atan =
  exports.asin =
  exports.acos =
  exports.abs =
  exports.glslTanh =
  exports.glslTan =
  exports.glslSqrt =
  exports.glslSigmoid =
  exports.glslRelu =
  exports.glslSin =
  exports.glslNot =
  exports.glslNeg =
  exports.glslLog =
  exports.glslLeakyRelu =
  exports.glslIdentity =
  exports.glslClip =
  exports.glslFloor =
  exports.glslExp =
  exports.glslElu =
  exports.glslCos =
  exports.glslCeil =
  exports.glslAtan =
  exports.glslAsin =
  exports.glslAcos =
  exports.glslAbs =
    void 0;
const attribute_with_cache_key_1 = require('../../../attribute-with-cache-key');
const util_1 = require('../../../util');
const glsl_definitions_1 = require('../glsl-definitions');
const glsl_source_1 = require('../glsl-source');
const types_1 = require('../types');
function glslAbs() {
  return glslBuiltinUnary('abs');
}
exports.glslAbs = glslAbs;
function glslAcos() {
  return glslBuiltinUnary('acos');
}
exports.glslAcos = glslAcos;
function glslAsin() {
  return glslBuiltinUnary('asin');
}
exports.glslAsin = glslAsin;
function glslAtan() {
  return glslBuiltinUnary('atan');
}
exports.glslAtan = glslAtan;
function glslCeil() {
  return glslBuiltinUnary('ceil');
}
exports.glslCeil = glslCeil;
function glslCos() {
  return glslBuiltinUnary('cos');
}
exports.glslCos = glslCos;
function glslElu(alpha) {
  const name = 'elu';
  const body = `
  const float alpha = float(${alpha});

  float ${name}_(float a) {
    return a >= 0.0 ? a: (exp(a) - 1.0) * alpha;
  }
  vec4 ${name}_(vec4 v) {
    return vec4(${name}_(v.x), ${name}_(v.y), ${name}_(v.z), ${name}_(v.w));
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslElu = glslElu;
function glslExp() {
  return glslBuiltinUnary('exp');
}
exports.glslExp = glslExp;
function glslFloor() {
  return glslBuiltinUnary('floor');
}
exports.glslFloor = glslFloor;
function glslClip(min, max) {
  const name = 'clip';
  const body = `
  const float min = float(${min});
  const float max = float(${max});

  float ${name}_(float a) {
    return clamp(a, min, max);
  }
  vec4 ${name}_(vec4 v) {
    return clamp(v, min, max);
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslClip = glslClip;
function glslIdentity() {
  const name = 'indentity';
  const body = `
  float ${name}_(float a) {
    return a;
  }
  vec4 ${name}_(vec4 v) {
    return v;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslIdentity = glslIdentity;
function glslLeakyRelu(alpha) {
  const name = 'leakyRelu';
  const body = `
  const float alpha = float(${alpha});

  float ${name}_(float a) {
    return a < 0.0 ? a * alpha : a;
  }
  vec4 ${name}_(vec4 v) {
    return vec4(${name}_(v.x), ${name}_(v.y), ${name}_(v.z), ${name}_(v.w));
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslLeakyRelu = glslLeakyRelu;
function glslLog() {
  return glslBuiltinUnary('log');
}
exports.glslLog = glslLog;
function glslNeg() {
  const name = 'neg';
  const body = `
  float ${name}_(float a) {
    return -a;
  }
  vec4 ${name}_(vec4 v) {
    return -v;
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslNeg = glslNeg;
function glslNot() {
  const name = 'not';
  const body = `
  float ${name}_(float a) {
    return float( ! bool(a) );
  }
  bool ${name}_(bool a) {
    return !a;
  }
  vec4 ${name}_(vec4 v) {
    return vec4(!bool(v.x), !bool(v.y), !bool(v.z), !bool(v.w));
  }
  bvec4 ${name}_(bvec4 v) {
    return bvec4(!v.x, !v.y, !v.z, !v.w);
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslNot = glslNot;
function glslSin() {
  return glslBuiltinUnary('sin');
}
exports.glslSin = glslSin;
function glslRelu() {
  const name = 'relu';
  const body = `
  float ${name}_(float a) {
    return max( a, 0.0 );
  }
  vec4 ${name}_(vec4 v) {
    return max( v, 0.0 );
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslRelu = glslRelu;
function glslSigmoid() {
  const name = 'sigmoid';
  const body = `
  float ${name}_(float a) {
    return 1.0 / (1.0 + exp(-a));
  }
  vec4 ${name}_(vec4 v) {
    return 1.0 / (1.0 + exp(-v));
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslSigmoid = glslSigmoid;
function glslSqrt() {
  return glslBuiltinUnary('sqrt');
}
exports.glslSqrt = glslSqrt;
function glslTan() {
  return glslBuiltinUnary('tan');
}
exports.glslTan = glslTan;
function glslTanh() {
  const name = 'tanh';
  const body = `
  float ${name}_(float a) {
    a = clamp(a, -10., 10.);
    a = exp(2.*a);
    return (a - 1.) / (a + 1.);
  }
  vec4 ${name}_(vec4 v) {
    v = clamp(v, -10., 10.);
    v = exp(2.*v);
    return (v - 1.) / (v + 1.);
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
exports.glslTanh = glslTanh;
function glslBuiltinUnary(name) {
  const body = `
  float ${name}_(float a) {
    return ${name}(a);
  }
  vec4 ${name}_(vec4 v) {
    return ${name}(v);
  }
  `;
  return { body, name, type: glsl_definitions_1.FunctionType.ValueBased };
}
/////
/////
/////
const createElementwiseProgramInfo = (handler, metadata, input, glslFunc) => {
  const textureType = handler.session.pack ? types_1.TextureType.packed : types_1.TextureType.unpacked;
  const glsl = (0, glsl_source_1.getGlsl)(handler.session.backend.glContext.version);
  return {
    ...metadata,
    output: { dims: input.dims, type: input.type, textureType },
    shaderSource: `
     ${glslFunc.body}
     void main() {
       vec4 v = ${glsl.texture2D}(A, TexCoords);
       v = ${glslFunc.name}_(v);
       ${glsl.output} = v;
     }
     `,
    hasMain: true,
  };
};
const createElementwiseProgramInfoLoader = (handler, input, glslFunc, cacheKey) => {
  const textureType = handler.session.pack ? types_1.TextureType.packed : types_1.TextureType.unpacked;
  const metadata = { name: glslFunc.name, inputTypes: [textureType], inputNames: ['A'], cacheHint: cacheKey };
  return { ...metadata, get: () => createElementwiseProgramInfo(handler, metadata, input, glslFunc) };
};
const abs = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslAbs()), inputs),
];
exports.abs = abs;
const acos = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslAcos()), inputs),
];
exports.acos = acos;
const asin = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslAsin()), inputs),
];
exports.asin = asin;
const atan = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslAtan()), inputs),
];
exports.atan = atan;
const clip = (handler, inputs, attributes) => [
  handler.run(
    createElementwiseProgramInfoLoader(
      handler,
      inputs[0],
      glslClip(attributes.min, attributes.max),
      attributes.cacheKey,
    ),
    inputs,
  ),
];
exports.clip = clip;
const parseClipAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({
    min: node.attributes.getFloat('min', util_1.MIN_CLIP),
    max: node.attributes.getFloat('max', util_1.MAX_CLIP),
  });
exports.parseClipAttributes = parseClipAttributes;
const clipV11 = (handler, inputs) => {
  const attributes = generateClipAttributesFromInputs(handler, inputs);
  return (0, exports.clip)(handler, [inputs[0]], attributes);
};
exports.clipV11 = clipV11;
const generateClipAttributesFromInputs = (handler, inputs) => {
  if (
    inputs.length >= 3 &&
    (!handler.session.isInitializer(inputs[1].dataId) || !handler.session.isInitializer(inputs[2].dataId))
  ) {
    throw new Error('dynamic clip attributes are not allowed');
  }
  const min = inputs.length >= 3 ? inputs[1].numberData[0] : util_1.MIN_CLIP;
  const max = inputs.length >= 3 ? inputs[2].numberData[0] : util_1.MAX_CLIP;
  return (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ min, max });
};
const ceil = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslCeil()), inputs),
];
exports.ceil = ceil;
const cos = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslCos()), inputs),
];
exports.cos = cos;
const elu = (handler, inputs, attributes) => [
  handler.run(
    createElementwiseProgramInfoLoader(handler, inputs[0], glslElu(attributes.alpha), attributes.cacheKey),
    inputs,
  ),
];
exports.elu = elu;
const parseEluAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ alpha: node.attributes.getFloat('alpha', 1.0) });
exports.parseEluAttributes = parseEluAttributes;
const exp = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslExp()), inputs),
];
exports.exp = exp;
const floor = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslFloor()), inputs),
];
exports.floor = floor;
const identity = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslIdentity()), inputs),
];
exports.identity = identity;
const leakyRelu = (handler, inputs, attributes) => [
  handler.run(
    createElementwiseProgramInfoLoader(handler, inputs[0], glslLeakyRelu(attributes.alpha), attributes.cacheKey),
    inputs,
  ),
];
exports.leakyRelu = leakyRelu;
const parseLeakyReluAttributes = (node) =>
  (0, attribute_with_cache_key_1.createAttributeWithCacheKey)({ alpha: node.attributes.getFloat('alpha', 0.01) });
exports.parseLeakyReluAttributes = parseLeakyReluAttributes;
const log = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslLog()), inputs),
];
exports.log = log;
const neg = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslNeg()), inputs),
];
exports.neg = neg;
const not = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslNot()), inputs),
];
exports.not = not;
const relu = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslRelu()), inputs),
];
exports.relu = relu;
const sigmoid = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslSigmoid()), inputs),
];
exports.sigmoid = sigmoid;
const sin = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslSin()), inputs),
];
exports.sin = sin;
const sqrt = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslSqrt()), inputs),
];
exports.sqrt = sqrt;
const tan = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslTan()), inputs),
];
exports.tan = tan;
const tanh = (handler, inputs) => [
  handler.run(createElementwiseProgramInfoLoader(handler, inputs[0], glslTanh()), inputs),
];
exports.tanh = tanh;
//# sourceMappingURL=unary-op.js.map
