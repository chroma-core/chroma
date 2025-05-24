'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.EncodingGlslLib = void 0;
const glsl_definitions_1 = require('./glsl-definitions');
/**
 * This GLSL library handles routines converting
 * float32 to/from Unsigned byte or float 16
 */
class EncodingGlslLib extends glsl_definitions_1.GlslLib {
  constructor(context) {
    super(context);
  }
  getFunctions() {
    return { ...this.encodeFloat32(), ...this.decodeFloat32() };
  }
  getCustomTypes() {
    return {};
  }
  encodeFloat32() {
    return {
      encode: new glsl_definitions_1.GlslLibRoutine(`highp vec4 encode(highp float f) {
        return vec4(f, 0.0, 0.0, 0.0);
      }
        `),
    };
  }
  decodeFloat32() {
    return {
      decode: new glsl_definitions_1.GlslLibRoutine(`highp float decode(highp vec4 rgba) {
        return rgba.r;
      }
        `),
    };
  }
  /**
   * returns the routine to encode encode a 32bit float to a vec4 (of unsigned bytes)
   * @credit: https://stackoverflow.com/questions/7059962/how-do-i-convert-a-vec4-rgba-value-to-a-float
   */
  encodeUint8() {
    const endianness = EncodingGlslLib.isLittleEndian() ? 'rgba.rgba=rgba.abgr;' : '';
    return {
      encode: new glsl_definitions_1.GlslLibRoutine(`
      highp vec4 encode(highp float f) {
        highp float F = abs(f);
        highp float Sign = step(0.0,-f);
        highp float Exponent = floor(log2(F));
        highp float Mantissa = (exp2(- Exponent) * F);
        Exponent = floor(log2(F) + 127.0) + floor(log2(Mantissa));
        highp vec4 rgba;
        rgba[0] = 128.0 * Sign  + floor(Exponent*exp2(-1.0));
        rgba[1] = 128.0 * mod(Exponent,2.0) + mod(floor(Mantissa*128.0),128.0);
        rgba[2] = floor(mod(floor(Mantissa*exp2(23.0 -8.0)),exp2(8.0)));
        rgba[3] = floor(exp2(23.0)*mod(Mantissa,exp2(-15.0)));
        ${endianness}
        rgba = rgba / 255.0; // values need to be normalized to [0,1]
        return rgba;
    }
        `),
    };
  }
  /**
   * returns the routine to encode a vec4 of unsigned bytes to float32
   * @credit: https://stackoverflow.com/questions/7059962/how-do-i-convert-a-vec4-rgba-value-to-a-float
   */
  decodeUint8() {
    const endianness = EncodingGlslLib.isLittleEndian() ? 'rgba.rgba=rgba.abgr;' : '';
    return {
      decode: new glsl_definitions_1.GlslLibRoutine(`
        highp float decode(highp vec4 rgba) {
          rgba = rgba * 255.0; // values need to be de-normalized from [0,1] to [0,255]
          ${endianness}
          highp float Sign = 1.0 - step(128.0,rgba[0])*2.0;
          highp float Exponent = 2.0 * mod(rgba[0],128.0) + step(128.0,rgba[1]) - 127.0;
          highp float Mantissa = mod(rgba[1],128.0)*65536.0 + rgba[2]*256.0 +rgba[3] + float(0x800000);
          highp float Result =  Sign * exp2(Exponent) * (Mantissa * exp2(-23.0 ));
          return Result;
      }
        `),
    };
  }
  /**
   * Determines if the machine is little endian or not
   * @credit: https://gist.github.com/TooTallNate/4750953
   */
  static isLittleEndian() {
    const b = new ArrayBuffer(4);
    const a = new Uint32Array(b);
    const c = new Uint8Array(b);
    a[0] = 0xdeadbeef;
    if (c[0] === 0xef) {
      return true;
    }
    if (c[0] === 0xde) {
      return false;
    }
    throw new Error('unknown endianness');
  }
}
exports.EncodingGlslLib = EncodingGlslLib;
//# sourceMappingURL=glsl-encoding-lib.js.map
