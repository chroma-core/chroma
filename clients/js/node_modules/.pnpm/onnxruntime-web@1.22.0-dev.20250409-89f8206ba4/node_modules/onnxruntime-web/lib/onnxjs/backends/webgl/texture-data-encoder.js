'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.Uint8DataEncoder = exports.RGBAFloatDataEncoder = exports.RedFloat32DataEncoder = void 0;
const instrument_1 = require('../../instrument');
/**
 * WebGL2 data encoder
 * Uses R32F as the format for texlet
 */
class RedFloat32DataEncoder {
  constructor(gl, channels = 1) {
    if (channels === 1) {
      this.internalFormat = gl.R32F;
      this.format = gl.RED;
      this.textureType = gl.FLOAT;
      this.channelSize = channels;
    } else if (channels === 4) {
      this.internalFormat = gl.RGBA32F;
      this.format = gl.RGBA;
      this.textureType = gl.FLOAT;
      this.channelSize = channels;
    } else {
      throw new Error(`Invalid number of channels: ${channels}`);
    }
  }
  encode(src, textureSize) {
    let result;
    let source;
    if (src.constructor !== Float32Array) {
      instrument_1.Logger.warning('Encoder', 'data was not of type Float32; creating new Float32Array');
      source = new Float32Array(src);
    }
    if (textureSize * this.channelSize > src.length) {
      instrument_1.Logger.warning('Encoder', 'Source data too small. Allocating larger array');
      source = src;
      result = this.allocate(textureSize * this.channelSize);
      source.forEach((v, i) => (result[i] = v));
    } else {
      source = src;
      result = source;
    }
    return result;
  }
  allocate(size) {
    return new Float32Array(size * 4);
  }
  decode(buffer, dataSize) {
    if (this.channelSize === 1) {
      const filteredData = buffer.filter((_value, index) => index % 4 === 0).subarray(0, dataSize);
      return filteredData;
    }
    return buffer.subarray(0, dataSize);
  }
}
exports.RedFloat32DataEncoder = RedFloat32DataEncoder;
/**
 * Data encoder for WebGL 1 with support for floating point texture
 */
class RGBAFloatDataEncoder {
  constructor(gl, channels = 1, textureType) {
    if (channels !== 1 && channels !== 4) {
      throw new Error(`Invalid number of channels: ${channels}`);
    }
    this.internalFormat = gl.RGBA;
    this.format = gl.RGBA;
    this.channelSize = channels;
    this.textureType = textureType || gl.FLOAT;
  }
  encode(src, textureSize) {
    let dest = src;
    if (this.channelSize === 1) {
      instrument_1.Logger.verbose('Encoder', 'Exploding into a larger array');
      dest = this.allocate(textureSize);
      src.forEach((v, i) => (dest[i * 4] = v));
    }
    return dest;
  }
  allocate(size) {
    return new Float32Array(size * 4);
  }
  decode(buffer, dataSize) {
    if (this.channelSize === 1) {
      const filteredData = buffer.filter((_value, index) => index % 4 === 0).subarray(0, dataSize);
      return filteredData;
    }
    return buffer.subarray(0, dataSize);
  }
}
exports.RGBAFloatDataEncoder = RGBAFloatDataEncoder;
class Uint8DataEncoder {
  constructor(gl, channels = 1) {
    this.channelSize = 4;
    if (channels === 1) {
      this.internalFormat = gl.ALPHA;
      this.format = gl.ALPHA; // not tested
      this.textureType = gl.UNSIGNED_BYTE;
      this.channelSize = channels;
    } else if (channels === 4) {
      this.internalFormat = gl.RGBA;
      this.format = gl.RGBA;
      this.textureType = gl.UNSIGNED_BYTE;
      this.channelSize = channels;
    } else {
      throw new Error(`Invalid number of channels: ${channels}`);
    }
  }
  encode(src, _textureSize) {
    return new Uint8Array(src.buffer, src.byteOffset, src.byteLength);
  }
  allocate(size) {
    return new Uint8Array(size * this.channelSize);
  }
  decode(buffer, dataSize) {
    if (buffer instanceof Uint8Array) {
      return buffer.subarray(0, dataSize);
    }
    throw new Error(`Invalid array type: ${buffer.constructor}`);
  }
}
exports.Uint8DataEncoder = Uint8DataEncoder;
//# sourceMappingURL=texture-data-encoder.js.map
