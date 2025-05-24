// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { Logger } from '../../instrument';

export declare namespace Encoder {
  export interface DataTypeMap {
    float: Float32Array;
    byte: Uint8Array;
    int: Uint32Array;
  }
  export type DataType = keyof DataTypeMap;
  type DataArrayType = DataTypeMap[DataType];
}

/* eslint-disable @typescript-eslint/naming-convention */
export const enum EncoderUsage {
  Default = 0,
  UploadOnly,
  Download4BytesAsFloat32,
}
/* eslint-enable @typescript-eslint/naming-convention */

/**
 * Abstraction for mapping data types to texture texlets
 * Encoding means how a Float32 is mapped to 1 or 4 channels for each texlet
 * Decoding means how a texlet's channels are mapped to a resulting Float32
 */
export interface DataEncoder {
  internalFormat: number;
  format: number;
  textureType: number;
  channelSize: number;
  encode(src: Encoder.DataArrayType, textureSize: number): Encoder.DataArrayType;
  allocate(size: number): Encoder.DataArrayType;
  decode(buffer: Encoder.DataArrayType, dataSize: number): Encoder.DataArrayType;
}
/**
 * WebGL2 data encoder
 * Uses R32F as the format for texlet
 */
export class RedFloat32DataEncoder implements DataEncoder {
  internalFormat: number;
  format: number;
  textureType: number;
  channelSize: number;
  constructor(gl: WebGL2RenderingContext, channels = 1) {
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
  encode(src: Encoder.DataArrayType, textureSize: number): Encoder.DataArrayType {
    let result: Float32Array;
    let source: Float32Array;
    if (src.constructor !== Float32Array) {
      Logger.warning('Encoder', 'data was not of type Float32; creating new Float32Array');
      source = new Float32Array(src);
    }
    if (textureSize * this.channelSize > src.length) {
      Logger.warning('Encoder', 'Source data too small. Allocating larger array');
      source = src as Float32Array;
      result = this.allocate(textureSize * this.channelSize) as Float32Array;
      source.forEach((v, i) => (result[i] = v));
    } else {
      source = src as Float32Array;
      result = source;
    }
    return result;
  }
  allocate(size: number): Encoder.DataArrayType {
    return new Float32Array(size * 4);
  }
  decode(buffer: Encoder.DataArrayType, dataSize: number): Float32Array {
    if (this.channelSize === 1) {
      const filteredData = (buffer as Float32Array).filter((_value, index) => index % 4 === 0).subarray(0, dataSize);
      return filteredData;
    }
    return buffer.subarray(0, dataSize) as Float32Array;
  }
}
/**
 * Data encoder for WebGL 1 with support for floating point texture
 */
export class RGBAFloatDataEncoder implements DataEncoder {
  internalFormat: number;
  format: number;
  textureType: number;
  channelSize: number;
  constructor(gl: WebGLRenderingContext, channels = 1, textureType?: number) {
    if (channels !== 1 && channels !== 4) {
      throw new Error(`Invalid number of channels: ${channels}`);
    }
    this.internalFormat = gl.RGBA;
    this.format = gl.RGBA;
    this.channelSize = channels;
    this.textureType = textureType || gl.FLOAT;
  }
  encode(src: Float32Array, textureSize: number): Encoder.DataArrayType {
    let dest = src;
    if (this.channelSize === 1) {
      Logger.verbose('Encoder', 'Exploding into a larger array');
      dest = this.allocate(textureSize) as Float32Array;
      src.forEach((v, i) => (dest[i * 4] = v));
    }
    return dest;
  }
  allocate(size: number): Encoder.DataArrayType {
    return new Float32Array(size * 4);
  }
  decode(buffer: Encoder.DataArrayType, dataSize: number): Float32Array {
    if (this.channelSize === 1) {
      const filteredData = (buffer as Float32Array).filter((_value, index) => index % 4 === 0).subarray(0, dataSize);
      return filteredData;
    }
    return buffer.subarray(0, dataSize) as Float32Array;
  }
}

export class Uint8DataEncoder implements DataEncoder {
  internalFormat: number;
  format: number;
  textureType: number;
  channelSize = 4;
  constructor(gl: WebGLRenderingContext, channels = 1) {
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
  encode(src: Uint8Array, _textureSize: number): Encoder.DataArrayType {
    return new Uint8Array(src.buffer, src.byteOffset, src.byteLength);
  }
  allocate(size: number): Encoder.DataArrayType {
    return new Uint8Array(size * this.channelSize);
  }
  decode(buffer: Encoder.DataArrayType, dataSize: number): Uint8Array {
    if (buffer instanceof Uint8Array) {
      return buffer.subarray(0, dataSize);
    }
    throw new Error(`Invalid array type: ${buffer.constructor}`);
  }
}
