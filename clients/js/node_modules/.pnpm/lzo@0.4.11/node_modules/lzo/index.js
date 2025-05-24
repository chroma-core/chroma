'use strict';

const binding = require('bindings')('node_lzo');

// LZO error codes from lzoconf.h
const errCodes = { 
  '-1': 'LZO_E_ERROR',
  '-2': 'LZO_E_OUT_OF_MEMORY',
  '-3': 'LZO_E_NOT_COMPRESSIBLE',
  '-4': 'LZO_E_INPUT_OVERRUN',
  '-5': 'LZO_E_OUTPUT_OVERRUN',
  '-6': 'LZO_E_LOOKBEHIND_OVERRUN',
  '-7': 'LZO_E_EOF_NOT_FOUND',
  '-8': 'LZO_E_INPUT_NOT_CONSUMED',
  '-9': 'LZO_E_NOT_YET_IMPLEMENTED',
  '-10': 'LZO_E_INVALID_ARGUMENT',
  '-11': 'LZO_E_INVALID_ALIGNMENT',
  '-12': 'LZO_E_OUTPUT_NOT_CONSUMED',
  '-99': 'LZO_E_INTERNAL_ERROR'
};

module.exports = {

  /**
   * Compress data with the lzo compression algorithm
   *
   * @param {Buffer} input - If the parameter is not a buffer, the function will try to convert via `Buffer.from`
   *
   * @return {Buffer} The compressed data
   */
  'compress': (input) => {
    if(!Buffer.isBuffer(input))
      input = Buffer.from(input);

    let output = Buffer.alloc(input.length + (input.length / 16) + 64 + 3),
        result = binding.compress(input, output);

    if(result.err !== 0)
      throw new Error('Compression failed with code: ' + errCodes[result.err]);
    else
      return output.slice(0, result.len);
  },

  /**
   * Decompress lzo-compressed data
   *
   * @param {Buffer} input - If the parameter is not a buffer, the function will try to convert via `Buffer.from`
   *
   * @return {Buffer} The decompressed data
   */
  'decompress': (input, length) => {
    if(!Buffer.isBuffer(input))
      input = Buffer.from(input);

    let output = Buffer.alloc(length || (input.length * 3)),
      result = binding.decompress(input, output);

    if(result.err !== 0)
      throw new Error('Decompression failed with code: ' + errCodes[result.err]);
    else
      return output.slice(0, result.len);
  },

  'version': binding.version,
  'versionDate': binding.versionDate,
  'errors': errCodes
};