var brotli = require('./build/encode');

/**
 * Compresses the given buffer
 * The second parameter is optional and specifies whether the buffer is
 * text or binary data (the default is binary).
 * Returns null on error
 */
module.exports = function(buffer, opts) {
  // default to binary data
  var quality = 11;
  var mode = 0;
  var lgwin = 22;
  
  if (typeof opts === 'boolean') {
    mode = opts ? 0 : 1;
  } else if (typeof opts === 'object') {
    quality = opts.quality || 11;
    mode = opts.mode || 0;
    lgwin = opts.lgwin || 22;
  }
  
  // allocate input buffer and copy data to it
  var buf = brotli._malloc(buffer.length);
  brotli.HEAPU8.set(buffer, buf);
  
  // allocate output buffer (same size + some padding to be sure it fits), and encode
  var outBuf = brotli._malloc(buffer.length + 1024);
  var encodedSize = brotli._encode(quality, lgwin, mode, buffer.length, buf, buffer.length, outBuf);
  
  var outBuffer = null;
  if (encodedSize !== -1) {
    // allocate and copy data to an output buffer
    outBuffer = new Uint8Array(encodedSize);
    outBuffer.set(brotli.HEAPU8.subarray(outBuf, outBuf + encodedSize));
  }
  
  // free malloc'd buffers
  brotli._free(buf);
  brotli._free(outBuf);
    
  return outBuffer;
};
