'use strict';

/**
 * Normalizes our expected stringified form of a function across versions of node
 * @param {Function} fn The function to stringify
 */
function normalizedFunctionString(fn) {
  return fn.toString().replace(/function *\(/, 'function (');
}

function newBuffer(item, encoding) {
  return new Buffer(item, encoding);
}

function allocBuffer() {
  return Buffer.alloc.apply(Buffer, arguments);
}

function toBuffer() {
  return Buffer.from.apply(Buffer, arguments);
}

module.exports = {
  normalizedFunctionString: normalizedFunctionString,
  allocBuffer: typeof Buffer.alloc === 'function' ? allocBuffer : newBuffer,
  toBuffer: typeof Buffer.from === 'function' ? toBuffer : newBuffer
};

