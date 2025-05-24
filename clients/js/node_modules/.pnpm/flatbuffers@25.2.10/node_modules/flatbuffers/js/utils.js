"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isLittleEndian = exports.float64 = exports.float32 = exports.int32 = void 0;
exports.int32 = new Int32Array(2);
exports.float32 = new Float32Array(exports.int32.buffer);
exports.float64 = new Float64Array(exports.int32.buffer);
exports.isLittleEndian = new Uint16Array(new Uint8Array([1, 0]).buffer)[0] === 1;
