"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toUTF8Array = exports.fromUTF8Array = void 0;
function fromUTF8Array(data) {
    const decoder = new TextDecoder();
    return decoder.decode(data);
}
exports.fromUTF8Array = fromUTF8Array;
function toUTF8Array(str) {
    const encoder = new TextEncoder();
    return encoder.encode(str);
}
exports.toUTF8Array = toUTF8Array;
