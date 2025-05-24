"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.paddingSize = exports.fromByteWidth = exports.uwidth = exports.fwidth = exports.iwidth = exports.toByteWidth = void 0;
const bit_width_js_1 = require("./bit-width.js");
function toByteWidth(bitWidth) {
    return 1 << bitWidth;
}
exports.toByteWidth = toByteWidth;
function iwidth(value) {
    if (value >= -128 && value <= 127)
        return bit_width_js_1.BitWidth.WIDTH8;
    if (value >= -32768 && value <= 32767)
        return bit_width_js_1.BitWidth.WIDTH16;
    if (value >= -2147483648 && value <= 2147483647)
        return bit_width_js_1.BitWidth.WIDTH32;
    return bit_width_js_1.BitWidth.WIDTH64;
}
exports.iwidth = iwidth;
function fwidth(value) {
    return value === Math.fround(value) ? bit_width_js_1.BitWidth.WIDTH32 : bit_width_js_1.BitWidth.WIDTH64;
}
exports.fwidth = fwidth;
function uwidth(value) {
    if (value <= 255)
        return bit_width_js_1.BitWidth.WIDTH8;
    if (value <= 65535)
        return bit_width_js_1.BitWidth.WIDTH16;
    if (value <= 4294967295)
        return bit_width_js_1.BitWidth.WIDTH32;
    return bit_width_js_1.BitWidth.WIDTH64;
}
exports.uwidth = uwidth;
function fromByteWidth(value) {
    if (value === 1)
        return bit_width_js_1.BitWidth.WIDTH8;
    if (value === 2)
        return bit_width_js_1.BitWidth.WIDTH16;
    if (value === 4)
        return bit_width_js_1.BitWidth.WIDTH32;
    return bit_width_js_1.BitWidth.WIDTH64;
}
exports.fromByteWidth = fromByteWidth;
function paddingSize(bufSize, scalarSize) {
    return (~bufSize + 1) & (scalarSize - 1);
}
exports.paddingSize = paddingSize;
