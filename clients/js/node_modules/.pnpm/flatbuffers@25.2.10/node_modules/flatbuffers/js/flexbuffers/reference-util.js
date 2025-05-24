"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.keyForIndex = exports.diffKeys = exports.keyIndex = exports.indirect = exports.readFloat = exports.readUInt = exports.readInt = exports.validateOffset = void 0;
const bit_width_js_1 = require("./bit-width.js");
const bit_width_util_js_1 = require("./bit-width-util.js");
const flexbuffers_util_js_1 = require("./flexbuffers-util.js");
function validateOffset(dataView, offset, width) {
    if (dataView.byteLength <= offset + width || (offset & ((0, bit_width_util_js_1.toByteWidth)(width) - 1)) !== 0) {
        throw "Bad offset: " + offset + ", width: " + width;
    }
}
exports.validateOffset = validateOffset;
function readInt(dataView, offset, width) {
    if (width < 2) {
        if (width < 1) {
            return dataView.getInt8(offset);
        }
        else {
            return dataView.getInt16(offset, true);
        }
    }
    else {
        if (width < 3) {
            return dataView.getInt32(offset, true);
        }
        else {
            if (dataView.setBigInt64 === undefined) {
                return BigInt(dataView.getUint32(offset, true)) + (BigInt(dataView.getUint32(offset + 4, true)) << BigInt(32));
            }
            return dataView.getBigInt64(offset, true);
        }
    }
}
exports.readInt = readInt;
function readUInt(dataView, offset, width) {
    if (width < 2) {
        if (width < 1) {
            return dataView.getUint8(offset);
        }
        else {
            return dataView.getUint16(offset, true);
        }
    }
    else {
        if (width < 3) {
            return dataView.getUint32(offset, true);
        }
        else {
            if (dataView.getBigUint64 === undefined) {
                return BigInt(dataView.getUint32(offset, true)) + (BigInt(dataView.getUint32(offset + 4, true)) << BigInt(32));
            }
            return dataView.getBigUint64(offset, true);
        }
    }
}
exports.readUInt = readUInt;
function readFloat(dataView, offset, width) {
    if (width < bit_width_js_1.BitWidth.WIDTH32) {
        throw "Bad width: " + width;
    }
    if (width === bit_width_js_1.BitWidth.WIDTH32) {
        return dataView.getFloat32(offset, true);
    }
    return dataView.getFloat64(offset, true);
}
exports.readFloat = readFloat;
function indirect(dataView, offset, width) {
    const step = readUInt(dataView, offset, width);
    return offset - step;
}
exports.indirect = indirect;
function keyIndex(key, dataView, offset, parentWidth, byteWidth, length) {
    const input = (0, flexbuffers_util_js_1.toUTF8Array)(key);
    const keysVectorOffset = indirect(dataView, offset, parentWidth) - byteWidth * 3;
    const bitWidth = (0, bit_width_util_js_1.fromByteWidth)(byteWidth);
    const indirectOffset = keysVectorOffset - Number(readUInt(dataView, keysVectorOffset, bitWidth));
    const _byteWidth = Number(readUInt(dataView, keysVectorOffset + byteWidth, bitWidth));
    let low = 0;
    let high = length - 1;
    while (low <= high) {
        const mid = (high + low) >> 1;
        const dif = diffKeys(input, mid, dataView, indirectOffset, _byteWidth);
        if (dif === 0)
            return mid;
        if (dif < 0) {
            high = mid - 1;
        }
        else {
            low = mid + 1;
        }
    }
    return null;
}
exports.keyIndex = keyIndex;
function diffKeys(input, index, dataView, offset, width) {
    const keyOffset = offset + index * width;
    const keyIndirectOffset = keyOffset - Number(readUInt(dataView, keyOffset, (0, bit_width_util_js_1.fromByteWidth)(width)));
    for (let i = 0; i < input.length; i++) {
        const dif = input[i] - dataView.getUint8(keyIndirectOffset + i);
        if (dif !== 0) {
            return dif;
        }
    }
    return dataView.getUint8(keyIndirectOffset + input.length) === 0 ? 0 : -1;
}
exports.diffKeys = diffKeys;
function keyForIndex(index, dataView, offset, parentWidth, byteWidth) {
    const keysVectorOffset = indirect(dataView, offset, parentWidth) - byteWidth * 3;
    const bitWidth = (0, bit_width_util_js_1.fromByteWidth)(byteWidth);
    const indirectOffset = keysVectorOffset - Number(readUInt(dataView, keysVectorOffset, bitWidth));
    const _byteWidth = Number(readUInt(dataView, keysVectorOffset + byteWidth, bitWidth));
    const keyOffset = indirectOffset + index * _byteWidth;
    const keyIndirectOffset = keyOffset - Number(readUInt(dataView, keyOffset, (0, bit_width_util_js_1.fromByteWidth)(_byteWidth)));
    let length = 0;
    while (dataView.getUint8(keyIndirectOffset + length) !== 0) {
        length++;
    }
    return (0, flexbuffers_util_js_1.fromUTF8Array)(new Uint8Array(dataView.buffer, keyIndirectOffset, length));
}
exports.keyForIndex = keyForIndex;
