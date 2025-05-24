"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Reference = exports.toReference = void 0;
const bit_width_util_js_1 = require("./bit-width-util.js");
const value_type_js_1 = require("./value-type.js");
const value_type_util_js_1 = require("./value-type-util.js");
const reference_util_js_1 = require("./reference-util.js");
const flexbuffers_util_js_1 = require("./flexbuffers-util.js");
const bit_width_js_1 = require("./bit-width.js");
function toReference(buffer) {
    const len = buffer.byteLength;
    if (len < 3) {
        throw "Buffer needs to be bigger than 3";
    }
    const dataView = new DataView(buffer);
    const byteWidth = dataView.getUint8(len - 1);
    const packedType = dataView.getUint8(len - 2);
    const parentWidth = (0, bit_width_util_js_1.fromByteWidth)(byteWidth);
    const offset = len - byteWidth - 2;
    return new Reference(dataView, offset, parentWidth, packedType, "/");
}
exports.toReference = toReference;
function valueForIndexWithKey(index, key, dataView, offset, parentWidth, byteWidth, length, path) {
    const _indirect = (0, reference_util_js_1.indirect)(dataView, offset, parentWidth);
    const elementOffset = _indirect + index * byteWidth;
    const packedType = dataView.getUint8(_indirect + length * byteWidth + index);
    return new Reference(dataView, elementOffset, (0, bit_width_util_js_1.fromByteWidth)(byteWidth), packedType, `${path}/${key}`);
}
class Reference {
    constructor(dataView, offset, parentWidth, packedType, path) {
        this.dataView = dataView;
        this.offset = offset;
        this.parentWidth = parentWidth;
        this.packedType = packedType;
        this.path = path;
        this._length = -1;
        this.byteWidth = 1 << (packedType & 3);
        this.valueType = packedType >> 2;
    }
    isNull() { return this.valueType === value_type_js_1.ValueType.NULL; }
    isNumber() { return (0, value_type_util_js_1.isNumber)(this.valueType) || (0, value_type_util_js_1.isIndirectNumber)(this.valueType); }
    isFloat() { return value_type_js_1.ValueType.FLOAT === this.valueType || value_type_js_1.ValueType.INDIRECT_FLOAT === this.valueType; }
    isInt() { return this.isNumber() && !this.isFloat(); }
    isString() { return value_type_js_1.ValueType.STRING === this.valueType || value_type_js_1.ValueType.KEY === this.valueType; }
    isBool() { return value_type_js_1.ValueType.BOOL === this.valueType; }
    isBlob() { return value_type_js_1.ValueType.BLOB === this.valueType; }
    isVector() { return (0, value_type_util_js_1.isAVector)(this.valueType); }
    isMap() { return value_type_js_1.ValueType.MAP === this.valueType; }
    boolValue() {
        if (this.isBool()) {
            return (0, reference_util_js_1.readInt)(this.dataView, this.offset, this.parentWidth) > 0;
        }
        return null;
    }
    intValue() {
        if (this.valueType === value_type_js_1.ValueType.INT) {
            return (0, reference_util_js_1.readInt)(this.dataView, this.offset, this.parentWidth);
        }
        if (this.valueType === value_type_js_1.ValueType.UINT) {
            return (0, reference_util_js_1.readUInt)(this.dataView, this.offset, this.parentWidth);
        }
        if (this.valueType === value_type_js_1.ValueType.INDIRECT_INT) {
            return (0, reference_util_js_1.readInt)(this.dataView, (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth), (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
        }
        if (this.valueType === value_type_js_1.ValueType.INDIRECT_UINT) {
            return (0, reference_util_js_1.readUInt)(this.dataView, (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth), (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
        }
        return null;
    }
    floatValue() {
        if (this.valueType === value_type_js_1.ValueType.FLOAT) {
            return (0, reference_util_js_1.readFloat)(this.dataView, this.offset, this.parentWidth);
        }
        if (this.valueType === value_type_js_1.ValueType.INDIRECT_FLOAT) {
            return (0, reference_util_js_1.readFloat)(this.dataView, (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth), (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
        }
        return null;
    }
    numericValue() { return this.floatValue() || this.intValue(); }
    stringValue() {
        if (this.valueType === value_type_js_1.ValueType.STRING || this.valueType === value_type_js_1.ValueType.KEY) {
            const begin = (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth);
            return (0, flexbuffers_util_js_1.fromUTF8Array)(new Uint8Array(this.dataView.buffer, begin, this.length()));
        }
        return null;
    }
    blobValue() {
        if (this.isBlob()) {
            const begin = (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth);
            return new Uint8Array(this.dataView.buffer, begin, this.length());
        }
        return null;
    }
    get(key) {
        const length = this.length();
        if (Number.isInteger(key) && (0, value_type_util_js_1.isAVector)(this.valueType)) {
            if (key >= length || key < 0) {
                throw `Key: [${key}] is not applicable on ${this.path} of ${this.valueType} length: ${length}`;
            }
            const _indirect = (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth);
            const elementOffset = _indirect + key * this.byteWidth;
            let _packedType = this.dataView.getUint8(_indirect + length * this.byteWidth + key);
            if ((0, value_type_util_js_1.isTypedVector)(this.valueType)) {
                const _valueType = (0, value_type_util_js_1.typedVectorElementType)(this.valueType);
                _packedType = (0, value_type_util_js_1.packedType)(_valueType, bit_width_js_1.BitWidth.WIDTH8);
            }
            else if ((0, value_type_util_js_1.isFixedTypedVector)(this.valueType)) {
                const _valueType = (0, value_type_util_js_1.fixedTypedVectorElementType)(this.valueType);
                _packedType = (0, value_type_util_js_1.packedType)(_valueType, bit_width_js_1.BitWidth.WIDTH8);
            }
            return new Reference(this.dataView, elementOffset, (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth), _packedType, `${this.path}[${key}]`);
        }
        if (typeof key === 'string') {
            const index = (0, reference_util_js_1.keyIndex)(key, this.dataView, this.offset, this.parentWidth, this.byteWidth, length);
            if (index !== null) {
                return valueForIndexWithKey(index, key, this.dataView, this.offset, this.parentWidth, this.byteWidth, length, this.path);
            }
        }
        throw `Key [${key}] is not applicable on ${this.path} of ${this.valueType}`;
    }
    length() {
        let size;
        if (this._length > -1) {
            return this._length;
        }
        if ((0, value_type_util_js_1.isFixedTypedVector)(this.valueType)) {
            this._length = (0, value_type_util_js_1.fixedTypedVectorElementSize)(this.valueType);
        }
        else if (this.valueType === value_type_js_1.ValueType.BLOB
            || this.valueType === value_type_js_1.ValueType.MAP
            || (0, value_type_util_js_1.isAVector)(this.valueType)) {
            this._length = (0, reference_util_js_1.readUInt)(this.dataView, (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth) - this.byteWidth, (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
        }
        else if (this.valueType === value_type_js_1.ValueType.NULL) {
            this._length = 0;
        }
        else if (this.valueType === value_type_js_1.ValueType.STRING) {
            const _indirect = (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth);
            let sizeByteWidth = this.byteWidth;
            size = (0, reference_util_js_1.readUInt)(this.dataView, _indirect - sizeByteWidth, (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
            while (this.dataView.getInt8(_indirect + size) !== 0) {
                sizeByteWidth <<= 1;
                size = (0, reference_util_js_1.readUInt)(this.dataView, _indirect - sizeByteWidth, (0, bit_width_util_js_1.fromByteWidth)(this.byteWidth));
            }
            this._length = size;
        }
        else if (this.valueType === value_type_js_1.ValueType.KEY) {
            const _indirect = (0, reference_util_js_1.indirect)(this.dataView, this.offset, this.parentWidth);
            size = 1;
            while (this.dataView.getInt8(_indirect + size) !== 0) {
                size++;
            }
            this._length = size;
        }
        else {
            this._length = 1;
        }
        return Number(this._length);
    }
    toObject() {
        const length = this.length();
        if (this.isVector()) {
            const result = [];
            for (let i = 0; i < length; i++) {
                result.push(this.get(i).toObject());
            }
            return result;
        }
        if (this.isMap()) {
            const result = {};
            for (let i = 0; i < length; i++) {
                const key = (0, reference_util_js_1.keyForIndex)(i, this.dataView, this.offset, this.parentWidth, this.byteWidth);
                result[key] = valueForIndexWithKey(i, key, this.dataView, this.offset, this.parentWidth, this.byteWidth, length, this.path).toObject();
            }
            return result;
        }
        if (this.isNull()) {
            return null;
        }
        if (this.isBool()) {
            return this.boolValue();
        }
        if (this.isNumber()) {
            return this.numericValue();
        }
        return this.blobValue() || this.stringValue();
    }
}
exports.Reference = Reference;
