"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.packedType = exports.fixedTypedVectorElementSize = exports.fixedTypedVectorElementType = exports.typedVectorElementType = exports.toTypedVector = exports.isAVector = exports.isFixedTypedVector = exports.isTypedVector = exports.isTypedVectorElement = exports.isIndirectNumber = exports.isNumber = exports.isInline = void 0;
const value_type_js_1 = require("./value-type.js");
function isInline(value) {
    return value === value_type_js_1.ValueType.BOOL
        || value <= value_type_js_1.ValueType.FLOAT;
}
exports.isInline = isInline;
function isNumber(value) {
    return value >= value_type_js_1.ValueType.INT
        && value <= value_type_js_1.ValueType.FLOAT;
}
exports.isNumber = isNumber;
function isIndirectNumber(value) {
    return value >= value_type_js_1.ValueType.INDIRECT_INT
        && value <= value_type_js_1.ValueType.INDIRECT_FLOAT;
}
exports.isIndirectNumber = isIndirectNumber;
function isTypedVectorElement(value) {
    return value === value_type_js_1.ValueType.BOOL
        || (value >= value_type_js_1.ValueType.INT
            && value <= value_type_js_1.ValueType.STRING);
}
exports.isTypedVectorElement = isTypedVectorElement;
function isTypedVector(value) {
    return value === value_type_js_1.ValueType.VECTOR_BOOL
        || (value >= value_type_js_1.ValueType.VECTOR_INT
            && value <= value_type_js_1.ValueType.VECTOR_STRING_DEPRECATED);
}
exports.isTypedVector = isTypedVector;
function isFixedTypedVector(value) {
    return value >= value_type_js_1.ValueType.VECTOR_INT2
        && value <= value_type_js_1.ValueType.VECTOR_FLOAT4;
}
exports.isFixedTypedVector = isFixedTypedVector;
function isAVector(value) {
    return isTypedVector(value)
        || isFixedTypedVector(value)
        || value === value_type_js_1.ValueType.VECTOR;
}
exports.isAVector = isAVector;
function toTypedVector(valueType, length) {
    if (length === 0)
        return valueType - value_type_js_1.ValueType.INT + value_type_js_1.ValueType.VECTOR_INT;
    if (length === 2)
        return valueType - value_type_js_1.ValueType.INT + value_type_js_1.ValueType.VECTOR_INT2;
    if (length === 3)
        return valueType - value_type_js_1.ValueType.INT + value_type_js_1.ValueType.VECTOR_INT3;
    if (length === 4)
        return valueType - value_type_js_1.ValueType.INT + value_type_js_1.ValueType.VECTOR_INT4;
    throw "Unexpected length " + length;
}
exports.toTypedVector = toTypedVector;
function typedVectorElementType(valueType) {
    return valueType - value_type_js_1.ValueType.VECTOR_INT + value_type_js_1.ValueType.INT;
}
exports.typedVectorElementType = typedVectorElementType;
function fixedTypedVectorElementType(valueType) {
    return ((valueType - value_type_js_1.ValueType.VECTOR_INT2) % 3) + value_type_js_1.ValueType.INT;
}
exports.fixedTypedVectorElementType = fixedTypedVectorElementType;
function fixedTypedVectorElementSize(valueType) {
    // The x / y >> 0 trick is to have an int division. Suppose to be faster than Math.floor()
    return (((valueType - value_type_js_1.ValueType.VECTOR_INT2) / 3) >> 0) + 2;
}
exports.fixedTypedVectorElementSize = fixedTypedVectorElementSize;
function packedType(valueType, bitWidth) {
    return bitWidth | (valueType << 2);
}
exports.packedType = packedType;
